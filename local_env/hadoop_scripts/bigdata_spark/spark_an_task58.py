from pyspark.sql import SparkSession
from pyspark.sql import Row

def process_liquidity(record):
    symbol = record[0]
    data = list(record[1])

    unique_data = {}
    for row in data:
        t_date = row[0]
        s_time = row[1]
        vol = row[2]

        if t_date not in unique_data or s_time > unique_data[t_date]['scrape_time']:
            unique_data[t_date] = {'scrape_time': s_time, 'volume': vol}

    result = []
    for t_date in sorted(unique_data.keys()):
        vol = unique_data[t_date]['volume']

        if vol >= 10000000:
            status = "High"
        elif vol >= 5000000:
            status = "Medium"
        else:
            status = "Low"

        result.append(Row(symbol=symbol, calc_date=t_date, liquidity_status=status))
    
    return result

spark = SparkSession.builder.appName("Spark_Task58_Liquidity").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("/user/hadoop/stock_cleaned_csv/")

parsed_rdd = rdd.map(lambda line: line.split(',')) \
    .filter(lambda parts: len(parts) >= 9 and parts[0].lower() != 'symbol') \
    .map(lambda parts: (parts[0], (parts[1], parts[2], float(parts[5]))))

result_rdd = parsed_rdd.groupByKey().flatMap(process_liquidity)

df = spark.createDataFrame(result_rdd)
df.write.mode("overwrite").json("/user/hadoop/stock_result/result_spark_task58")

spark.stop()
