from pyspark.sql import SparkSession
from pyspark.sql import Row

def process_streaks(record):
    symbol = record[0]
    data = list(record[1]) 

    unique_data = {}
    for row in data:
        t_date = row[0]
        s_time = row[1]
        u_day = row[2]
        d_day = row[3]

        if t_date not in unique_data or s_time > unique_data[t_date]['scrape_time']:
            unique_data[t_date] = {
                'scrape_time': s_time, 
                'up_day': u_day, 
                'down_day': d_day
            }

    streak_up = 0
    streak_down = 0
    result = []

    for t_date in sorted(unique_data.keys()):
        u_day = unique_data[t_date]['up_day']
        d_day = unique_data[t_date]['down_day']

        if u_day == 1:
            streak_up += 1
            streak_down = 0
        elif d_day == 1:
            streak_down += 1
            streak_up = 0

        result.append(Row(symbol=symbol, calc_date=t_date, up_days_count=streak_up, down_days_count=streak_down))
    
    return result

spark = SparkSession.builder.appName("Spark_Task53_Streak").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

rdd = spark.sparkContext.textFile("/user/hadoop/stock_cleaned_csv/")

parsed_rdd = rdd.map(lambda line: line.split(',')) \
    .filter(lambda parts: len(parts) >= 9 and parts[0].lower() != 'symbol') \
    .map(lambda parts: (
        parts[0],
        (
            parts[1], 
            parts[2], 
            1 if float(parts[6]) > float(parts[4]) else 0, 
            1 if float(parts[6]) < float(parts[4]) else 0
        )
    ))

result_rdd = parsed_rdd.groupByKey().flatMap(process_streaks)

df = spark.createDataFrame(result_rdd)
df.write.mode("overwrite").json("/user/hadoop/stock_result/result_spark_task53")

spark.stop()
 
