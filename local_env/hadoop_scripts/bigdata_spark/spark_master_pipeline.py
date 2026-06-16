from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("Spark_Master_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

daily_safe_schema = StructType([
    StructField("symbol",   StringType(), True),
    StructField("calc_date", StringType(), True),
])

monthly_safe_schema = StructType([
    StructField("symbol",     StringType(), True),
    StructField("calc_year",  StringType(), True),
    StructField("calc_month", StringType(), True)
])

def read_json_safe(path, schema):
    try:
        return spark.read.json(path)
    except Exception as e:
        print(f"hư mục {path} không tồn tại hoặc trống. Tạo DataFrame rỗng!")
        return spark.createDataFrame([], schema)

df51  = read_json_safe("/user/hadoop/stock_result/result_spark_task51",  daily_safe_schema)
df52  = read_json_safe("/user/hadoop/stock_result/result_spark_task52",  daily_safe_schema)
df53  = read_json_safe("/user/hadoop/stock_result/result_spark_task53",  daily_safe_schema)
df54  = read_json_safe("/user/hadoop/stock_result/result_spark_task54",  daily_safe_schema)
df56  = read_json_safe("/user/hadoop/stock_result/result_spark_task56",  daily_safe_schema)
df58  = read_json_safe("/user/hadoop/stock_result/result_spark_task58",  daily_safe_schema)
df510 = read_json_safe("/user/hadoop/stock_result/result_spark_task510", daily_safe_schema)

df57 = read_json_safe("/user/hadoop/stock_result/result_spark_task57", daily_safe_schema)
if "symbol" not in df57.columns:
    df57 = df57.withColumn("symbol", lit("MARKET"))

df_volume_tong_hop = df51.unionByName(df57, allowMissingColumns=True)

daily_df = df_volume_tong_hop \
    .join(df52,  on=["symbol", "calc_date"], how="outer") \
    .join(df53,  on=["symbol", "calc_date"], how="outer") \
    .join(df56,  on=["symbol", "calc_date"], how="outer") \
    .join(df58,  on=["symbol", "calc_date"], how="outer") \
    .join(df510, on=["symbol", "calc_date"], how="outer") \
    .join(df54,  on=["symbol"],              how="left")

seen, to_drop = set(), []
for c in daily_df.columns:
    if c in seen:
        to_drop.append(c)
    else:
        seen.add(c)
if to_drop:
    daily_df = daily_df.drop(*to_drop)

daily_df.select(to_json(struct([col(c) for c in daily_df.columns])).alias("value")) \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock_daily_topic") \
    .save()

df55 = read_json_safe("/user/hadoop/stock_result/result_spark_task55", monthly_safe_schema)
df59 = read_json_safe("/user/hadoop/stock_result/result_spark_task59", monthly_safe_schema)

monthly_df = df55.join(df59, on=["symbol", "calc_year", "calc_month"], how="outer")

seen, to_drop = set(), []
for c in monthly_df.columns:
    if c in seen:
        to_drop.append(c)
    else:
        seen.add(c)
if to_drop:
    monthly_df = monthly_df.drop(*to_drop)

monthly_df.select(to_json(struct([col(c) for c in monthly_df.columns])).alias("value")) \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock_monthly_topic") \
    .save()

spark.stop()
