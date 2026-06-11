from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit

spark = SparkSession.builder \
    .appName("Spark_Master_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

df51  = spark.read.json("/user/hadoop/stock_result/result_spark_task51")
df52  = spark.read.json("/user/hadoop/stock_result/result_spark_task52")
df53  = spark.read.json("/user/hadoop/stock_result/result_spark_task53")
df54  = spark.read.json("/user/hadoop/stock_result/result_spark_task54")
df56  = spark.read.json("/user/hadoop/stock_result/result_spark_task56")
df58  = spark.read.json("/user/hadoop/stock_result/result_spark_task58")
df510 = spark.read.json("/user/hadoop/stock_result/result_spark_task510")

df57 = spark.read.json("/user/hadoop/stock_result/result_spark_task57")
if "symbol" not in df57.columns:
    df57 = df57.withColumn("symbol", lit("MARKET"))

df_volume_tong_hop = df51.unionByName(df57, allowMissingColumns=True)

daily_df = df_volume_tong_hop \
    .join(df52,  on=["symbol", "calc_date"], how="outer") \
    .join(df53,  on=["symbol", "calc_date"], how="outer") \
    .join(df54,  on=["symbol"], how="left") \
    .join(df56,  on=["symbol", "calc_date"], how="outer") \
    .join(df58,  on=["symbol", "calc_date"], how="outer") \
    .join(df510, on=["symbol", "calc_date"], how="outer")

seen, to_drop = set(), []
for c in daily_df.columns:
    if c in seen: to_drop.append(c)
    else: seen.add(c)
if to_drop:
    daily_df = daily_df.drop(*to_drop)

daily_df.select(to_json(struct([col(c) for c in daily_df.columns])).alias("value")) \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock_daily_topic") \
    .save()

df55 = spark.read.json("/user/hadoop/stock_result/result_spark_task55")
df59 = spark.read.json("/user/hadoop/stock_result/result_spark_task59")

monthly_df = df55.join(df59, on=["symbol", "calc_year", "calc_month"], how="outer")

monthly_df.select(to_json(struct([col(c) for c in monthly_df.columns])).alias("value")) \
    .write.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "stock_monthly_topic") \
    .save()

spark.stop()
