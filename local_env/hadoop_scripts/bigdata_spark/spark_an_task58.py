import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, row_number, when, coalesce
from pyspark.sql.window import Window

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Spark_Task58_Liquidity_Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

df_date = df.withColumn(
    "norm_date",
    coalesce(
        to_date(col("Trading_Date"), "yyyy-MM-dd"),
        to_date(col("Trading_Date"), "dd/MM/yyyy"),
        to_date(col("Trading_Date"), "dd-MM-dash")
    )
).filter(col("norm_date").isNotNull()) \
 .filter((dayofweek(col("norm_date")) != 1) & (dayofweek(col("norm_date")) != 7))

window_day = Window.partitionBy("Symbol", "norm_date").orderBy(col("Scrape_Time").desc())

df_eod = df_date.withColumn("rn", row_number().over(window_day)).filter(col("rn") == 1).drop("rn")

df_result = df_eod.withColumn(
    "liquidity_status",
    when(col("Volume") >= 10000000, "High")
    .when(col("Volume") >= 5000000, "Medium")
    .otherwise("Low")
).select(
    col("Symbol").alias("symbol"),
    col("norm_date").alias("calc_date"),
    "liquidity_status"
).orderBy("symbol", "calc_date")

output_path = "/user/hadoop/stock_result/result_spark_task58"
df_result.write.mode("overwrite").json(output_path)

spark.stop()
