import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max as _max, min as _min, round, row_number

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Task52_MinMaxClose_DF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

window_dedup = Window.partitionBy("Symbol", "Trading_Date").orderBy(col("Scrape_Time").desc())
df_latest = df.withColumn("row_num", row_number().over(window_dedup)).filter(col("row_num") == 1).drop("row_num")

window_minmax = Window.partitionBy("Symbol").orderBy("Trading_Date")

df_minmax = df_latest.select(
    col("Symbol").alias("symbol"),
    col("Trading_Date").alias("calc_date"),
    round(_max("Close").over(window_minmax), 2).alias("max_close_price"),
    round(_min("Close").over(window_minmax), 2).alias("min_close_price")
).orderBy("symbol", "calc_date")

print(" KET QUA TASK 52: MAX/MIN (10 DONG DAU) ")
df_minmax.show(10, truncate=False)

output_path = "/user/hadoop/stock_result/result_spark_task52/"
df_minmax.write.mode("overwrite").json(output_path)

print(f"\nDa luu toan bo ket qua xuong HDFS tai: {output_path}")
spark.stop()
