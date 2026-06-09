import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, year, month, row_number

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Task59_MonthlyVolume_DF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

window_dedup = Window.partitionBy("Symbol", "Trading_Date").orderBy(col("Scrape_Time").desc())
df_latest = df.withColumn("row_num", row_number().over(window_dedup)).filter(col("row_num") == 1).drop("row_num")

df_monthly_vol = df_latest.groupBy(
    col("Symbol").alias("symbol"),
    year(col("Trading_Date")).alias("calc_year"),
    month(col("Trading_Date")).alias("calc_month")
).agg(
    _sum("Volume").cast("bigint").alias("monthly_total_volume")
).orderBy("symbol", "calc_year", "calc_month")

print(" KET QUA TASK 59: TONG KHOI LUONG THEO THANG (10 DONG DAU) ")
df_monthly_vol.show(10, truncate=False)

output_path = "/user/hadoop/stock_result/result_spark_task59/"
df_monthly_vol.write.mode("overwrite").json(output_path)

print(f"\nDa luu toan bo ket qua xuong HDFS tai: {output_path}")
spark.stop()
