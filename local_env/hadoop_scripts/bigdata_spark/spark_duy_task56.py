import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, row_number
from pyspark.sql.window import Window

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Task56_PriceVolatility_Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

window_spec = Window.partitionBy("Symbol", "Trading_Date").orderBy(col("Scrape_Time").desc())

df_filtered = df.withColumn("rank", row_number().over(window_spec)) \
                .filter(col("rank") == 1) 

df_volatility = df_filtered.select(
    col("Symbol").alias("symbol"),
    col("Trading_Date").cast("string").alias("calc_date"),
    round(col("High") - col("Low"), 2).alias("max_intraday_volatility")
)

output_path = "/user/hadoop/stock_result/result_spark_task56/"
df_volatility.write.mode("overwrite").json(output_path)

print(f"✅ Spark Task 5.6 XONG (Đã sửa logic chống trùng)! Lưu tại: {output_path}")
spark.stop()
