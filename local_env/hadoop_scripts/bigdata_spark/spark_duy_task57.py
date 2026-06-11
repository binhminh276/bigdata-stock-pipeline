import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, row_number
from pyspark.sql.window import Window

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
spark = SparkSession.builder.appName("Task57_MarketVolume_Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

window_spec = Window.partitionBy("Symbol", "Trading_Date").orderBy(col("Scrape_Time").desc())

df_latest_snapshot = df.withColumn("rank", row_number().over(window_spec)) \
                       .filter(col("rank") == 1)

df_market_vol = df_latest_snapshot.groupBy("Trading_Date").agg(
    sum("Volume").cast("bigint").alias("total_volume")
).withColumn("symbol", lit("MARKET")) \
 .withColumnRenamed("Trading_Date", "calc_date") \
 .select("symbol", "calc_date", "total_volume")

output_path = "/user/hadoop/stock_result/result_spark_task57/"
df_market_vol.write.mode("overwrite").json(output_path)

print(f"✅ Spark Task 5.7 XONG (Đã sửa logic cộng dồn)! Kết quả JSON đã lưu tại: {output_path}")
spark.stop()
