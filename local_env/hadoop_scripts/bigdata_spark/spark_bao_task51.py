import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, to_date, coalesce

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
spark = SparkSession.builder.appName("Task51_RealtimeVolume_Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/user/hadoop/stock_cleaned/")

df_date = df.withColumn(
    "calc_date",
    coalesce(
        to_date(col("Trading_Date"), "yyyy-MM-dd"),
        to_date(col("Trading_Date"), "dd/MM/yyyy"),
        to_date(col("Trading_Date"), "dd-MM-dash")
    )
)

df_clean_date = df_date.filter(col("calc_date").isNotNull())

df_result = df_clean_date.groupBy("Symbol", "calc_date").agg(
    max("Volume").alias("total_volume")
).select(
    col("Symbol").alias("symbol"),
    "calc_date",
    "total_volume"
)

output_path = "/user/hadoop/stock_result/result_spark_task51/"
df_result.write.mode("overwrite").json(output_path)

spark.stop()