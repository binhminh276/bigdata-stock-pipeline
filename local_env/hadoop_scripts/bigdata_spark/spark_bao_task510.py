import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, round, expr, to_date, coalesce

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
spark = SparkSession.builder.appName("Task510_RealtimeMaxDrop_Fixed").getOrCreate()
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

df_with_drop = df_clean_date.withColumn(
    "intraday_drop",
    expr("CASE WHEN Open > Low THEN Open - Low ELSE 0 END")
)

df_result = df_with_drop.groupBy("Symbol", "calc_date").agg(
    round(max("intraday_drop"), 2).alias("max_intraday_drop")
).select(
    col("Symbol").alias("symbol"),
    "calc_date",
    "max_intraday_drop"
)

output_path = "/user/hadoop/stock_result/result_spark_task510/"
df_result.write.mode("overwrite").json(output_path)

spark.stop()