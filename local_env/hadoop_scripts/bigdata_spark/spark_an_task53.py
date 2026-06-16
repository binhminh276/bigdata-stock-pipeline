import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, dayofweek, row_number, lag, coalesce, when, sum as _sum
from pyspark.sql.window import Window

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Spark_Task53_Streak_Fixed").getOrCreate()
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
 .filter((dayofweek(col("norm_date")) != 1) & (dayofweek(col("norm_date")) != 7

window_day = Window.partitionBy("Symbol", "norm_date").orderBy(col("Scrape_Time").desc())
df_eod = df_date.withColumn("rn", row_number().over(window_day)).filter(col("rn") == 1).drop("rn")

window_timeline = Window.partitionBy("Symbol").orderBy("norm_date")

df_with_prev = df_eod.withColumn("prev_close", lag("Close", 1).over(window_timeline))

df_signals = df_with_prev.withColumn(
    "trend_signal",
    when(col("prev_close").isNull(), 0)                    
    .when(col("Close") > col("prev_close"), 1)             
    .when(col("Close") < col("prev_close"), -1)            
    .otherwise(0)                                      
)

#
df_island = df_signals.withColumn(
    "is_new_streak",
    when(col("trend_signal") != lag("trend_signal", 1).over(window_timeline), 1).otherwise(0)
).withColumn(
    "streak_id",
    _sum("is_new_streak").over(window_timeline)
)

window_streak_run = Window.partitionBy("Symbol", "streak_id").orderBy("norm_date")

df_result = df_island.withColumn(
    "up_days_count",
    when(col("trend_signal") == 1, row_number().over(window_streak_run)).otherwise(0)
).withColumn(
    "down_days_count",
    when(col("trend_signal") == -1, row_number().over(window_streak_run)).otherwise(0)
).select(
    col("Symbol").alias("symbol"),          
    col("norm_date").alias("calc_date"),    
    "up_days_count",                        
    "down_days_count"                       
).orderBy("symbol", "calc_date")

output_path = "/user/hadoop/stock_result/result_spark_task53"
df_result.write.mode("overwrite").json(output_path)

spark.stop()
