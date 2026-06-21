import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder.appName("Spark_Task53_Streak_Simple").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("/user/hadoop/stock_cleaned/")

df_date = df.filter((dayofweek(col("trading_date")) != 1) & (dayofweek(col("trading_date")) != 7))

window_day = Window.partitionBy("symbol", "trading_date").orderBy(col("scrape_time").desc())
df_eod = df_date.withColumn("rn", row_number().over(window_day)).filter(col("rn") == 1).drop("rn")

def calc_streak(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("trading_date")
    up_list, down_list = [], []
    streak_up, streak_down, prev_close = 0, 0, None

    for close in pdf["close"]:
        if prev_close is None:
            streak_up, streak_down = 0, 0
        elif close > prev_close:
            streak_up += 1
            streak_down = 0
        elif close < prev_close:
            streak_down += 1
            streak_up = 0
        else:
            streak_up, streak_down = 0, 0
        up_list.append(streak_up)
        down_list.append(streak_down)
        prev_close = close

    pdf["up_days_count"] = up_list
    pdf["down_days_count"] = down_list
    return pdf[["symbol", "trading_date", "up_days_count", "down_days_count"]]

schema = StructType([
    StructField("symbol", StringType()),
    StructField("trading_date", DateType()),
    StructField("up_days_count", IntegerType()),
    StructField("down_days_count", IntegerType()),
])

df_result = df_eod.groupBy("symbol").applyInPandas(calc_streak, schema=schema) \
    .select(
        "symbol",
        col("trading_date").alias("calc_date"),
        "up_days_count",
        "down_days_count"
    ).orderBy("symbol", "calc_date")

output_path = "/user/hadoop/stock_result/result_spark_task53"
df_result.write.mode("overwrite").json(output_path)

spark.stop()
