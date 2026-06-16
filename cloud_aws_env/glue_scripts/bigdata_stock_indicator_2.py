import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max as _max, min as _min, round, row_number, sum as _sum, year, month, lit, avg

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

KAFKA_BROKER = "32.195.43.148:9092" 
TOPIC_DAILY = "stock_daily_topic"
TOPIC_MONTHLY = "stock_monthly_topic"

input_path = "s3://stock-data-lake-group/processed/stock_cleaned/"
df = spark.read.parquet(input_path)

window_dedup = Window.partitionBy("symbol", "trading_date").orderBy(col("scrape_time").desc())
df_latest = df.withColumn("row_num", row_number().over(window_dedup)).filter(col("row_num") == 1).drop("row_num")

df_minmax = df_latest.groupBy("symbol", "trading_date").agg(
    round(_max("close_price"), 2).alias("max_close_price"),
    round(_min("close_price"), 2).alias("min_close_price")
)

df_monthly_vol = df_latest.groupBy(
    col("symbol"),
    year(col("trading_date")).alias("calc_year"),
    month(col("trading_date")).alias("calc_month")
).agg(
    round(avg("close_price"), 2).alias("monthly_avg_close"),
    _sum("volume").cast("bigint").alias("monthly_total_volume")
)

rows_daily = df_minmax.collect()
rows_monthly = df_monthly_vol.collect()

if rows_daily or rows_monthly:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda m: m.encode('utf-8') if m else None,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    
    for row in rows_daily:
        payload_daily = {
            "symbol": str(row["symbol"]),
            "calc_date": str(row["trading_date"]),
            "total_volume": None, 
            "max_intraday_drop": None,       
            "max_close_price": float(row["max_close_price"]),
            "min_close_price": float(row["min_close_price"]),
            "up_days_count": None,           
            "down_days_count": None,         
            "max_volume_date": None,
            "max_volume_value": None,
            "max_intraday_volatility": None, 
            "liquidity_status": None         
        }
        producer.send(TOPIC_DAILY, key=str(row["symbol"]), value=payload_daily)

    for row in rows_monthly:
        payload_monthly = {
            "symbol": str(row["symbol"]),
            "calc_year": int(row["calc_year"]),
            "calc_month": int(row["calc_month"]),
            "monthly_avg_close": float(row["monthly_avg_close"]),
            "monthly_total_volume": int(row["monthly_total_volume"])
        }
        producer.send(TOPIC_MONTHLY, key=str(row["symbol"]), value=payload_monthly)
            
    producer.flush()
    producer.close()
    print("🚀 SUCCESS: Luồng 2 đã dội dữ liệu thành công lên Kafka!")