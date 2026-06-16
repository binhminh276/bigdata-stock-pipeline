import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, row_number, sum as _sum, lit
from pyspark.sql.window import Window

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

KAFKA_BROKER = "32.195.43.148:9092" 
TOPIC_DAILY = "stock_daily_topic"

input_path = "s3://stock-data-lake-group/processed/stock_cleaned/"
df = spark.read.parquet(input_path)

window_spec = Window.partitionBy("symbol", "trading_date").orderBy(col("scrape_time").desc())
df_filtered = df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)


df_volatility = df_filtered.select(
    col("symbol"),
    col("trading_date").cast("string").alias("calc_date"),
    round(col("high_price") - col("low_price"), 2).alias("max_intraday_volatility")
)

df_market_vol = df_filtered.groupBy("trading_date").agg(
    _sum("volume").cast("bigint").alias("total_volume")
).withColumn("symbol", lit("MARKET")) \
 .withColumn("calc_date", col("trading_date").cast("string"))

rows_vol = df_volatility.collect()
rows_mkt = df_market_vol.collect()

if rows_vol or rows_mkt:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda m: m.encode('utf-8') if m else None,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    for row in rows_vol:
        payload = {
            "symbol": str(row.symbol),
            "calc_date": str(row.calc_date),
            "max_intraday_volatility": float(row.max_intraday_volatility),
            "total_volume": None, "max_intraday_drop": None, "max_close_price": None,
            "min_close_price": None, "up_days_count": None, "down_days_count": None,
            "liquidity_status": None, "sma_price": None
        }
        producer.send(TOPIC_DAILY, key=str(row.symbol), value=payload)

    for row in rows_mkt:
        payload_mkt = {
            "symbol": "MARKET",
            "calc_date": str(row.calc_date),
            "total_volume": int(row.total_volume),
            "max_volume_date": str(row.calc_date),
            "max_volume_value": int(row.total_volume),
            "max_intraday_drop": 0.0, "max_close_price": 0.0, "min_close_price": 0.0,
            "up_days_count": 0, "down_days_count": 0, "max_intraday_volatility": 0.0,
            "liquidity_status": "STABLE", "sma_price": 0.0
        }
        producer.send(TOPIC_DAILY, key="MARKET", value=payload_mkt)
        
    producer.flush()
    producer.close()
    print("🚀 SUCCESS: Luồng 5 hoàn tất!")