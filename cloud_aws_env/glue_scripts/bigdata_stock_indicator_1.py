import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, round, to_date, concat_ws, expr


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
KAFKA_BROKER = "32.195.43.148:9092" 
TOPIC_DAILY = "stock_daily_topic"

input_path = "s3://stock-data-lake-group/processed/stock_cleaned/"
df = spark.read.parquet(input_path)

df_with_date = df.withColumn(
    "calc_date", 
    to_date(concat_ws("-", col("p_year"), col("p_month"), col("p_day")), "yyyy-MM-dd")
)

df_analytics = df_with_date.groupBy("symbol", "calc_date").agg(
    _sum("volume").alias("total_volume"),
    round(_max(expr("CASE WHEN open_price > low_price THEN open_price - low_price ELSE 0 END")), 2).alias("max_intraday_drop")
)

print("Spark đang xử lý luồng 1...")
local_rows = df_analytics.collect()

if local_rows:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda m: m.encode('utf-8') if m else None,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    
    for row in local_rows:
            payload = {
            "symbol": str(row["symbol"]),
            "calc_date": str(row["calc_date"]),
            "total_volume": int(row["total_volume"]),
            "max_intraday_drop": float(row["max_intraday_drop"]),
            "max_close_price": None,
            "min_close_price": None,
            "up_days_count": None,
            "down_days_count": None,
            "max_volume_date": None,
            "max_volume_value": None,
            "max_intraday_volatility": None,
            "liquidity_status": None,
            "sma_price": None
        }
        producer.send(TOPIC_DAILY, key=str(row["symbol"]), value=payload)
        
    producer.flush()
    producer.close()
    print(f"SUCCESS: Luồng 1 đã dội {len(local_rows)} dòng vào Kafka!")
else:
    print("WARNING: Không tìm thấy dữ liệu.")