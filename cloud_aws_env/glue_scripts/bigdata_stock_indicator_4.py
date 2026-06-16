import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

KAFKA_BROKER = "32.195.43.148:9092" 
TOPIC_DAILY = "stock_daily_topic"

input_path = "s3://stock-data-lake-group/processed/stock_cleaned/"
df = spark.read.parquet(input_path)

def process_advanced_analytics(partition_data):
    unique_data = {}
    for row in partition_data:
        symbol = row.symbol
        t_date = str(row.trading_date).strip()
        s_time = row.scrape_time
        close_p = row.close_price
        high_p = row.high_price
        low_p = row.low_price

        if t_date not in unique_data or s_time > unique_data[t_date]['scrape_time']:
            unique_data[t_date] = {
                'symbol': symbol,
                'scrape_time': s_time,
                'close_price': close_p,
                'volatility': round(float(high_p) - float(low_p), 2)
            }
    
    results = []
    sorted_dates = sorted(unique_data.keys())
    
    for i, t_date in enumerate(sorted_dates):
        item = unique_data[t_date]
        start_idx = max(0, i - 4)
        recent_dates = sorted_dates[start_idx:i + 1]
        
        total_close = sum(unique_data[d]['close_price'] for d in recent_dates)
        sma_5 = round(total_close / len(recent_dates), 2)
        
        results.append(Row(
            symbol=str(item['symbol']), 
            calc_date=str(t_date), 
            max_intraday_volatility=float(item['volatility']),
            sma_price=float(sma_5)
        ))
    return results

rdd_paired = df.rdd.map(lambda row: (row.symbol, row))
rdd_mapped = rdd_paired.groupByKey().flatMap(lambda g: process_advanced_analytics(g[1]))
df_analytics = spark.createDataFrame(rdd_mapped)

print("🚀 Spark Luồng 4 đang tính toán Volatility & SMA 5 ngày...")
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
            "symbol": str(row.symbol),
            "calc_date": str(row.calc_date),
            "max_intraday_volatility": float(row.max_intraday_volatility),
            "sma_price": float(row.sma_price),
            "total_volume": None,
            "max_intraday_drop": None,
            "max_close_price": None,
            "min_close_price": None,
            "up_days_count": None,
            "down_days_count": None,
            "liquidity_status": None
        }
        producer.send(TOPIC_DAILY, key=str(row.symbol), value=payload)
        
    producer.flush()
    producer.close()
    print("🚀 SUCCESS: Luồng 4 đã dội dữ liệu thành công lên Kafka!")
else:
    print("WARNING: Không có dữ liệu để tính toán.")