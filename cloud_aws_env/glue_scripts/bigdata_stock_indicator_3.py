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
        t_date = str(row.trading_date)
        s_time = row.scrape_time
        close_p = row.close_price
        open_p = row.open_price
        vol = row.volume

        if t_date not in unique_data or s_time > unique_data[t_date]['scrape_time']:
            unique_data[t_date] = {
                'symbol': symbol,
                'scrape_time': s_time,
                'volume': vol,
                'up_day': 1 if close_p > open_p else 0,
                'down_day': 1 if close_p < open_p else 0
            }
    
    streak_up = 0
    streak_down = 0
    results = []
    
    for t_date in sorted(unique_data.keys()):
        item = unique_data[t_date]
        if item['up_day'] == 1:
            streak_up += 1
            streak_down = 0
        elif item['down_day'] == 1:
            streak_down += 1
            streak_up = 0
        else:
            streak_up = 0
            streak_down = 0
            
        status = "High" if item['volume'] >= 10000000 else ("Medium" if item['volume'] >= 5000000 else "Low")
        
        results.append(Row(
            symbol=str(item['symbol']), 
            calc_date=str(t_date), 
            up_days_count=int(streak_up), 
            down_days_count=int(streak_down),
            liquidity_status=str(status)
        ))
    return results

rdd_paired = df.rdd.map(lambda row: (row.symbol, row))
rdd_mapped = rdd_paired.groupByKey().flatMap(lambda g: process_advanced_analytics(g[1]))
df_analytics = spark.createDataFrame(rdd_mapped)

print("🚀 Spark Luồng 3 đang xử lý chuỗi tăng giảm...")
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
            "up_days_count": int(row.up_days_count),
            "down_days_count": int(row.down_days_count),
            "liquidity_status": str(row.liquidity_status),
            "total_volume": None,
            "max_intraday_drop": None,
            "max_close_price": None,
            "min_close_price": None,
            "max_intraday_volatility": None
        }
        producer.send(TOPIC_DAILY, key=str(row.symbol), value=payload)
        
    producer.flush()
    producer.close()
    print("🚀 SUCCESS: Luồng 3 hoàn tất, dội chỉ số Streak & Liquidity lên Kafka!")
else:
    print("WARNING: Không có dữ liệu để tính toán.")