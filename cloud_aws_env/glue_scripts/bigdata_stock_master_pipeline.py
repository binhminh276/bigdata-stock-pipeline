import sys
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from kafka import KafkaProducer

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

KAFKA_BROKER = "32.195.43.148:9092"

def write_to_kafka(df, topic):
    """Hàm bắn dữ liệu từ Spark DF sang Kafka bằng Producer thuần"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda m: json.dumps(m, default=str).encode('utf-8')
    )
    
    data_list = df.toJSON().map(lambda j: json.loads(j)).collect()
    
    for record in data_list:
        producer.send(topic, value=record)
    
    producer.flush()
    producer.close()
    print(f"✅ Đã bắn {len(data_list)} bản ghi sang topic: {topic}")


input_path = "s3://stock-data-lake-group/processed/stock_cleaned/"
df = spark.read.parquet(input_path)

print("🚀 Master Pipeline: Đang tổng hợp Snapshot Daily...")
write_to_kafka(df, "stock_daily_snapshot")

print("🚀 Master Pipeline: Đang tổng hợp Snapshot Monthly...")
try:
    df_monthly = spark.read.parquet("s3://stock-data-lake-group/processed/stock_monthly_cleaned/")
    write_to_kafka(df_monthly, "stock_monthly_snapshot")
except Exception as e:
    print(f"⚠️ Chưa có dữ liệu Monthly để xuất snapshot: {e}")

print("🎉 SUCCESS: Master Pipeline đã hoàn tất toàn bộ tiến trình!")
spark.stop()