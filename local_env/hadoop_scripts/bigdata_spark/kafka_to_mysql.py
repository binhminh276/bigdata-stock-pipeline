import json
import mysql.connector
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'stock_daily_topic', 'stock_monthly_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="bigdata_stock"
)
cursor = db.cursor()

upsert_daily = """
INSERT INTO tbl_stock_daily_analysis (
    symbol, calc_date, total_volume, max_close_price, min_close_price,
    up_days_count, down_days_count, max_volume_date, max_volume_value,
    max_intraday_volatility, liquidity_status, max_intraday_drop
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    total_volume = VALUES(total_volume),
    max_close_price = VALUES(max_close_price),
    min_close_price = VALUES(min_close_price),
    up_days_count = VALUES(up_days_count),
    down_days_count = VALUES(down_days_count),
    max_volume_date = VALUES(max_volume_date),
    max_volume_value = VALUES(max_volume_value),
    max_intraday_volatility = VALUES(max_intraday_volatility),
    liquidity_status = VALUES(liquidity_status),
    max_intraday_drop = VALUES(max_intraday_drop)
"""

upsert_monthly = """
INSERT INTO tbl_stock_monthly_analysis (
    symbol, calc_year, calc_month, monthly_avg_close, monthly_total_volume
) VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    monthly_avg_close = VALUES(monthly_avg_close),
    monthly_total_volume = VALUES(monthly_total_volume)
"""

try:
    print("Consumer bắt đầu hoạt động, đang lắng nghe Kafka...")
    for message in consumer:
        data = message.value

        try:
            if message.topic == 'stock_daily_topic':
                cursor.execute(upsert_daily, (
                    data.get("symbol"), data.get("calc_date"), data.get("total_volume"),
                    data.get("max_close_price"), data.get("min_close_price"), data.get("up_days_count"),
                    data.get("down_days_count"), data.get("max_volume_date"), data.get("max_volume_value"),
                    data.get("max_intraday_volatility"), data.get("liquidity_status"), data.get("max_intraday_drop")
                ))

            elif message.topic == 'stock_monthly_topic':
                if data.get("calc_year") is None:
                    continue

                cursor.execute(upsert_monthly, (
                    data.get("symbol"), data.get("calc_year"), data.get("calc_month"),
                    data.get("monthly_avg_close") or 0, data.get("monthly_total_volume")
                ))

            db.commit()
        except Exception as e:
            print(f"Lỗi khi nạp data (nhưng tiếp tục chạy): {e}")
            db.rollback()

except KeyboardInterrupt:
    print("Consumer đã dừng.")
finally:
    cursor.close()
    db.close()
