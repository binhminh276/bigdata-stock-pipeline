import os
import time
import pandas as pd
from datetime import datetime
import pymysql
from vnstock.api.quote import Quote

# ==========================================
# CẤU HÌNH HỆ THỐNG
# ==========================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'bigdata_stock'
}
CSV_FILE_PATH = '/home/hadoop/local_data/Unified_Banks.csv'

def run_pipeline_realtime():
    current_time_obj = datetime.now()
    current_time_str = current_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    today = current_time_obj.strftime("%Y-%m-%d")

    print(f"\n[{current_time_str}] KHỞI ĐỘNG CÀO DỮ LIỆU TỪ API...")

    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # [TỰ ĐỘNG DỌN RÁC] Nếu đúng 09:00 sáng thì xóa sạch bảng để đón phiên mới
        current_hour = current_time_obj.hour
        current_minute = current_time_obj.minute
        if current_hour == 9 and current_minute < 5:
            print("🌅 Sáng rồi! Tự động TRUNCATE dọn sạch MySQL của ngày hôm qua...")
            cursor.execute("TRUNCATE TABLE tbl_raw_stock")
            connection.commit()

        # NHỊP 1: ĐỌC CONFIG
        cursor.execute("SELECT symbol, source FROM tbl_bank_list WHERE status = 1")
        config_list = cursor.fetchall()

        if not config_list:
            print("❌ Không có mã ngân hàng nào được kích hoạt.")
            return

        scraped_data = []

        # NHỊP 2: CÀO DỮ LIỆU (CHỈ LẤY GIÁ TRỊ MỚI NHẤT)
        for item in config_list:
            symbol = item[0]
            source_name = item[1]

            print(f"  + Đang cào mã [{symbol}]...", end=" ")
            try:
                q = Quote(symbol=symbol, source='VCI')
                df = q.history(start=today, end=today)

                if not df.empty:
                    # CHỈ LẤY DÒNG CUỐI CÙNG (Dòng cập nhật mới nhất)
                    row = df.iloc[-1]

                    # Cập nhật Key khớp 100% với tên cột MySQL
                    scraped_data.append({
                        "symbol": symbol,
                        "trading_date": str(row.get("time"))[:10],
                        "scrape_time": current_time_str,
                        "source": source_name,
                        "close_price": float(row.get("close")),
                        "volume": int(row.get("volume")),
                        "open_price": float(row.get("open")),
                        "high_price": float(row.get("high")),
                        "low_price": float(row.get("low"))
                    })
                    print("Thành công")
                else:
                    print("Không có dữ liệu mới")
            except Exception as e:
                print(f"Lỗi: {e}")
            time.sleep(0.5)

        # NHỊP 3: XUẤT RA FILE LÀM BỘ ĐỆM
        if scraped_data:
            df_new = pd.DataFrame(scraped_data)
            df_new.to_csv(CSV_FILE_PATH, mode='w', index=False, header=True)
            print(f"\n✅ Đã xuất chuẩn {len(df_new)} bản ghi mới nhất ra file tạm.")
        else:
            print("\n⚠️ Không có dữ liệu mới. Dừng tiến trình.")
            return

        # NHỊP 4: NẠP DỮ LIỆU MỚI (LƯU NỐI TIẾP VÀO MYSQL)
        print(f"-> Đang nạp dữ liệu nối tiếp vào Trạm MySQL...")
        df_csv = pd.read_csv(CSV_FILE_PATH)

        # Lệnh Insert sử dụng chính xác tên cột từ DESCRIBE của bác
        sql_insert = """
        INSERT INTO tbl_raw_stock
        (symbol, trading_date, scrape_time, source, close_price, volume, open_price, high_price, low_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_to_insert = []
        for _, row in df_csv.iterrows():
            data_to_insert.append((
                row['symbol'], row['trading_date'], row['scrape_time'], row['source'],
                row['close_price'], row['volume'], row['open_price'], row['high_price'], row['low_price']
            ))

        cursor.executemany(sql_insert, data_to_insert)
        connection.commit()
        print(f"✅ Đã THÊM NỐI TIẾP {cursor.rowcount} dòng MỚI vào tbl_raw_stock!")

        # NHỊP 5: DỌN DẸP FILE TẠM
        if os.path.exists(CSV_FILE_PATH):
            os.remove(CSV_FILE_PATH)
            print(f"🗑️ Đã tự động xóa file đệm: {CSV_FILE_PATH}")

    except Exception as err:
        print(f"❌ Lỗi hệ thống: {err}")
    finally:
        if 'connection' in locals() and connection.open:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    run_pipeline_realtime()