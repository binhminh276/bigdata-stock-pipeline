import json
import boto3
from datetime import datetime
import time
import sys
import os
import shutil

os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
time.tzset()
os.environ['HOME'] = '/tmp'
os.environ['XDG_CACHE_HOME'] = '/tmp'

if not os.path.exists('/tmp/cleaned_marker_v5'):
    for item in os.listdir('/tmp'):
        item_path = os.path.join('/tmp', item)
        try:
            if os.path.isdir(item_path): shutil.rmtree(item_path)
            else: os.remove(item_path)
        except Exception: pass
    with open('/tmp/cleaned_marker_v5', 'w') as f: f.write('clean')


sys.path.insert(0, '/tmp/')
try:
    from vnstock.api.quote import Quote
    import pymysql
except ImportError:
    import subprocess
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", 
        "vnstock", "pymysql", "vnai", "tenacity", "annotated_types",
        "pydantic==2.10.6", "pydantic_core==2.27.2",
        "--target", "/tmp/", "--no-deps"
    ])
    from vnstock.api.quote import Quote
    import pymysql

import pandas as pd

def lambda_handler(event, context):
    BUCKET_RAW_NAME = "stock-data-lake-group"
    
    RDS_CONFIG = {
        'host': '10.0.1.161', 
        'user': 'admin',
        'password': '12345678',
        'database': 'bigdata_stock',
        'port': 3306,
        'connect_timeout': 5
    }
    
    current_time_obj = datetime.now()
    current_time_str = current_time_obj.strftime("%Y-%m-%d %H:%M:%S")
    today = current_time_obj.strftime("%Y-%m-%d")
    
    config_list = []

    try:
        connection = pymysql.connect(**RDS_CONFIG)
        with connection.cursor() as cursor:
            sql_query = "SELECT symbol, source FROM tbl_bank_list WHERE status = 1"
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            for row in rows:
                config_list.append((row[0], row[1]))
        connection.close()
        print(f"Connected RDS. Found {len(config_list)} symbols từ tbl_bank_list.")
        
    except Exception as db_err:
        print(f"RDS Connection warning: {db_err}. Kích hoạt danh sách mã Fallback tĩnh.")
        config_list = [
            ('VCB', 'CafeF'), ('BID', 'CafeF'), ('CTG', 'CafeF'), ('MBB', 'CafeF'),
            ('TCB', 'CafeF'), ('VPB', 'CafeF'), ('ACB', 'CafeF'), ('STB', 'CafeF'),
            ('SHB', 'TCBS'), ('HDB', 'TCBS'), ('VIB', 'TCBS'), ('TPB', 'TCBS'),
            ('EIB', 'TCBS'), ('MSB', 'TCBS'), ('SSB', 'TCBS'), ('LPB', 'TCBS'),
            ('OCB', 'FireAnt'), ('NAB', 'FireAnt'), ('KLB', 'FireAnt'), ('BVB', 'FireAnt')
        ]

    if not config_list:
        return {'statusCode': 200, 'body': 'No configuration found.'}

    scraped_data = []


    for item in config_list:
        symbol = item[0]
        db_source = item[1] 
        api_source = 'vci'

        try:
            q = Quote(symbol=symbol, source=api_source)
            df = q.history(start=today, end=today)
            
            if df is not None and not df.empty:
                last_row = df.iloc[-1]
                
            
                scraped_data.append({
                    "symbol": str(symbol),
                    "trading_date": str(last_row.get("time", today))[:10],
                    "scrape_time": str(current_time_str),
                    "source": str(db_source), 
                    "close_price": float(last_row.get("close", 0)),
                    "volume": int(last_row.get("volume", 0)),
                    "open_price": float(last_row.get("open", 0)),
                    "high_price": float(last_row.get("high", 0)),
                    "low_price": float(last_row.get("low", 0))
                })
                print(f"Scraped thành công mã {symbol}.")
            else:
                print(f"Không có dữ liệu mới cho mã {symbol}.")
        except Exception as e:
            print(f"Lỗi khi cào mã {symbol}: {e}")
            
        time.sleep(0.12)

    if not scraped_data:
        return {'statusCode': 200, 'body': 'No new data collected.'}
        

    year = current_time_obj.strftime("%Y")
    month = current_time_obj.strftime("%m")
    day = current_time_obj.strftime("%d")
    current_hour = current_time_obj.hour
    
    time_suffix = current_time_obj.strftime("%H%M")
    
    batch_label = "1145" if current_hour < 13 else "1515"
    
    file_name = f"data_{year}{month}{day}_{time_suffix}.json"
    s3_key = f"raw/year={year}/month={month}/day={day}/batch={batch_label}/{file_name}"
    
    json_lines_body = "\n".join([json.dumps(record, ensure_ascii=False) for record in scraped_data])
    
    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(
            Bucket=BUCKET_RAW_NAME,
            Key=s3_key,
            Body=json_lines_body,
            ContentType='application/json'
        )
        print(f"Đã upload file thô sạch lên S3: s3://{BUCKET_RAW_NAME}/{s3_key}")
        return {
            'statusCode': 200,
            'body': json.dumps(f'Ingestion Success: {s3_key} với {len(scraped_data)} bản ghi.')
        }
    except Exception as s3_err:
        print(f"S3 Upload Error: {s3_err}")
        return {'statusCode': 500, 'body': json.dumps('S3 Upload Failed')}