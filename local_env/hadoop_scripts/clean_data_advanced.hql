CREATE DATABASE IF NOT EXISTS bigdata_bank;
USE bigdata_bank;

-- 1. BẢNG EXTERNAL
DROP TABLE IF EXISTS raw_banks;
CREATE EXTERNAL TABLE raw_banks (
    symbol STRING,
    trading_date STRING,
    scrape_time STRING,
    source STRING,
    close_price STRING,
    volume BIGINT,
    open_price STRING,
    high_price STRING,
    low_price STRING
)
STORED AS PARQUET
LOCATION '/user/hadoop/stock_raw/';

-- 2. BẢNG CLEANED
CREATE TABLE IF NOT EXISTS cleaned_banks (
    Symbol STRING,
    Trading_Date DATE,        
    Scrape_Time TIMESTAMP,    
    Source STRING,
    Close DOUBLE,             
    Volume BIGINT,            
    Open DOUBLE,              
    High DOUBLE,              
    Low DOUBLE                
)
STORED AS PARQUET
LOCATION '/user/hadoop/stock_cleaned/';

-- 3. TIỀN XỬ LÝ
WITH ParsedData AS (
    SELECT
        UPPER(TRIM(symbol)) AS Symbol,
        TO_DATE(FROM_UNIXTIME(CAST(CAST(trading_date AS BIGINT) / 1000 AS BIGINT))) AS Trading_Date,
        FROM_UNIXTIME(CAST(CAST(scrape_time AS BIGINT) / 1000 AS BIGINT)) AS Scrape_Time,
        source AS Source,
        CAST(close_price AS DOUBLE) AS Close,
        CAST(volume AS BIGINT) AS Volume,
        CAST(open_price AS DOUBLE) AS Open,
        CAST(high_price AS DOUBLE) AS High,
        CAST(low_price AS DOUBLE) AS Low,
        
        ROW_NUMBER() OVER (
            PARTITION BY
                UPPER(TRIM(symbol)),
                TO_DATE(FROM_UNIXTIME(CAST(CAST(trading_date AS BIGINT) / 1000 AS BIGINT))),
                FROM_UNIXTIME(CAST(CAST(scrape_time AS BIGINT) / 1000 AS BIGINT))
            ORDER BY CAST(scrape_time AS BIGINT) DESC
        ) as row_num
    FROM raw_banks
    WHERE symbol IS NOT NULL
      AND trading_date IS NOT NULL
)

-- Bắt buộc dùng INSERT INTO để lưu dồn tích lũy lịch sử
INSERT INTO TABLE cleaned_banks
SELECT 
    Symbol, Trading_Date, Scrape_Time, Source, Close, Volume, Open, High, Low
FROM ParsedData
WHERE row_num = 1                  
  AND Volume > 0                   
  AND Close > 0              
  AND Open > 0
  AND High > 0
  AND Low > 0
  AND High >= Low      
  AND High >= Close 
  AND Low <= Close;

-- PHẦN BỔ SUNG: TẠO BẢN SAO DỮ LIỆU SẠCH DẠNG CSV CHO MAPREDUCE
DROP TABLE IF EXISTS cleaned_banks_csv;
CREATE EXTERNAL TABLE cleaned_banks_csv (
    Symbol STRING,
    Trading_Date DATE,        
    Scrape_Time TIMESTAMP,    
    Source STRING,
    Close DOUBLE,             
    Volume BIGINT,            
    Open DOUBLE,              
    High DOUBLE,              
    Low DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/stock_cleaned_csv/';

-- Đổ dữ liệu từ bảng Parquet sang bảng CSV
INSERT OVERWRITE TABLE cleaned_banks_csv
SELECT * FROM cleaned_banks;
