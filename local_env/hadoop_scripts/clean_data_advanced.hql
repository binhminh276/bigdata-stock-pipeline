CREATE DATABASE IF NOT EXISTS bigdata_bank;
USE bigdata_bank;

-- 1. BẢNG RAW
DROP TABLE IF EXISTS raw_banks;
CREATE EXTERNAL TABLE raw_banks (
    symbol STRING,          -- Cột 1: ACB
    trading_date STRING,    -- Cột 2: 2026-06-12
    scrape_time STRING,     -- Cột 3: 2026-06-15 09:00:05.0
    source STRING,          -- Cột 4: CafeF
    close_price STRING,     -- Cột 5: 22.83
    volume BIGINT,          -- Cột 6: 61192641
    open_price STRING,      -- Cột 7: 23.00
    high_price STRING,      -- Cột 8: 23.35
    low_price STRING        -- Cột 9: 22.83
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

-- 3. TIỀN XỬ LÝ VÀ LỌC TRÙNG
WITH ParsedData AS (
    SELECT
        UPPER(TRIM(symbol)) AS Symbol,
        CAST(FROM_UNIXTIME(CAST(CAST(trading_date AS BIGINT) / 1000 AS BIGINT)) AS DATE) AS Trading_Date,
        CAST(FROM_UNIXTIME(CAST(CAST(scrape_time AS BIGINT) / 1000 AS BIGINT)) AS TIMESTAMP) AS Scrape_Time,
        source AS Source,
        CAST(close_price AS DOUBLE) AS Close,
        volume AS Volume,
        CAST(open_price AS DOUBLE) AS Open,
        CAST(high_price AS DOUBLE) AS High,
        CAST(low_price AS DOUBLE) AS Low,

        ROW_NUMBER() OVER (
            PARTITION BY
                UPPER(TRIM(symbol)),
                CAST(FROM_UNIXTIME(CAST(CAST(trading_date AS BIGINT) / 1000 AS BIGINT)) AS DATE),
                CAST(FROM_UNIXTIME(CAST(CAST(scrape_time AS BIGINT) / 1000 AS BIGINT)) AS TIMESTAMP)
            ORDER BY CAST(FROM_UNIXTIME(CAST(CAST(scrape_time AS BIGINT) / 1000 AS BIGINT)) AS TIMESTAMP) DESC
        ) as row_num
    FROM raw_banks
    WHERE symbol IS NOT NULL
      AND trading_date IS NOT NULL
      AND close_price NOT IN ('', 'NULL', 'null') 
      AND close_price IS NOT NULL
)

-- Làm sạch logic và đưa vào Parquet
INSERT INTO TABLE cleaned_banks
SELECT Symbol, Trading_Date, Scrape_Time, Source, Close, Volume, Open, High, Low
FROM ParsedData
WHERE row_num = 1
  AND Volume > 0 AND Close > 0 AND Open > 0 AND High > 0 AND Low > 0
  AND High >= Low AND High >= Close AND Low <= Close;

-- 4. KẾT XUẤT ĐỒNG BỘ RA FILE CSV SẠCH
DROP TABLE IF EXISTS cleaned_banks_csv;
CREATE EXTERNAL TABLE cleaned_banks_csv (
    Symbol STRING,
    Trading_Date STRING,    
    Scrape_Time STRING,     
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

INSERT OVERWRITE TABLE cleaned_banks_csv
SELECT 
    Symbol, 
    CAST(Trading_Date AS STRING), 
    CAST(Scrape_Time AS STRING), 
    Source, Close, Volume, Open, High, Low 
FROM cleaned_banks
WHERE Trading_Date IS NOT NULL;
