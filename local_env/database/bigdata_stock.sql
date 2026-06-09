-- Tạo Database
CREATE DATABASE IF NOT EXISTS bigdata_stock;
USE bigdata_stock;

-- ====================================================================
-- BẢNG 1: Chứa dữ liệu thô (Duy cào về đổ vào đây, An dùng Sqoop kéo đi)
-- ====================================================================
CREATE TABLE IF NOT EXISTS tbl_raw_stock (
    symbol VARCHAR(10) NOT NULL,
    trading_date DATE NOT NULL,
    scrape_time DATETIME NOT NULL,
    source VARCHAR(50),
    close_price DECIMAL(10, 2) NOT NULL,
    volume BIGINT NOT NULL,
    open_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    PRIMARY KEY (symbol, trading_date)
);

-- ====================================================================
-- BẢNG 2A: Kết quả phân tích theo NGÀY & TÍCH LŨY LỊCH SỬ
-- Phục vụ các nhiệm vụ: 5.1, 5.2, 5.3, 5.4, 5.6, 5.7, 5.8, 5.10
-- ====================================================================
DROP TABLE IF EXISTS tbl_stock_daily_analysis;
CREATE TABLE tbl_stock_daily_analysis (
    symbol VARCHAR(10) NOT NULL,          -- Mã CP (Nếu là nhiệm vụ 5.7 thì ghi chữ 'MARKET')
    calc_date DATE NOT NULL,              -- Ngày chốt tính toán
    total_volume BIGINT,                  -- [Bảo - 5.1] & [Duy - 5.7] Tổng Volume mã đó hoặc toàn thị trường
    max_close_price DECIMAL(10, 2),       -- [BiMi - 5.2] Giá đóng cửa cao nhất lịch sử tính đến ngày này
    min_close_price DECIMAL(10, 2),       -- [BiMi - 5.2] Giá đóng cửa thấp nhất lịch sử tính đến ngày này
    up_days_count INT,                    -- [Phúc An - 5.3] Số ngày tăng giá tích lũy
    down_days_count INT,                  -- [Phúc An - 5.3] Số ngày giảm giá tích lũy
    max_volume_date DATE,                 -- [Nhóm - 5.4] Ngày có khối lượng giao dịch kỷ lục
    max_volume_value BIGINT,              -- [Nhóm - 5.4] Giá trị khối lượng kỷ lục của ngày đó
    max_intraday_volatility DECIMAL(10,2),-- [Duy - 5.6] Biên độ dao động lớn nhất trong ngày (High - Low)
    liquidity_status VARCHAR(20),         -- [Phúc An - 5.8] Phân loại thanh khoản (Cao/Trung bình/Thấp)
    max_intraday_drop DECIMAL(10, 2),     -- [Bảo - 5.10] Giá trị sụt giảm lớn nhất trong một ngày
    sma_price DECIMAL(10, 2),             -- Đường SMA (Tính thêm phục vụ web đồ thị)
    PRIMARY KEY (symbol, calc_date)
);

-- ====================================================================
-- BẢNG 2B: Kết quả phân tích gom nhóm theo THÁNG
-- Phục vụ các nhiệm vụ: 5.5, 5.9
-- ====================================================================
DROP TABLE IF EXISTS tbl_stock_monthly_analysis;
CREATE TABLE tbl_stock_monthly_analysis (
    symbol VARCHAR(10) NOT NULL,
    calc_year INT NOT NULL,               -- Năm tính toán (Ví dụ: 2026)
    calc_month INT NOT NULL,              -- Tháng tính toán (Ví dụ: 6)
    monthly_avg_close DECIMAL(10, 2),     -- [Nhóm - 5.5] Trung bình giá đóng cửa của tháng đó
    monthly_total_volume BIGINT,          -- [BiMi - 5.9] Tổng khối lượng gom nhóm theo tháng
    PRIMARY KEY (symbol, calc_year, calc_month)
);