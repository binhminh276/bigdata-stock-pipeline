-- Tạo Database
CREATE DATABASE IF NOT EXISTS bigdata_stock;
USE bigdata_stock;

-- ====================================================================
-- BẢNG DANH MỤC: Quản lý danh sách các mã ngân hàng cần cào (CRUD từ Web)
-- Bảng này liên kết trực tiếp với giao diện Web Streamlit của Thưởng
-- ====================================================================
CREATE TABLE IF NOT EXISTS tbl_bank_list (
    id INT AUTO_INCREMENT,
    symbol VARCHAR(10) NOT NULL,
    bank_name VARCHAR(100),
    source VARCHAR(50), -- Nền tảng cào dữ liệu (CafeF, TCBS, FireAnt)
    status INT DEFAULT 1, -- 1: Đang kích hoạt cào, 0: Tạm ngưng cào
    PRIMARY KEY (id),
    UNIQUE KEY (symbol) -- Đảm bảo không bị trùng lặp mã ngân hàng
);

-- BƠM ĐẦY ĐỦ 20 MÃ NGÂN HÀNG THEO NGUỒN CHUẨN CỦA NHÓM
INSERT INTO tbl_bank_list (symbol, bank_name, source, status) VALUES
-- Nhóm 1: Nguồn cào CafeF (8 Mã)
('VCB', 'Vietcombank', 'CafeF', 1),
('BID', 'BIDV', 'CafeF', 1),
('CTG', 'VietinBank', 'CafeF', 1),
('MBB', 'MBBank', 'CafeF', 1),
('TCB', 'Techcombank', 'CafeF', 1),
('VPB', 'VPBank', 'CafeF', 1),
('ACB', 'ACB', 'CafeF', 1),
('STB', 'Sacombank', 'CafeF', 1),

-- Nhóm 2: Nguồn cào TCBS (8 Mã)
('SHB', 'SHB', 'TCBS', 1),
('HDB', 'HDBank', 'TCBS', 1),
('VIB', 'VIB', 'TCBS', 1),
('TPB', 'TPBank', 'TCBS', 1),
('EIB', 'Eximbank', 'TCBS', 1),
('MSB', 'MSB', 'TCBS', 1),
('SSB', 'SeABank', 'TCBS', 1),
('LPB', 'LPBank', 'TCBS', 1),

-- Nhóm 3: Nguồn cào FireAnt (4 Mã)
('OCB', 'OCB', 'FireAnt', 1),
('NAB', 'NamA_Bank', 'FireAnt', 1),
('KLB', 'KienLongBank', 'FireAnt', 1),
('BVB', 'BaoVietBank', 'FireAnt', 1);

-- ====================================================================
-- BẢNG 1: Chứa dữ liệu thô (Duy cào về đổ vào đây, An dùng Sqoop kéo đi)
-- Đã chuẩn hóa tên cột viết hoa chữ cái đầu để đồng bộ luồng Big Data
-- ====================================================================
CREATE TABLE IF NOT EXISTS tbl_raw_stock (
    Symbol VARCHAR(10) NOT NULL,
    Trading_Date DATE NOT NULL,
    Scrape_Time DATETIME NOT NULL,
    Source VARCHAR(50),
    Close DECIMAL(10, 2) NOT NULL,
    Volume BIGINT NOT NULL,
    Open DECIMAL(10, 2),
    High DECIMAL(10, 2),
    Low DECIMAL(10, 2),
    PRIMARY KEY (Symbol, Scrape_Time, Trading_Date)   
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