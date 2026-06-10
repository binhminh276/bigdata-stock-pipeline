
"""
database.py — DatabaseManager v3
Kết nối thực tế theo local_env/frontend/visualization/data_access.py

Drill REST  → http://{DRILL_HOST}:8047/query.json
HDFS path   → /user/hadoop/stock_cleaned_csv   (CSV không header, dùng columns[N])
MySQL path  → bigdata_stock.tbl_raw_stock / tbl_stock_daily_analysis / tbl_bank_list

DB_MODE=drill  → đọc HDFS qua Drill REST (production, đang chạy thật)
DB_MODE=mysql  → đọc MySQL trực tiếp qua SQLAlchemy
DB_MODE=dummy  → sinh data giả (dev offline)

Mặc định: DB_MODE=drill
"""

from __future__ import annotations

import os
import logging
import requests
from typing import Optional
from datetime import date, timedelta, datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ─── Chế độ kết nối ────────────────────────────────────────────────────────
DB_MODE: str = os.getenv("DB_MODE", "drill")

# ─── Thông số kết nối (đặt trong .env hoặc giữ default) ───────────────────
DRILL_HOST:  str = os.getenv("DRILL_HOST",  "100.80.217.65")
DRILL_PORT:  str = os.getenv("DRILL_PORT",  "8047")

# HDFS paths (khớp workflow_tutorial.txt bước 3)
HDFS_CLEANED_CSV: str = os.getenv(
    "HDFS_CLEANED_CSV", "/user/hadoop/stock_cleaned_csv"
)
HDFS_RESULT_DIR: str = os.getenv(
    "HDFS_RESULT_DIR", "/user/hadoop/stock_result"
)

# MySQL (dùng khi DB_MODE=mysql hoặc khi Drill cần ghi DML)
MYSQL_HOST:  str = os.getenv("DB_HOST",     "127.0.0.1")
MYSQL_PORT:  str = os.getenv("DB_PORT",     "3306")
MYSQL_USER:  str = os.getenv("DB_USER",     "root")
MYSQL_PASS:  str = os.getenv("DB_PASSWORD", "123456")
MYSQL_DB:    str = os.getenv("DB_NAME",     "bigdata_stock")

# ─── Meta 4 mã chính ───────────────────────────────────────────────────────
BANK_META: dict[str, dict] = {
    "ACB": {"name": "Asia Commercial Bank",   "base": 24_500},
    "STB": {"name": "Sacombank",              "base": 32_100},
    "OCB": {"name": "Orient Commercial Bank", "base": 13_800},
    "LPB": {"name": "LienVietPostBank",       "base": 16_200},
    "VCB": {"name": "Vietcombank",            "base": 82_500},
    "BID": {"name": "BIDV",                   "base": 47_300},
    "CTG": {"name": "VietinBank",             "base": 38_900},
    "MBB": {"name": "MB Bank",               "base": 24_600},
    "TCB": {"name": "Techcombank",            "base": 35_200},
}


# ─────────────────────────────────────────────────────────────────────────────
class DatabaseManager:
    """
    Một class, ba backend.
    Swap bằng biến môi trường DB_MODE — không cần đổi code GUI.
    """

    def __init__(self) -> None:
        self._mode = DB_MODE
        self._drill_url = f"http://{DRILL_HOST}:{DRILL_PORT}/query.json"
        self._engine = None   # SQLAlchemy engine — chỉ tạo khi cần

        if self._mode == "mysql":
            self._engine = self._make_mysql_engine()

    # ═══════════════════════════════════════════════════════
    # Kết nối nội bộ
    # ═══════════════════════════════════════════════════════

    def _make_mysql_engine(self):
        try:
            from sqlalchemy import create_engine
            url = (
                f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}"
                f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
            )
            return create_engine(url, pool_pre_ping=True, pool_recycle=1800)
        except Exception as exc:
            logger.error("MySQL engine lỗi: %s → fallback dummy", exc)
            self._mode = "dummy"
            return None

    def _mysql_engine(self):
        """Lazy-init engine — dùng cho DML từ crud.py dù đang ở drill mode."""
        if self._engine is None:
            self._engine = self._make_mysql_engine()
        return self._engine

    # ═══════════════════════════════════════════════════════
    # Drill REST — hàm gốc từ data_access.py
    # ═══════════════════════════════════════════════════════

    def _drill(self, sql: str) -> pd.DataFrame:
        """
        Gửi SQL tới Drill REST /query.json.
        Khớp hoàn toàn với data_access.DataAccessLayer.load_data().
        """
        try:
            resp = requests.post(
                self._drill_url,
                json={"queryType": "SQL", "query": sql},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error("Drill %s: %s", resp.status_code, resp.text[:300])
                return pd.DataFrame()
            rows = resp.json().get("rows", [])
            if not rows:
                logger.warning("Drill trả về 0 rows cho query: %s", sql[:80])
                return pd.DataFrame()
            return pd.DataFrame(rows)
        except requests.exceptions.ConnectionError:
            logger.error("Không kết nối được Drill tại %s", self._drill_url)
            return pd.DataFrame()
        except Exception as exc:
            logger.error("Drill lỗi: %s", exc)
            return pd.DataFrame()

    # ═══════════════════════════════════════════════════════
    # MySQL query (read)
    # ═══════════════════════════════════════════════════════

    def _mysql(self, sql: str, params: dict | None = None) -> pd.DataFrame:
        from sqlalchemy import text
        try:
            with self._mysql_engine().connect() as conn:
                return pd.read_sql(text(sql), conn, params=params or {})
        except Exception as exc:
            logger.error("MySQL query lỗi: %s", exc)
            return pd.DataFrame()

    # ═══════════════════════════════════════════════════════
    # Public execute — cho crud.py (INSERT/UPDATE/DELETE)
    # Drill là read-only middleware → DML luôn đi thẳng MySQL
    # ═══════════════════════════════════════════════════════

    def execute(self, sql: str, params: dict | None = None) -> bool:
        if self._mode == "dummy":
            logger.info("[dummy] execute bỏ qua: %s", sql[:60])
            return True
        from sqlalchemy import text
        try:
            with self._mysql_engine().begin() as conn:
                conn.execute(text(sql), params or {})
            return True
        except Exception as exc:
            logger.error("Execute lỗi: %s", exc)
            return False

    # ═══════════════════════════════════════════════════════
    # Health check
    # ═══════════════════════════════════════════════════════

    def connect(self) -> bool:
        if self._mode == "dummy":
            return True
        if self._mode == "drill":
            try:
                r = requests.post(
                    self._drill_url,
                    json={"queryType": "SQL", "query": "SELECT 1 FROM sys.version"},
                    headers={"Content-Type": "application/json"},
                    timeout=5,
                )
                return r.status_code == 200
            except Exception:
                return False
        if self._mode == "mysql":
            try:
                from sqlalchemy import text
                with self._mysql_engine().connect() as c:
                    c.execute(text("SELECT 1"))
                return True
            except Exception:
                return False
        return False

    # ═══════════════════════════════════════════════════════
    # HDFS Drill query helpers
    # Schema HDFS (không có header, theo data_access.py):
    #   [0]=symbol  [1]=trading_date  [2]=scrape_time  [3]=source
    #   [4]=close   [5]=volume        [6]=open         [7]=high  [8]=low
    # ═══════════════════════════════════════════════════════

    def _hdfs_raw_sql(
        self,
        symbols:    list[str] | None,
        start_date: date | None,
        end_date:   date | None,
    ) -> str:
        """
        Tạo SQL đọc HDFS cleaned CSV qua Drill.
        Giống hệt data_access.py nhưng thêm WHERE lọc symbol và ngày.
        Drill dùng CAST(columns[N] AS type) để ép kiểu.
        """
        where_parts = []

        # Lọc symbol
        if symbols:
            sym_list = ", ".join(f"'{s}'" for s in symbols)
            where_parts.append(f"columns[0] IN ({sym_list})")

        # Lọc ngày
        if start_date:
            where_parts.append(
                f"CAST(columns[1] AS DATE) >= DATE '{start_date}'"
            )
        if end_date:
            where_parts.append(
                f"CAST(columns[1] AS DATE) <= DATE '{end_date}'"
            )

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        return f"""
        SELECT
            columns[0] AS symbol,
            columns[1] AS trading_date,
            columns[2] AS scrape_time,
            columns[3] AS source,
            CAST(columns[4] AS DOUBLE) AS close,
            CAST(columns[5] AS BIGINT) AS volume,
            CAST(columns[6] AS DOUBLE) AS open,
            CAST(columns[7] AS DOUBLE) AS high,
            CAST(columns[8] AS DOUBLE) AS low
        FROM table(
            dfs.`{HDFS_CLEANED_CSV}`
            (type => 'text', fieldDelimiter => ',', extractHeader => false)
        )
        {where_clause}
        ORDER BY columns[0], columns[1]
        """

    @staticmethod
    def _dedup_latest(df: pd.DataFrame) -> pd.DataFrame:
        """
        Lọc bản ghi mới nhất theo scrape_time cho mỗi (symbol, trading_date).
        Đúng với logic business_logic.py: chỉ giữ scrape_time MAX của ngày.
        """
        if df.empty:
            return df
        df = df.copy()
        df["scrape_time"] = pd.to_datetime(df["scrape_time"], errors="coerce")
        df["trading_date"] = pd.to_datetime(df["trading_date"], errors="coerce")
        df = (
            df.sort_values("scrape_time")
              .drop_duplicates(subset=["symbol", "trading_date"], keep="last")
              .sort_values(["symbol", "trading_date"])
              .reset_index(drop=True)
        )
        # Rename về chuẩn của GUI
        df = df.rename(columns={"trading_date": "date"})
        # Ép kiểu số
        for col in ("close", "open", "high", "low"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        return df

    # ═══════════════════════════════════════════════════════
    # PUBLIC API — get_raw_data
    # ═══════════════════════════════════════════════════════

    def get_raw_data(
        self,
        symbols:    list[str] | None = None,
        start_date: date | None      = None,
        end_date:   date | None      = None,
    ) -> pd.DataFrame:
        """
        Đọc dữ liệu OHLCV.
        drill  → HDFS /user/hadoop/stock_cleaned_csv qua Drill REST
        mysql  → tbl_raw_stock
        dummy  → sinh ngẫu nhiên
        """
        if self._mode == "dummy":
            return self._gen_raw(symbols, start_date, end_date)

        if self._mode == "drill":
            sql = self._hdfs_raw_sql(symbols, start_date, end_date)
            df  = self._drill(sql)
            return self._dedup_latest(df)

        # mysql mode
        sym_w, p = self._sym_where(symbols)
        dt_w,  p = self._date_where(p, start_date, end_date, "Trading_Date")
        sql = f"""
            SELECT Symbol AS symbol, Trading_Date AS date,
                   Open AS open, High AS high, Low AS low,
                   Close AS close, Volume AS volume, Source AS source
            FROM tbl_raw_stock
            WHERE 1=1 {sym_w} {dt_w}
            ORDER BY Symbol, Trading_Date
        """
        df = self._mysql(sql, p)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df

    # ═══════════════════════════════════════════════════════
    # PUBLIC API — get_analysis_data
    # Đọc tbl_stock_daily_analysis (kết quả MapReduce đổ từ Sqoop Export)
    # Drill đọc MySQL table qua storage plugin "mysql_db"
    # ═══════════════════════════════════════════════════════

    def get_analysis_data(
        self,
        symbols:    list[str] | None = None,
        start_date: date | None      = None,
        end_date:   date | None      = None,
    ) -> pd.DataFrame:
        if self._mode == "dummy":
            return self._gen_analysis(symbols, start_date, end_date)

        sym_w, p = self._sym_where(symbols)
        dt_w,  p = self._date_where(p, start_date, end_date, "calc_date")

        sql_body = f"""
            SELECT symbol,
                   calc_date                    AS date,
                   total_volume,
                   max_close_price,
                   min_close_price,
                   up_days_count,
                   down_days_count,
                   max_intraday_volatility       AS price_variance,
                   liquidity_status,
                   max_intraday_drop,
                   sma_price
            FROM tbl_stock_daily_analysis
            WHERE 1=1 {sym_w} {dt_w}
            ORDER BY symbol, calc_date
        """

        if self._mode == "drill":
            sql = sql_body.replace(
                "FROM tbl_stock_daily_analysis",
                f"FROM mysql_db.`{MYSQL_DB}`.tbl_stock_daily_analysis",
            )
            sql = self._bind(sql, p)
            df  = self._drill(sql)
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            return df

        df = self._mysql(sql_body, p)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df

    # ═══════════════════════════════════════════════════════
    # PUBLIC API — get_monthly_analysis
    # ═══════════════════════════════════════════════════════

    def get_monthly_analysis(
        self,
        symbols: list[str] | None = None,
        year:    int | None       = None,
    ) -> pd.DataFrame:
        if self._mode == "dummy":
            return self._gen_monthly(symbols)

        sym_w, p = self._sym_where(symbols)
        yr_w = ""
        if year:
            yr_w = " AND calc_year = :year"
            p["year"] = year

        sql_body = f"""
            SELECT symbol, calc_year, calc_month,
                   monthly_avg_close, monthly_total_volume
            FROM tbl_stock_monthly_analysis
            WHERE 1=1 {sym_w} {yr_w}
            ORDER BY symbol, calc_year, calc_month
        """

        if self._mode == "drill":
            sql = sql_body.replace(
                "FROM tbl_stock_monthly_analysis",
                f"FROM mysql_db.`{MYSQL_DB}`.tbl_stock_monthly_analysis",
            )
            sql = self._bind(sql, p)
            return self._drill(sql)

        return self._mysql(sql_body, p)

    # ═══════════════════════════════════════════════════════
    # PUBLIC API — get_summary (KPI cards)
    # ═══════════════════════════════════════════════════════

    def get_summary(self) -> pd.DataFrame:
        if self._mode == "dummy":
            return self._gen_summary()

        sql_body = """
            SELECT a.symbol,
                   a.max_close_price              AS price,
                   a.total_volume,
                   a.max_intraday_volatility       AS price_variance,
                   a.calc_date
            FROM tbl_stock_daily_analysis a
            INNER JOIN (
                SELECT symbol, MAX(calc_date) AS latest
                FROM tbl_stock_daily_analysis
                GROUP BY symbol
            ) t ON a.symbol = t.symbol AND a.calc_date = t.latest
            ORDER BY a.symbol
        """

        if self._mode == "drill":
            sql = sql_body.replace(
                "FROM tbl_stock_daily_analysis",
                f"FROM mysql_db.`{MYSQL_DB}`.tbl_stock_daily_analysis",
            ).replace(
                "tbl_stock_daily_analysis\n            ) t",
                f"mysql_db.`{MYSQL_DB}`.tbl_stock_daily_analysis\n            ) t",
            )
            return self._drill(sql)

        return self._mysql(sql_body)

    # ═══════════════════════════════════════════════════════
    # PUBLIC API — tbl_bank_list (cho crud.py)
    # Luôn đọc từ MySQL dù đang ở drill mode
    # ═══════════════════════════════════════════════════════

    def get_bank_list(self) -> pd.DataFrame:
        if self._mode == "dummy":
            return self._gen_bank_list()

        sql = """
            SELECT id, symbol, bank_name, source, status
            FROM tbl_bank_list
            ORDER BY id
        """
        # Drill không có schema tbl_bank_list → đọc thẳng MySQL
        df = self._mysql(sql)
        if df.empty:
            logger.warning("tbl_bank_list rỗng → trả dummy")
            return self._gen_bank_list()
        return df

    # ═══════════════════════════════════════════════════════
    # Alias / compat
    # ═══════════════════════════════════════════════════════

    def get_stock_by_ticker(
        self, symbol: str, days: int = 365, use_analysis: bool = False,
    ) -> pd.DataFrame:
        end   = date.today()
        start = end - timedelta(days=days)
        return (self.get_analysis_data([symbol], start, end)
                if use_analysis else self.get_raw_data([symbol], start, end))

    def get_all_data(
        self,
        symbols:    list[str] | None = None,
        start_date: date | None      = None,
        end_date:   date | None      = None,
    ) -> pd.DataFrame:
        df = self.get_raw_data(symbols, start_date, end_date)
        if "symbol" in df.columns:
            df = df.rename(columns={"symbol": "ticker"})
        return df

    def get_volatility_top(self, n: int = 10) -> pd.DataFrame:
        if self._mode == "dummy":
            raw = self._gen_raw()
            raw["pct"] = raw.groupby("symbol")["close"].pct_change().abs()
            return raw.nlargest(n, "pct")[["date","symbol","close","pct"]].reset_index(drop=True)

        sql_body = f"""
            SELECT calc_date AS date, symbol,
                   max_close_price                   AS close,
                   max_intraday_volatility / NULLIF(max_close_price, 0) AS pct
            FROM tbl_stock_daily_analysis
            ORDER BY max_intraday_volatility DESC
            LIMIT {int(n)}
        """
        if self._mode == "drill":
            sql = sql_body.replace(
                "FROM tbl_stock_daily_analysis",
                f"FROM mysql_db.`{MYSQL_DB}`.tbl_stock_daily_analysis",
            )
            return self._drill(sql)
        return self._mysql(sql_body)

    # ═══════════════════════════════════════════════════════
    # WHERE clause helpers (mysql named params)
    # ═══════════════════════════════════════════════════════

    @staticmethod
    def _sym_where(symbols: list[str] | None) -> tuple[str, dict]:
        if not symbols:
            return "", {}
        ph  = ",".join(f":s{i}" for i in range(len(symbols)))
        par = {f"s{i}": s for i, s in enumerate(symbols)}
        return f"AND symbol IN ({ph})", par

    @staticmethod
    def _date_where(
        params: dict,
        start:  date | None,
        end:    date | None,
        col:    str = "date",
    ) -> tuple[str, dict]:
        c = ""
        if start:
            c += f" AND {col} >= :start"; params["start"] = start
        if end:
            c += f" AND {col} <= :end";   params["end"]   = end
        return c, params

    @staticmethod
    def _bind(sql: str, params: dict) -> str:
        """Thay named params thành literal — chỉ dùng cho Drill (không hỗ trợ :param)."""
        for k, v in params.items():
            if isinstance(v, str):
                sql = sql.replace(f":{k}", f"'{v.replace(chr(39), chr(39)*2)}'")
            elif isinstance(v, (date, datetime)):
                sql = sql.replace(f":{k}", f"'{v}'")
            else:
                sql = sql.replace(f":{k}", str(v))
        return sql

    # ═══════════════════════════════════════════════════════
    # Dummy generators (dev offline / fallback)
    # ═══════════════════════════════════════════════════════

    def _gen_raw(
        self,
        symbols:    list[str] | None = None,
        start_date: date | None      = None,
        end_date:   date | None      = None,
    ) -> pd.DataFrame:
        target = symbols or list(BANK_META.keys())
        end    = end_date   or date.today()
        start  = start_date or (end - timedelta(days=365 * 5))
        frames = []
        for sym in target:
            meta = BANK_META.get(sym, {"base": 20_000})
            np.random.seed(abs(hash(sym)) % 99_999)
            dates = pd.bdate_range(str(start), str(end))
            n = len(dates)
            if n == 0:
                continue
            ret    = np.random.normal(0.00025, 0.015, n)
            prices = np.maximum(meta["base"] * np.exp(np.cumsum(ret)), 1_000)
            vol    = (np.random.randint(500_000, 6_000_000, n) *
                      (1 + np.abs(ret) * 4)).astype(int)
            high   = prices * (1 + np.abs(np.random.normal(0, 0.007, n)))
            low    = prices * (1 - np.abs(np.random.normal(0, 0.007, n)))
            op     = np.roll(prices, 1); op[0] = prices[0]
            frames.append(pd.DataFrame({
                "symbol": sym, "date": dates,
                "open": op.round(0), "high": high.round(0),
                "low": low.round(0), "close": prices.round(0),
                "volume": vol, "source": "dummy",
            }))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _gen_analysis(
        self,
        symbols:    list[str] | None = None,
        start_date: date | None      = None,
        end_date:   date | None      = None,
    ) -> pd.DataFrame:
        raw = self._gen_raw(symbols, start_date, end_date)
        if raw.empty:
            return raw
        g = raw.groupby(["symbol", "date"])
        r = g.agg(
            total_volume=("volume","sum"),
            max_close_price=("close","max"),
            min_close_price=("close","min"),
            _hi=("high","max"), _lo=("low","min"),
        ).reset_index()
        r["price_variance"]         = r["_hi"] - r["_lo"]
        r["max_intraday_volatility"] = r["price_variance"]
        r["sma_price"]              = r["max_close_price"].rolling(20, min_periods=1).mean()
        r["up_days_count"]          = 0
        r["down_days_count"]        = 0
        r["liquidity_status"]       = r["total_volume"].apply(
            lambda v: "Cao" if v > 3_000_000 else ("Vừa" if v > 1_000_000 else "Thấp")
        )
        r["max_intraday_drop"]      = r["price_variance"] * 0.6
        return r.drop(columns=["_hi","_lo"]).rename(columns={"date": "calc_date"})

    def _gen_monthly(self, symbols: list[str] | None = None) -> pd.DataFrame:
        raw = self._gen_raw(symbols)
        if raw.empty:
            return raw
        raw = raw.copy()
        raw["calc_year"]  = pd.to_datetime(raw["date"]).dt.year
        raw["calc_month"] = pd.to_datetime(raw["date"]).dt.month
        return (
            raw.groupby(["symbol", "calc_year", "calc_month"])
               .agg(monthly_avg_close=("close","mean"),
                    monthly_total_volume=("volume","sum"))
               .reset_index()
        )

    def _gen_summary(self) -> pd.DataFrame:
        rows = []
        for sym, meta in BANK_META.items():
            np.random.seed(abs(hash(sym)) % 99_999)
            price = meta["base"] * (1 + np.random.normal(0, 0.012))
            rows.append({
                "symbol":       sym,
                "name":         meta["name"],
                "price":        round(price, 0),
                "pct_change":   round(np.random.normal(0.4, 2.2), 2),
                "total_volume": int(np.random.randint(800_000, 5_000_000)),
                "price_variance": round(price * np.random.uniform(0.01, 0.04), 0),
                "sma20":        round(price * np.random.uniform(0.97, 1.03), 0),
                "sma50":        round(price * np.random.uniform(0.95, 1.05), 0),
                "max_close":    round(price * np.random.uniform(1.05, 1.22), 0),
                "min_close":    round(price * np.random.uniform(0.78, 0.95), 0),
            })
        return pd.DataFrame(rows)

    @staticmethod
    def _gen_bank_list() -> pd.DataFrame:
        return pd.DataFrame([
            {"id":1,"symbol":"VCB","bank_name":"Vietcombank","source":"CafeF","status":1},
            {"id":2,"symbol":"BID","bank_name":"BIDV","source":"CafeF","status":1},
            {"id":3,"symbol":"CTG","bank_name":"VietinBank","source":"CafeF","status":1},
            {"id":4,"symbol":"MBB","bank_name":"MBBank","source":"CafeF","status":1},
            {"id":5,"symbol":"TCB","bank_name":"Techcombank","source":"CafeF","status":1},
            {"id":6,"symbol":"VPB","bank_name":"VPBank","source":"CafeF","status":1},
            {"id":7,"symbol":"ACB","bank_name":"ACB","source":"CafeF","status":1},
            {"id":8,"symbol":"STB","bank_name":"Sacombank","source":"CafeF","status":1},
            {"id":9,"symbol":"SHB","bank_name":"SHB","source":"TCBS","status":1},
            {"id":10,"symbol":"HDB","bank_name":"HDBank","source":"TCBS","status":1},
            {"id":11,"symbol":"VIB","bank_name":"VIB","source":"TCBS","status":0},
            {"id":12,"symbol":"TPB","bank_name":"TPBank","source":"TCBS","status":1},
            {"id":13,"symbol":"EIB","bank_name":"Eximbank","source":"TCBS","status":0},
            {"id":14,"symbol":"MSB","bank_name":"MSB","source":"TCBS","status":1},
            {"id":15,"symbol":"SSB","bank_name":"SeABank","source":"TCBS","status":1},
            {"id":16,"symbol":"LPB","bank_name":"LPBank","source":"TCBS","status":1},
            {"id":17,"symbol":"OCB","bank_name":"OCB","source":"FireAnt","status":1},
            {"id":18,"symbol":"NAB","bank_name":"NamA Bank","source":"FireAnt","status":0},
            {"id":19,"symbol":"KLB","bank_name":"KienLong Bank","source":"FireAnt","status":1},
            {"id":20,"symbol":"BVB","bank_name":"Bao Viet Bank","source":"FireAnt","status":0},
        ])

    # ═══════════════════════════════════════════════════════
    # Properties
    # ═══════════════════════════════════════════════════════

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def bank_meta(self) -> dict:
        return BANK_META

    @property
    def available_symbols(self) -> list[str]:
        return list(BANK_META.keys())
