"""
database.py — SSH Tunnel qua subprocess (native ssh command)
Không dùng sshtunnel/paramiko — tránh lỗi DSSKey với paramiko mới.
"""

from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ── Bank metadata ─────────────────────────────────────────────────────────────
BANK_META = {
    "VCB": {"name": "Vietcombank",          "source": "CafeF"},
    "BID": {"name": "BIDV",                  "source": "CafeF"},
    "CTG": {"name": "Vietinbank",            "source": "CafeF"},
    "MBB": {"name": "MB Bank",               "source": "CafeF"},
    "TCB": {"name": "Techcombank",           "source": "CafeF"},
    "VPB": {"name": "VPBank",                "source": "CafeF"},
    "ACB": {"name": "Asia Comm. Bank",       "source": "CafeF"},
    "STB": {"name": "Sacombank",             "source": "CafeF"},
    "SHB": {"name": "SHB",                   "source": "TCBS"},
    "HDB": {"name": "HDBank",                "source": "TCBS"},
    "VIB": {"name": "VIB",                   "source": "TCBS"},
    "TPB": {"name": "TPBank",                "source": "TCBS"},
    "EIB": {"name": "Eximbank",              "source": "TCBS"},
    "MSB": {"name": "MSB",                   "source": "TCBS"},
    "SSB": {"name": "SeABank",               "source": "TCBS"},
    "LPB": {"name": "LienVietPostBank",      "source": "TCBS"},
    "OCB": {"name": "Orient Comm. Bank",     "source": "FireAnt"},
    "NAB": {"name": "Nam A Bank",            "source": "FireAnt"},
    "KLB": {"name": "Kien Long Bank",        "source": "FireAnt"},
    "BVB": {"name": "Viet Capital Bank",     "source": "FireAnt"},
}

SOURCE_GROUPS = {
    "CafeF":   [s for s, m in BANK_META.items() if m["source"] == "CafeF"],
    "TCBS":    [s for s, m in BANK_META.items() if m["source"] == "TCBS"],
    "FireAnt": [s for s, m in BANK_META.items() if m["source"] == "FireAnt"],
}

SOURCE_BADGE = {
    "CafeF":   "badge-blue",
    "TCBS":    "badge-purple",
    "FireAnt": "badge-cyan",
}


def get_dynamic_bank_meta():
    if "DYNAMIC_BANK_META" not in st.session_state:
        st.session_state.DYNAMIC_BANK_META = BANK_META.copy()
    return st.session_state.DYNAMIC_BANK_META


# ── SSH Tunnel via subprocess ─────────────────────────────────────────────────

class SSHTunnel:
    """SSH tunnel dùng native `ssh` command — không cần paramiko/sshtunnel."""

    def __init__(self):
        self._proc: subprocess.Popen | None = None
        self._lock = threading.Lock()
        self.local_port = int(os.getenv("TUNNEL_LOCAL_PORT", "3307"))

    def _find_pem(self) -> str:
        key = os.getenv("SSH_KEY_PATH", "bigdata_key.pem")
        candidates = [
            key,
            str(Path(__file__).parent / key),
            str(Path.home() / ".ssh" / key),
            str(Path.home() / key),
        ]
        for p in candidates:
            if Path(p).exists():
                # Path(p).chmod(0o600)  # chmod không dùng trên Windows
                return str(Path(p).resolve())
        raise FileNotFoundError(
            f"Không tìm thấy '{key}'. "
            "Đặt bigdata_key.pem cùng thư mục với app.py"
        )

    def _is_port_open(self) -> bool:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            result = s.connect_ex(("127.0.0.1", self.local_port))
            s.close()
            return result == 0
        except Exception:
            return False

    def start(self) -> int:
        """Khởi động tunnel, trả về local_port. Thread-safe."""
        with self._lock:
            # Nếu tunnel đang chạy và port còn mở → dùng lại
            if self._proc and self._proc.poll() is None and self._is_port_open():
                return self.local_port

            pem      = self._find_pem()
            ssh_host = os.getenv("SSH_HOST", "ec2-32-195-43-148.compute-1.amazonaws.com")
            ssh_user = os.getenv("SSH_USER", "ubuntu")
            ssh_port = os.getenv("SSH_PORT", "22")
            my_host  = os.getenv("MYSQL_HOST", "10.0.1.161")
            my_port  = os.getenv("MYSQL_PORT", "3306")

            cmd = [
                "ssh",
                "-i", pem,
                "-N",
                "-L", f"{self.local_port}:{my_host}:{my_port}",
                "-p", ssh_port,
                "-o", "StrictHostKeyChecking=no",
                "-o", "ExitOnForwardFailure=yes",
                "-o", "ServerAliveInterval=30",
                "-o", "ServerAliveCountMax=3",
                "-o", "ConnectTimeout=15",
                f"{ssh_user}@{ssh_host}",
            ]

            print(f"[SSHTunnel] Starting: 127.0.0.1:{self.local_port} → {my_host}:{my_port}")
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

            # Chờ port sẵn sàng (tối đa 12 giây)
            for i in range(12):
                time.sleep(1)
                if self._is_port_open():
                    print(f"[SSHTunnel] Ready after {i+1}s")
                    return self.local_port
                if self._proc.poll() is not None:
                    err = self._proc.stderr.read().decode().strip()
                    raise ConnectionError(f"SSH process exited early: {err}")

            err = self._proc.stderr.read(500).decode().strip()
            self._proc.kill()
            raise TimeoutError(f"Tunnel không mở sau 12s. SSH stderr: {err}")

    def stop(self):
        with self._lock:
            if self._proc:
                self._proc.kill()
                self._proc = None
                print("[SSHTunnel] Stopped.")


# Singleton tunnel dùng chung toàn app
_tunnel = SSHTunnel()


# ── DatabaseManager ───────────────────────────────────────────────────────────

class DatabaseManager:
    def __init__(self, mode: str = "drill", vm_ip: str | None = None):
        self.mode      = mode
        self.vm_ip     = vm_ip or os.getenv("DRILL_HOST", "100.80.217.65")
        self.drill_url = f"http://{self.vm_ip}:{os.getenv('DRILL_PORT', '8047')}/query.json"
        self.hdfs_path = os.getenv("HDFS_PATH", "/user/hadoop/stock_cleaned_csv/000000_0")
        self._engine   = None

    # ── Engine (lazy, tạo khi cần) ───────────────────────────────────────────

    def _get_engine(self):
        if self._engine is not None:
            return self._engine

        local_port = _tunnel.start()
        user  = os.getenv("MYSQL_USER",     "admin")
        pwd   = os.getenv("MYSQL_PASSWORD", "")
        db    = os.getenv("MYSQL_DB",       "bigdata_stock")

        conn_str = (
            f"mysql+mysqlconnector://{user}:{pwd}"
            f"@127.0.0.1:{local_port}/{db}?connection_timeout=10"
        )
        self._engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=3,
            max_overflow=5,
        )
        print(f"[DB] Engine → 127.0.0.1:{local_port}/{db}")
        return self._engine

    # ── Health check ─────────────────────────────────────────────────────────

    def connect(self) -> bool:
        # 1. Thử Drill
        try:
            r = requests.post(
                self.drill_url,
                json={"queryType": "SQL", "query": "SELECT 1"},
                headers={"Content-Type": "application/json"},
                timeout=3,
            )
            if r.status_code == 200:
                return True
        except Exception:
            pass

        # 2. Thử MySQL qua tunnel
        try:
            with self._get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"[connect] MySQL failed: {e}")
            return False

    # ── Drill query ───────────────────────────────────────────────────────────

    def _run_drill_query(self, sql: str) -> pd.DataFrame:
        try:
            r = requests.post(
                self.drill_url,
                json={"queryType": "SQL", "query": sql},
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if r.status_code == 200:
                return pd.DataFrame(r.json().get("rows", []))
            print("Drill lỗi:", r.text[:200])
        except Exception as e:
            print("Drill thất bại:", e)
        return pd.DataFrame()

    # ── MySQL query ───────────────────────────────────────────────────────────

    def _run_mysql_query(self, sql: str) -> pd.DataFrame:
        try:
            with self._get_engine().connect() as conn:
                return pd.read_sql(text(sql), conn)
        except Exception as e:
            print(f"[MySQL] {e}")
            return pd.DataFrame()

    # ── get_raw_data ──────────────────────────────────────────────────────────

    def get_raw_data(self, symbols=None, start_date=None, end_date=None) -> pd.DataFrame:
        df = self._get_raw_drill(symbols, start_date, end_date)
        if not df.empty:
            return df
        print("[get_raw_data] Drill empty → fallback MySQL")
        return self._get_raw_mysql(symbols, start_date, end_date)

    def _get_raw_drill(self, symbols, start_date, end_date) -> pd.DataFrame:
        sql = f"""
        SELECT
            columns[0] AS symbol, columns[1] AS trading_date,
            columns[2] AS scrape_time, columns[3] AS source,
            CAST(columns[4] AS DOUBLE) AS close,
            CAST(columns[5] AS DOUBLE) AS volume,
            CAST(columns[6] AS DOUBLE) AS open,
            CAST(columns[7] AS DOUBLE) AS high,
            CAST(columns[8] AS DOUBLE) AS low
        FROM table(dfs.`{self.hdfs_path}`
            (type => 'text', fieldDelimiter => ',', extractHeader => false))
        WHERE 1=1
        """
        if symbols:
            sym_str = ", ".join(f"'{s}'" for s in symbols)
            sql += f" AND columns[0] IN ({sym_str})"
        df = self._run_drill_query(sql)
        return self._normalize_raw(df, start_date, end_date) if not df.empty else df

    def _get_raw_mysql(self, symbols, start_date, end_date) -> pd.DataFrame:
        where = ["1=1"]
        if symbols:
            where.append(f"symbol IN ({', '.join(repr(s) for s in symbols)})")
        if start_date:
            where.append(f"trading_date >= '{start_date}'")
        if end_date:
            where.append(f"trading_date <= '{end_date}'")
        sql = f"""
            SELECT symbol, trading_date, scrape_time, source,
                   close, volume, open, high, low
            FROM bigdata_stock.tbl_raw_stock
            WHERE {' AND '.join(where)}
            ORDER BY trading_date DESC LIMIT 50000
        """
        df = self._run_mysql_query(sql)
        return self._normalize_raw(df, start_date, end_date) if not df.empty else df

    @staticmethod
    def _normalize_raw(df, start_date, end_date):
        df["trading_date"] = pd.to_datetime(df.get("trading_date", df.get("date")), errors="coerce")
        df = df.dropna(subset=["trading_date"]).rename(columns={"trading_date": "date"})
        for col in ["close", "volume", "open", "high", "low"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if start_date and end_date:
            s, e = pd.to_datetime(start_date).date(), pd.to_datetime(end_date).date()
            df = df[(df["date"].dt.date >= s) & (df["date"].dt.date <= e)]
        return df

    # ── get_analysis_data ─────────────────────────────────────────────────────

    def get_analysis_data(self, symbols=None, start_date=None, end_date=None) -> pd.DataFrame:
        sql = "SELECT * FROM mysql_db.bigdata_stock.tbl_stock_daily_analysis WHERE 1=1"
        if symbols:
            sql += f" AND symbol IN ({', '.join(repr(s) for s in symbols)})"
        df = self._run_drill_query(sql)
        if df.empty:
            where = ["1=1"]
            if symbols:
                where.append(f"symbol IN ({', '.join(repr(s) for s in symbols)})")
            df = self._run_mysql_query(
                f"SELECT * FROM bigdata_stock.tbl_stock_daily_analysis WHERE {' AND '.join(where)}"
            )
        if not df.empty:
            df["calc_date"] = pd.to_datetime(df["calc_date"], errors="coerce")
            df = df.dropna(subset=["calc_date"])
            for col in ["total_volume", "max_close_price", "min_close_price",
                        "max_intraday_drop", "max_intraday_volatility"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "max_close_price" in df.columns:
                df["avg_close_price"] = df["max_close_price"]
        return df

    # ── get_monthly_analysis_data ─────────────────────────────────────────────

    def get_monthly_analysis_data(self, symbols=None) -> pd.DataFrame:
        sql = "SELECT * FROM mysql_db.bigdata_stock.tbl_stock_monthly_analysis WHERE 1=1"
        if symbols:
            sql += f" AND symbol IN ({', '.join(repr(s) for s in symbols)})"
        df = self._run_drill_query(sql)
        if df.empty:
            where = ["1=1"]
            if symbols:
                where.append(f"symbol IN ({', '.join(repr(s) for s in symbols)})")
            df = self._run_mysql_query(
                f"SELECT * FROM bigdata_stock.tbl_stock_monthly_analysis WHERE {' AND '.join(where)}"
            )
        return df

    def get_summary(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["symbol", "price", "price_variance", "pct_change", "total_volume"])

    def execute(self, sql: str, params: dict | None = None) -> bool:
        try:
            with self._get_engine().begin() as conn:
                conn.execute(text(sql), params or {})
            return True
        except Exception as e:
            print(f"[execute] {e}")
            return False