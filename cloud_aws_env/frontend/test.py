"""
test_connection.py — SSH Tunnel qua subprocess (native ssh command)
Không dùng sshtunnel/paramiko — tránh lỗi DSSKey
Chạy: python test_connection.py
"""

import os
import sys
import time
import subprocess
import socket
from dotenv import load_dotenv

load_dotenv()

print("=" * 50)
print("  BigData Stock — Connection Test")
print("=" * 50)

# ── Config ────────────────────────────────────────────────
SSH_HOST   = os.getenv("SSH_HOST",  "ec2-32-195-43-148.compute-1.amazonaws.com")
SSH_USER   = os.getenv("SSH_USER",  "ubuntu")
PEM_FILE   = os.getenv("SSH_KEY_PATH", "bigdata_key.pem")
MYSQL_HOST = os.getenv("MYSQL_HOST", "10.0.1.161")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
LOCAL_PORT = int(os.getenv("TUNNEL_LOCAL_PORT", "3307"))
MYSQL_USER = os.getenv("MYSQL_USER", "admin")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB   = os.getenv("MYSQL_DB", "bigdata_stock")

# ── 1. Kiểm tra file .pem ─────────────────────────────────
print("\n[1/4] Kiểm tra file .pem...")
pem_found = None
for p in [PEM_FILE, f"frontend/{PEM_FILE}", os.path.expanduser(f"~/.ssh/{PEM_FILE}")]:
    if os.path.exists(p):
        # os.chmod(p, 0o600)  # không dùng trên Windows
        pem_found = p
        break

if not pem_found:
    print(f"  ❌ Không tìm thấy '{PEM_FILE}'")
    sys.exit(1)
print(f"  ✅ {pem_found} (chmod 600)")

# ── 2. Kiểm tra packages ──────────────────────────────────
print("\n[2/4] Kiểm tra packages...")
for pkg in ["dotenv", "sqlalchemy", "mysql.connector"]:
    try:
        __import__(pkg.split(".")[0])
        print(f"  ✅ {pkg}")
    except ImportError:
        print(f"  ❌ {pkg} chưa cài — chạy: pip install python-dotenv sqlalchemy mysql-connector-python")
        sys.exit(1)

# ── 3. Mở SSH Tunnel bằng subprocess ─────────────────────
print(f"\n[3/4] Mở SSH Tunnel 127.0.0.1:{LOCAL_PORT} → {MYSQL_HOST}:{MYSQL_PORT}...")

ssh_cmd = [
    "ssh",
    "-i", pem_found,
    "-N",                          # không chạy lệnh, chỉ tunnel
    "-L", f"{LOCAL_PORT}:{MYSQL_HOST}:{MYSQL_PORT}",
    "-o", "StrictHostKeyChecking=no",
    "-o", "ExitOnForwardFailure=yes",
    "-o", "ServerAliveInterval=30",
    "-o", "ConnectTimeout=10",
    f"{SSH_USER}@{SSH_HOST}",
]

tunnel_proc = subprocess.Popen(
    ssh_cmd,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.PIPE,
)

# Chờ tunnel mở (tối đa 10 giây)
for i in range(10):
    time.sleep(1)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(("127.0.0.1", LOCAL_PORT))
    sock.close()
    if result == 0:
        print(f"  ✅ SSH Tunnel active — 127.0.0.1:{LOCAL_PORT} ready (sau {i+1}s)")
        break
else:
    err = tunnel_proc.stderr.read().decode()
    print(f"  ❌ Tunnel không mở được sau 10s")
    print(f"     SSH stderr: {err.strip()}")
    tunnel_proc.kill()
    sys.exit(1)

# ── 4. Kết nối MySQL ──────────────────────────────────────
print(f"\n[4/4] Kết nối MySQL — 127.0.0.1:{LOCAL_PORT}/{MYSQL_DB}...")
import mysql.connector

try:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=LOCAL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        connection_timeout=10,
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"  ✅ MySQL connected!")
    print(f"  📋 Tables: {', '.join(tables) if tables else '(trống)'}")
    conn.close()
except Exception as e:
    print(f"  ❌ MySQL lỗi: {e}")
    tunnel_proc.kill()
    sys.exit(1)

tunnel_proc.kill()

print("\n" + "=" * 50)
print("  ✅ Tất cả OK! Chạy app:")
print("  streamlit run app.py")
print("=" * 50)