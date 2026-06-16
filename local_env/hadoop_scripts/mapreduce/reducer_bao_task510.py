#!/usr/bin/env python3
import sys
import json

current_key = None
max_drop = 0.0

def emit_result(key_str, drop_val):
    sym, dt = key_str.split('\t')
    print(json.dumps({"symbol": sym, "calc_date": dt, "max_intraday_drop": round(drop_val, 2)}))

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        sym, dt, drop_str = line.split('\t')
        key = f"{sym}\t{dt}"
        drop = float(drop_str)
    except ValueError: continue

    if current_key == key:
        if drop > max_drop: max_drop = drop
    else:
        if current_key:
            emit_result(current_key, max_drop)
        current_key = key
        max_drop = drop

if current_key:
    emit_result(current_key, max_drop)