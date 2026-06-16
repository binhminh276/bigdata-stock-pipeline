#!/usr/bin/env python3
import sys
import json

current_key = None
max_volume = 0

def emit_result(key_str, vol):
    sym, dt = key_str.split('\t')
    print(json.dumps({"symbol": sym, "calc_date": dt, "total_volume": vol}))

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        sym, dt, vol_str = line.split('\t')
        key = f"{sym}\t{dt}"
        volume = int(vol_str)
    except ValueError: continue

    if current_key == key:
        if volume > max_volume:
            max_volume = volume
    else:
        if current_key:
            emit_result(current_key, max_volume)
        current_key = key
        max_volume = volume

if current_key:
    emit_result(current_key, max_volume)