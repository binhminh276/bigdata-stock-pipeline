#!/usr/bin/env python3
import sys
import json

current_key = None
daily_volume = {}

# Hàm tổng hợp khối lượng theo tháng sau khi đã lọc bản ghi mới nhất của từng ngày
def process_group(key_str, daily_dict):
    sym, year, month = key_str.split(',')
    total_vol = sum(val[1] for val in daily_dict.values())
    
    output_dict = {
        "symbol": sym,
        "calc_year": int(year),
        "calc_month": int(month),
        "monthly_total_volume": total_vol
    }
    print(json.dumps(output_dict))

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) != 4:
            continue
            
        key_str = parts[0]
        date = parts[1]
        scrape_time = parts[2]
        vol = int(parts[3])
        
        if current_key == key_str:
            if date not in daily_volume or scrape_time > daily_volume[date][0]:
                daily_volume[date] = (scrape_time, vol)
        else:
            if current_key:
                process_group(current_key, daily_volume)
            current_key = key_str
            daily_volume = {date: (scrape_time, vol)}
            
    except ValueError:
        continue

if current_key:
    process_group(current_key, daily_volume)
