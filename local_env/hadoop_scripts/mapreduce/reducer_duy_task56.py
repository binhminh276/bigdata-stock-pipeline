#!/usr/bin/env python3
import sys
import json

current_key = None
latest_scrape_time = ""
max_high = 0.0
min_low = 0.0

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) != 3:
            continue

        sym = parts[0]
        dt = parts[1]
        key = (sym, dt)
        
        scrape_time, high_str, low_str = parts[2].split(',')
        high = float(high_str)
        low = float(low_str)

        if current_key == key:
            # Lấy mốc thời gian muộn nhất trong ngày (Ưu tiên chốt phiên chiều 15:00 hơn phiên sáng 11:30)
            if scrape_time > latest_scrape_time:
                latest_scrape_time = scrape_time
                max_high = high
                min_low = low
        else:
            if current_key:
                variance = round(max_high - min_low, 2)
                print(json.dumps({
                    "symbol": current_key[0],
                    "calc_date": current_key[1],
                    "max_intraday_volatility": variance
                }))
            
            current_key = key
            latest_scrape_time = scrape_time
            max_high = high
            min_low = low

    except Exception:
        continue

if current_key:
    variance = round(max_high - min_low, 2)
    print(json.dumps({
        "symbol": current_key[0],
        "calc_date": current_key[1],
        "max_intraday_volatility": variance
    }))
