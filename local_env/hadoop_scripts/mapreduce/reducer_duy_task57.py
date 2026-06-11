#!/usr/bin/env python3
import sys
import json

current_date = None
stocks_in_day = {}

def export_market_data(date, stocks_dict):
    """Hàm phụ trợ tính tổng khối lượng của các mã hợp lệ và xuất JSON"""
    total_market_volume = 0
    for sym, (scrape_time, vol) in stocks_dict.items():
        total_market_volume += vol
        
    output_dict = {
        "symbol": "MARKET",
        "calc_date": date,
        "total_volume": total_market_volume
    }
    print(json.dumps(output_dict))

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) != 2:
            continue

        date = parts[0]
        sym, scrape_time, vol_str = parts[1].split(',')
        volume = int(vol_str)

        if current_date == date:
            if sym in stocks_in_day:
                if scrape_time > stocks_in_day[sym][0]:
                    stocks_in_day[sym] = (scrape_time, volume)
            else:
                stocks_in_day[sym] = (scrape_time, volume)
        else:
            if current_date:
                export_market_data(current_date, stocks_in_day)
            
            current_date = date
            stocks_in_day = {sym: (scrape_time, volume)}

    except Exception:
        continue

if current_date:
    export_market_data(current_date, stocks_in_day)
