#!/usr/bin/env python3
import sys
import json

current_symbol = None
daily_records = {}

def process_and_print(symbol, records_dict):
    for t_date in sorted(records_dict.keys()):
        vol = records_dict[t_date]['volume']
        
        if vol >= 10000000:
            status = "High"
        elif vol >= 5000000:
            status = "Medium"
        else:
            status = "Low"

        output_dict = {
            "symbol": symbol,
            "calc_date": t_date,
            "liquidity_status": status
        }
        print(json.dumps(output_dict))

for line in sys.stdin:
    line = line.strip()
    try:
        symbol, values = line.split('\t')
        t_date, s_time, vol_str = values.split(',')
        volume = float(vol_str)
    except ValueError:
        continue

    if current_symbol == symbol:
        if t_date not in daily_records or s_time > daily_records[t_date]['scrape_time']:
            daily_records[t_date] = {'scrape_time': s_time, 'volume': volume}
    else:
        if current_symbol:
            process_and_print(current_symbol, daily_records)
        
        current_symbol = symbol
        daily_records = {t_date: {'scrape_time': s_time, 'volume': volume}}

if current_symbol:
    process_and_print(current_symbol, daily_records)
