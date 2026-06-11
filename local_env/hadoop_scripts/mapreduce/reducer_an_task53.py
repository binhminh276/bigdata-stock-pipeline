#!/usr/bin/env python3
import sys
import json

current_symbol = None
daily_records = {}

def process_and_print(symbol, records_dict):
    streak_up = 0
    streak_down = 0
    
    for t_date in sorted(records_dict.keys()):
        u_day = records_dict[t_date]['up_day']
        d_day = records_dict[t_date]['down_day']

        if u_day == 1:
            streak_up += 1
            streak_down = 0
        elif d_day == 1:
            streak_down += 1
            streak_up = 0

        output_dict = {
            "symbol": symbol,
            "calc_date": t_date,
            "up_days_count": streak_up,
            "down_days_count": streak_down
        }
        print(json.dumps(output_dict))

for line in sys.stdin:
    line = line.strip()
    try:
        symbol, values = line.split('\t')
        t_date, s_time, up_str, down_str = values.split(',')
        u_day = int(up_str)
        d_day = int(down_str)
    except ValueError:
        continue

    if current_symbol == symbol:
        # Lọc chốt phiên
        if t_date not in daily_records or s_time > daily_records[t_date]['scrape_time']:
            daily_records[t_date] = {'scrape_time': s_time, 'up_day': u_day, 'down_day': d_day}
    else:
        if current_symbol:
            process_and_print(current_symbol, daily_records)
            
        current_symbol = symbol
        daily_records = {t_date: {'scrape_time': s_time, 'up_day': u_day, 'down_day': d_day}}

if current_symbol:
    process_and_print(current_symbol, daily_records)
