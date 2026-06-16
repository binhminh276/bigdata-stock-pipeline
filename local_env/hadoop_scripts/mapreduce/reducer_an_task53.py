#!/usr/bin/env python3
import sys
import json
from datetime import datetime

current_symbol = None
records = []

def process_symbol_streak(symbol, records):
    unique_records = {}
    for calc_date_str, scrape_time, close in records:
        if calc_date_str not in unique_records or scrape_time > unique_records[calc_date_str]['scrape_time']:
            unique_records[calc_date_str] = {'close': close}

    sorted_dates = sorted(unique_records.keys(), key=lambda d: datetime.strptime(d, "%Y-%m-%d").date())

    streak_up = 0
    streak_down = 0
    prev_close = None

    for date_str in sorted_dates:
        close = unique_records[date_str]['close']

        if prev_close is None:
            streak_up = 0
            streak_down = 0
        else:
            if close > prev_close:
                streak_up += 1
                streak_down = 0
            elif close < prev_close:
                streak_down += 1
                streak_up = 0
            else:
                streak_up = 0
                streak_down = 0

        output_dict = {
            "symbol": symbol,
            "calc_date": date_str,
            "up_days_count": streak_up,
            "down_days_count": streak_down
        }
        print(json.dumps(output_dict))
        
        prev_close = close

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        parts = line.split('\t')
        if len(parts) != 4:
            continue

        sym = parts[0]
        date_str = parts[1]
        scrape_time = parts[2]
        close = float(parts[3])

        if current_symbol == sym:
            records.append((date_str, scrape_time, close))
        else:
            if current_symbol:
                process_symbol_streak(current_symbol, records)
            current_symbol = sym
            records = [(date_str, scrape_time, close)]
    except Exception:
        continue

if current_symbol:
    process_symbol_streak(current_symbol, records) 
