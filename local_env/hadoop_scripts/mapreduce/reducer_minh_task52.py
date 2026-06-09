#!/usr/bin/env python3
import sys
import json

current_symbol = None
records = []

# Xử lý tính toán giá trị min/max luỹ kế và chỉ lấy giá đóng cửa của scrape_time mới nhất trong ngày
def process_symbol(symbol, records):
    unique_records = {}
    for date, scrape_time, close in records:
        if date not in unique_records or scrape_time > unique_records[date]['scrape_time']:
            unique_records[date] = {'scrape_time': scrape_time, 'close': close}

    sorted_dates = sorted(unique_records.keys())

    max_close = float('-inf')
    min_close = float('inf')

    for date in sorted_dates:
        close = unique_records[date]['close']

        if close > max_close:
            max_close = close
        if close < min_close:
            min_close = close

        output_dict = {
            "symbol": symbol,
            "calc_date": date,
            "max_close_price": round(max_close, 2),
            "min_close_price": round(min_close, 2)
        }
        print(json.dumps(output_dict))

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) != 4:
            continue

        sym = parts[0]
        date = parts[1]
        scrape_time = parts[2]
        close = float(parts[3])

        if current_symbol == sym:
            records.append((date, scrape_time, close))
        else:
            if current_symbol:
                process_symbol(current_symbol, records)
            current_symbol = sym
            records = [(date, scrape_time, close)]
    except Exception:
        continue

if current_symbol:
    process_symbol(current_symbol, records)
