#!/usr/bin/env python3
import sys
import json

current_symbol = None
max_close = float('-inf')
min_close = float('inf')
last_date = None

for line in sys.stdin:
    line = line.strip()
    try:
        symbol, values = line.split('\t')
        t_date, close_str = values.split(',')
        close_price = float(close_str)
    except ValueError:
        continue

    if current_symbol == symbol:
        if close_price > max_close:
            max_close = close_price
        if close_price < min_close:
            min_close = close_price
        last_date = t_date
    else:
        if current_symbol:
            output_dict = {
                "symbol": current_symbol,
                "calc_date": last_date,
                "max_close_price": max_close,
                "min_close_price": min_close
            }
            print(json.dumps(output_dict))

        current_symbol = symbol
        max_close = close_price
        min_close = close_price
        last_date = t_date

if current_symbol:
    output_dict = {
        "symbol": current_symbol,
        "calc_date": last_date,
        "max_close_price": max_close,
        "min_close_price": min_close
    }
    print(json.dumps(output_dict))
