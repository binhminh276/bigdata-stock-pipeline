#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')

    if len(parts) < 9 or parts[0].lower() == 'symbol':
        continue

    symbol = parts[0]
    calc_date = parts[1]
    scrape_time = parts[2]

    try:
        open_price = float(parts[4])
        close_price = float(parts[6])

        up_day = 1 if close_price > open_price else 0
        down_day = 1 if close_price < open_price else 0

        print(f"{symbol}\t{calc_date},{scrape_time},{up_day},{down_day}")
    except ValueError:
        continue
