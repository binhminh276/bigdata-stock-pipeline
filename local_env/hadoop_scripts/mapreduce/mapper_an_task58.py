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
        volume = float(parts[5])
        print(f"{symbol}\t{calc_date},{scrape_time},{volume}")
    except ValueError:
        continue
