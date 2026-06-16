#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    parts = line.split(',')

    if len(parts) < 9 or parts[0].lower() == 'symbol': continue
    try:
        symbol = parts[0]
        calc_date = parts[1]
        
        if parts[6] == '\\N' or parts[8] == '\\N': continue

        open_price = float(parts[6])
        low_price = float(parts[8])

        drop = open_price - low_price
        if drop < 0: drop = 0.0

        print(f"{symbol}\t{calc_date}\t{drop}")
    except ValueError:
        continue