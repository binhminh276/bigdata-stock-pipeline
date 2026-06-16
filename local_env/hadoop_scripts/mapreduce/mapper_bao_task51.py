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

        if not parts[5] or parts[5] == '\\N': continue

        volume = int(float(parts[5]))
        
        print(f"{symbol}\t{calc_date}\t{volume}")
    except ValueError:
        continue