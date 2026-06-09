#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    parts = line.split(',')

    if len(parts) >= 9 and parts[0] != 'symbol' and parts[0] != 'Symbol':
        try:
            symbol = parts[0]
            trading_date = parts[1]
            close_price = float(parts[4])
            
            print(f"{symbol}\t{trading_date},{close_price}")
        except ValueError:
            continue
