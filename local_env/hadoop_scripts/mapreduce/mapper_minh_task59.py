#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')
    
    if len(parts) == 9 and parts[0].lower() != 'symbol':
        try:
            symbol = parts[0]
            date = parts[1]
            volume = int(parts[5])
            
            year, month, _ = date.split('-')
            
            print(f"{symbol}\t{year}\t{month}\t{volume}")
        except ValueError:
            continue
