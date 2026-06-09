#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')

    if len(parts) >= 9 and parts[0].lower() != 'symbol':
        try:
            symbol = parts[0]
            date = parts[1]
            scrape_time = parts[2]
            close = float(parts[4])

            print(f"{symbol}\t{date}\t{scrape_time}\t{close}")
        except ValueError:
            continue
