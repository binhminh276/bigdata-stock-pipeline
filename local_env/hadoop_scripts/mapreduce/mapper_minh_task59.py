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
            volume = int(parts[5])

            year, month, _ = date.split('-')

            print(f"{symbol},{year},{month}\t{date}\t{scrape_time}\t{volume}")
        except ValueError:
            continue
