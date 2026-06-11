#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')

    if len(parts) == 9 and parts[0] != 'Symbol' and parts[1] != '\\N':
        try:
            symbol = parts[0]
            date = parts[1]
            scrape_time = parts[2]
            high = float(parts[7])
            low = float(parts[8])

            print(f"{symbol}\t{date}\t{scrape_time},{high},{low}")
        except ValueError:
            continue
