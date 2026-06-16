#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split(',')
    if len(parts) < 9 or parts[0].lower() == 'symbol':
        continue

    try:
        symbol = parts[0].strip()
        raw_date = parts[1].strip()
        scrape_time = parts[2].strip()
        close_price = float(parts[4].strip())

        parsed_date = None
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%H"):
            try:
                parsed_date = datetime.strptime(raw_date, fmt)
                break
            except ValueError:
                continue

        if not parsed_date:
            continue

        if parsed_date.weekday() in [5, 6]:
            continue

        calc_date_str = parsed_date.strftime("%Y-%m-%d")
        print(f"{symbol}\t{calc_date_str}\t{scrape_time}\t{close_price}")
    except (ValueError, IndexError):
        continue 
