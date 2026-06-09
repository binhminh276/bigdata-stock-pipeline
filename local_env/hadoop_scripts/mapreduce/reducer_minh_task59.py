#!/usr/bin/env python3
import sys
import json

current_key = None
total_vol = 0

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) != 4:
            continue
            
        sym = parts[0]
        year = int(parts[1])
        month = int(parts[2])
        vol = int(parts[3])
        
        key = (sym, year, month)
        
        if current_key == key:
            total_vol += vol
        else:
            if current_key:
                output_dict = {
                    "symbol": current_key[0],
                    "calc_year": current_key[1],
                    "calc_month": current_key[2],
                    "monthly_total_volume": total_vol
                }
                print(json.dumps(output_dict))
            
            current_key = key
            total_vol = vol
            
    except Exception:
        continue

if current_key:
    output_dict = {
        "symbol": current_key[0],
        "calc_year": current_key[1],
        "calc_month": current_key[2],
        "monthly_total_volume": total_vol
    }
    print(json.dumps(output_dict))
