import requests
import pandas as pd

def fetch_data_from_drill(vm_ip, query):
    url = f"http://{vm_ip}:8047/query.json"
    payload = {"queryType": "SQL", "query": query}
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            rows = response.json().get("rows", [])
            df = pd.DataFrame(rows)
            if not df.empty:
                if 'trading_date' in df.columns:
                    df['trading_date'] = pd.to_datetime(df['trading_date'], errors='coerce')
                numeric_cols = ['close', 'volume', 'open', 'high', 'low']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        else:
            print("Loi Drill:", response.text)
            return pd.DataFrame()
    except Exception as e:
        print("Loi ket noi:", str(e))
        return pd.DataFrame()