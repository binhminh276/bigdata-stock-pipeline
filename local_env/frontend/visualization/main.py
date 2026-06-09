from dal import fetch_data_from_drill
import bll

def main():
    vm_ip = "100.80.217.65"
    query = """
    SELECT 
        columns[0] AS symbol,
        columns[1] AS trading_date,
        columns[2] AS scrape_time,
        columns[3] AS source,
        columns[4] AS close,
        columns[5] AS volume,
        columns[6] AS open,
        columns[7] AS high,
        columns[8] AS low
    FROM table(dfs.`/user/hadoop/stock_cleaned_csv/000000_0` (type => 'text', fieldDelimiter => ',', extractHeader => false)) 
    LIMIT 20000
    """
    
    df = fetch_data_from_drill(vm_ip, query)
    
    if not df.empty:
        if 'symbol' not in df.columns:
            print("Loi: Khong tim thay cot 'symbol'. Danh sach cot hien tai:")
            print(df.columns.tolist())
            return
            
        target_symbol = 'ACB'
        
        bll.plot_close_price_line(df, target_symbol)
        bll.plot_volume_bar(df, target_symbol)
        bll.plot_candlestick(df, target_symbol)
        bll.plot_price_boxplot(df)
        bll.plot_volume_price_scatter(df, target_symbol)
        bll.plot_close_price_histogram(df, target_symbol)
        bll.plot_moving_average(df, target_symbol)
        
        print("Da ve va luu 7 bieu do thanh cong.")
    else:
        print("Khong lay duoc du lieu hoac du lieu trong.")

if __name__ == "__main__":
    main()