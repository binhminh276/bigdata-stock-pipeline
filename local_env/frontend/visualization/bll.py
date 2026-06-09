import matplotlib.pyplot as plt
import seaborn as sns
import mplfinance as mpf

chart_path = 'local_env\\frontend\\visualization\\charts'

def plot_close_price_line(df, symbol):
    data = df[df['symbol'] == symbol].sort_values('trading_date')
    plt.figure(figsize=(12, 6))
    plt.plot(data['trading_date'], data['close'])
    plt.title(f'Close Price Trend - {symbol}')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.savefig(f'{chart_path}/line_chart_{symbol}.png')
    plt.close()

def plot_volume_bar(df, symbol):
    data = df[df['symbol'] == symbol].sort_values('trading_date')
    plt.figure(figsize=(12, 6))
    plt.bar(data['trading_date'], data['volume'])
    plt.title(f'Trading Volume - {symbol}')
    plt.xlabel('Date')
    plt.ylabel('Volume')
    plt.savefig(f'{chart_path}/bar_chart_{symbol}.png')
    plt.close()

def plot_candlestick(df, symbol):
    data = df[df['symbol'] == symbol].copy()
    data.set_index('trading_date', inplace=True)
    data.sort_index(inplace=True)
    mpf.plot(data, type='candle', style='charles', title=f'Candlestick - {symbol}', savefig=f'{chart_path}/candlestick_{symbol}.png')

def plot_price_boxplot(df):
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='symbol', y='close', data=df)
    plt.title('Close Price Distribution by Symbol')
    plt.xlabel('Symbol')
    plt.ylabel('Close Price')
    plt.savefig(f'{chart_path}/boxplot_symbols.png')
    plt.close()

def plot_volume_price_scatter(df, symbol):
    data = df[df['symbol'] == symbol]
    plt.figure(figsize=(10, 6))
    plt.scatter(data['volume'], data['close'], alpha=0.5)
    plt.title(f'Volume vs Close Price - {symbol}')
    plt.xlabel('Volume')
    plt.ylabel('Close Price')
    plt.savefig(f'{chart_path}/scatter_{symbol}.png')
    plt.close()

def plot_close_price_histogram(df, symbol):
    data = df[df['symbol'] == symbol]
    plt.figure(figsize=(10, 6))
    plt.hist(data['close'].dropna(), bins=30, edgecolor='black')
    plt.title(f'Close Price Histogram - {symbol}')
    plt.xlabel('Close Price')
    plt.ylabel('Frequency')
    plt.savefig(f'{chart_path}/histogram_{symbol}.png')
    plt.close()

def plot_moving_average(df, symbol, window=30):
    data = df[df['symbol'] == symbol].sort_values('trading_date').copy()
    data['MA'] = data['close'].rolling(window=window).mean()
    plt.figure(figsize=(12, 6))
    plt.plot(data['trading_date'], data['close'], label='Close Price')
    plt.plot(data['trading_date'], data['MA'], label=f'{window}-Day MA', color='red')
    plt.title(f'Moving Average ({window} Days) - {symbol}')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.savefig(f'{chart_path}/ma_{symbol}.png')
    plt.close()