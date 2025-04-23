import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import datetime
matplotlib.use('Agg')

name = 'APPLE'; ticker = 'AAPL'

now = datetime.datetime.now()
start = now - datetime.timedelta(days=365*10)
start = start.strftime('%Y-%m-%d'); end = now.strftime('%Y-%m-%d')

data = yf.download(ticker, start=start, end=end)

data['MA20'] = data['Close'].rolling(window=20).mean()
data['MA60'] = data['Close'].rolling(window=60).mean()
data['MA120'] = data['Close'].rolling(window=120).mean()

if isinstance(data.columns, pd.MultiIndex):
    data.columns = [col[0] for col in data.columns]
    data.columns.name = None

data = data.reset_index().set_index('Date')
data.to_csv(f'{name}_stock.csv', index=False)

golden_cross = (data['MA20'] > data['MA60']) & (data['MA20'].shift(1) <= data['MA60'].shift(1))
death_cross = (data['MA20'] < data['MA60']) & (data['MA20'].shift(1) >= data['MA60'].shift(1))

golden_cross = data[golden_cross]
death_cross = data[death_cross]

golden_cross_filtered = golden_cross[golden_cross['Close'] > golden_cross['MA120']]
death_cross_filtered = death_cross[death_cross['Close'] < death_cross['MA120']]

plt.figure(figsize=(14, 6))
plt.plot(data['Close'], label='Close Price', color='black', alpha=0.6)
plt.plot(data['MA20'], label='MA20', linestyle='--')
plt.plot(data['MA60'], label='MA60', linestyle='--')
plt.plot(data['MA120'], label='MA120', linestyle='--')

plt.scatter(golden_cross_filtered.index, golden_cross_filtered.Close, label='Golden Cross', color = 'green', marker='^', s=100)
plt.scatter(death_cross_filtered.index, death_cross_filtered.Close, label='Death Cross', color = 'red', marker='v', s=100)

plt.title(f'{name} Stock Price with Moving Averages')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(f'{name}_stock.png')
plt.close()