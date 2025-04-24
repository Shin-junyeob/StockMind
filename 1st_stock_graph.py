import yfinance as yf
import datetime
import matplotlib.pyplot as plt

now = datetime.datetime.now()
start = now - datetime.timedelta(days=10*365)
start = start.strftime('%Y-%m-%d'); end = now.strftime('%Y-%m-%d')

name = 'APPLE'; ticker = 'AAPL'
data = yf.download(ticker, start = start, end = end)

data['MA20'] = data['Close'].rolling(window=20).mean()
data['MA60'] = data['Close'].rolling(window=60).mean()
data['MA120'] = data['Close'].rolling(window=120).mean()

golden_cross = (data['MA20'] > data['MA60']) & (data['MA20'].shift(1) <= data['MA60'].shift(1))
death_cross = (data['MA20'] < data['MA60']) & (data['MA20'].shift(1) >= data['MA60'].shift(1))

golden_cross_dates = data[golden_cross].index
death_cross_dates = data[death_cross].index

plt.figure(figsize=(14,6))
plt.plot(data['Close'], label='Close')
plt.plot(data['MA20'], label='MA20')
plt.plot(data['MA60'], label='MA60')
plt.plot(data['MA120'], label='MA120')
plt.scatter(golden_cross_dates, data.loc[golden_cross_dates]['Close'], label='Golden Cross', marker='^', color='green')
plt.scatter(death_cross_dates, data.loc[death_cross_dates]['Close'], label='Death Cross', marker='v', color='red')
plt.legend()
plt.title(f'{ticker} Moving Averages & Crosses')
plt.show()