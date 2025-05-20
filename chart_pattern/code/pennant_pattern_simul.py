import numpy as np
import pandas as pd
import mplfinance as mpf
import os
import random
from datetime import datetime, timedelta

output_dir = "../results/pennant_pattern"
os.makedirs(output_dir, exist_ok=True)

def generate_structured_flag_candlestick(num, bullish=True, seed=None):
    if bullish:
        output_dir = '../results/pennant_pattern/bullish'
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = '../results/pennant_pattern/bearish'
        os.makedirs(output_dir, exist_ok=True)
    if seed is not None:
        np.random.seed(seed)
        random.seed(seed)

    total_days = 7 + 5 + 3  # pole + flag + breakout
    base_date = datetime.today()
    dates = [base_date - timedelta(days=i) for i in reversed(range(total_days))]

    start_price = 100
    open_prices, high_prices, low_prices, close_prices = [], [], [], []
    current_price = start_price

    # Pole 구간 (급격한 상승/하락)
    for _ in range(7):
        body = np.random.uniform(1.5, 3.0)
        wick = np.random.uniform(0.5, 1.5)
        direction = 1 if bullish else -1
        open_p = current_price
        close_p = open_p + direction * body
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick
        open_prices.append(open_p)
        high_prices.append(high_p)
        low_prices.append(low_p)
        close_prices.append(close_p)
        current_price = close_p

    # Pennant 구간 (삼각 수렴)
    for i in range(5):
        range_decay = 1.0 - (i / 5)  # 점점 좁아지는 범위
        body = np.random.uniform(-1.0, 1.0) * range_decay * (-1 if bullish else 1)
        wick = np.random.uniform(0.3, 0.8) * range_decay

        open_p = current_price
        close_p = open_p + body
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick

        open_prices.append(open_p)
        high_prices.append(high_p)
        low_prices.append(low_p)
        close_prices.append(close_p)
        current_price = close_p


    # Breakout 구간 (추세 재개)
    for _ in range(3):
        body = np.random.uniform(2.0, 3.5)
        wick = np.random.uniform(0.5, 1.0)
        direction = 1 if bullish else -1
        open_p = current_price
        close_p = open_p + direction * body
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick
        open_prices.append(open_p)
        high_prices.append(high_p)
        low_prices.append(low_p)
        close_prices.append(close_p)
        current_price = close_p

    ohlc = pd.DataFrame({
        "Open": open_prices,
        "High": high_prices,
        "Low": low_prices,
        "Close": close_prices
    }, index=pd.DatetimeIndex(dates))

    label = "bullish" if bullish else "bearish"
    filename = f"{label}_pennant_{num+1}.png"
    filepath = os.path.join(output_dir, filename)

    mpf.plot(
        ohlc,
        type='candle',
        style='charles',
        title=f"{label.capitalize()} Flag",
        savefig=filepath
    )

# 실행
for num in range(1000):
    generate_structured_flag_candlestick(num, bullish=True)
    generate_structured_flag_candlestick(num, bullish=False)