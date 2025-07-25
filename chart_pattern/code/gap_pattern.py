import numpy as np
import pandas as pd
import mplfinance as mpf
import os
import random
from tqdm import tqdm
from datetime import datetime, timedelta

def create_gap_chart(num, bullish=True):
    base_date = datetime.today()
    pre_days = random.randint(5, 8)
    post_days = random.randint(5, 8)
    total_days = pre_days + 1 + post_days
    dates = [base_date - timedelta(days=i) for i in reversed(range(total_days))]

    start_price = 100
    open_prices, high_prices, low_prices, close_prices = [], [], [], []
    current_price = start_price

    direction = 1 if bullish else -1

    # Pre-gap 구간
    for _ in range(pre_days):
        body = np.random.uniform(-0.5, 0.5) + direction * np.random.uniform(0, 0.3)
        wick = np.random.uniform(0.3, 0.8)
        open_p = current_price
        close_p = open_p + body
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick
        open_prices.append(open_p)
        close_prices.append(close_p)
        high_prices.append(high_p)
        low_prices.append(low_p)
        current_price = close_p

    # Gap 발생일
    gap_size = np.random.uniform(3.0, 5.0) * direction
    open_p = current_price + gap_size
    body = np.random.uniform(-0.5, 0.5)
    close_p = open_p + body
    wick = np.random.uniform(0.3, 0.8)
    open_prices.append(open_p)
    close_prices.append(close_p)
    high_prices.append(max(open_p, close_p) + wick)
    low_prices.append(min(open_p, close_p) - wick)
    current_price = close_p

    # Post-gap 구간
    for _ in range(post_days):
        body = np.random.uniform(-0.5, 0.5) + direction * np.random.uniform(0, 0.3)
        wick = np.random.uniform(0.3, 0.8)
        open_p = current_price
        close_p = open_p + body
        high_p = max(open_p, close_p) + wick
        low_p = min(open_p, close_p) - wick
        open_prices.append(open_p)
        close_prices.append(close_p)
        high_prices.append(high_p)
        low_prices.append(low_p)
        current_price = close_p

    df = pd.DataFrame({
        "Open": open_prices,
        "High": high_prices,
        "Low": low_prices,
        "Close": close_prices
    }, index=pd.DatetimeIndex(dates))

    return save_gap_file(num, df, bullish)

def save_gap_file(num, df, bullish):
    base_dir = '../results/short_pattern/gap_pattern'
    label = 'bullish' if bullish else 'bearish'
    output_dir = os.path.join(base_dir, label)
    os.makedirs(output_dir, exist_ok=True)

    file_name = f'{label}_gap_{(num+1):04d}.png'
    file_path = os.path.join(output_dir, file_name)

    mpf.plot(
        df,
        type='candle',
        style='charles',
        title=f'{label.capitalize()} Gap Pattern',
        savefig=file_path
    )


def start():
    for num in tqdm(range(1000), desc='Generating Gap Charts'):
        create_gap_chart(num, bullish=True)
        create_gap_chart(num, bullish=False)
    return ('Success!')
