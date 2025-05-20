import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# Load data
df = pd.read_csv("../results/stock_daily_2year.csv")
df['datetime'] = pd.to_datetime(df['datetime'])
df = df.sort_values(by=['symbol', 'datetime']).reset_index(drop=True)

# Parameters
pole_days = 5
pole_gain = 0.12
flag_days = 7
flag_range_ratio = 0.08
future_window = 5
zigzag_min_count = 3
min_flag_std = 0.005
output_dir = "../results/flag_charts_strict"

os.makedirs(output_dir, exist_ok=True)

# Detection logic
results = []

for symbol in df['symbol'].unique():
    df_sym = df[df['symbol'] == symbol].reset_index(drop=True)

    for i in range(pole_days, len(df_sym) - flag_days - future_window):
        start_price = df_sym.loc[i - pole_days, 'close']
        end_price = df_sym.loc[i, 'close']
        pole_return = (end_price - start_price) / start_price

        if pole_return > pole_gain:
            pole_slice = df_sym.loc[i - pole_days:i]
            if (pole_slice['close'].pct_change() > 0.05).sum() == 0:
                continue

            flag_slice = df_sym.loc[i + 1:i + flag_days]
            flag_high = flag_slice['high'].max()
            flag_low = flag_slice['low'].min()
            range_ratio = (flag_high - flag_low) / end_price

            if range_ratio < flag_range_ratio:
                # Zigzag count
                closes = flag_slice['close'].values
                zigzag_count = 0
                for j in range(1, len(closes) - 1):
                    if (closes[j - 1] < closes[j] > closes[j + 1]) or (closes[j - 1] > closes[j] < closes[j + 1]):
                        zigzag_count += 1

                std_dev = np.std(flag_slice['close'].pct_change().dropna())

                if zigzag_count >= zigzag_min_count and std_dev >= min_flag_std:
                    pattern_date = df_sym.loc[i + flag_days, 'datetime']
                    now_price = df_sym.loc[i + flag_days, 'close']
                    future_price = df_sym.loc[i + flag_days + future_window, 'close']
                    future_return = (future_price - now_price) / now_price
                    label = int((future_return > 0) == (pole_return > 0))

                    results.append({
                        'symbol': symbol,
                        'pattern_date': pattern_date,
                        'pattern': 'flag',
                        'pole_return': pole_return,
                        'flag_days': flag_days,
                        'future_return': future_return,
                        'label': label
                    })

                    # Visualization
                    start_idx = i - pole_days
                    end_idx = i + flag_days + future_window
                    df_plot = df_sym.loc[start_idx:end_idx].reset_index(drop=True)

                    plt.figure(figsize=(10, 5))
                    plt.plot(df_plot['datetime'], df_plot['close'], marker='o', label='Close Price')

                    pole_end = pole_days
                    flag_end = pole_days + flag_days

                    plt.axvspan(df_plot['datetime'][0], df_plot['datetime'][pole_end], color='orange', alpha=0.3, label='Pole')
                    plt.axvspan(df_plot['datetime'][pole_end + 1], df_plot['datetime'][flag_end], color='blue', alpha=0.2, label='Flag')

                    if label == 1:
                        plt.axvspan(df_plot['datetime'][flag_end + 1], df_plot['datetime'].iloc[-1], color='green', alpha=0.2, label='Future (label=1)')
                    else:
                        plt.axvspan(df_plot['datetime'][flag_end + 1], df_plot['datetime'].iloc[-1], color='red', alpha=0.2, label='Future (label=0)')

                    plt.axvline(df_plot['datetime'][flag_end], color='red', linestyle='--', label='Pattern Date')
                    plt.title(f"Flag Pattern - {symbol} - {pattern_date.date()}")
                    plt.xlabel("Date")
                    plt.ylabel("Close Price")
                    plt.legend()
                    plt.grid(True)
                    plt.tight_layout()
                    filename = f"{symbol}_{pattern_date.date()}.png"
                    plt.savefig(os.path.join(output_dir, filename))
                    plt.close()

# Save results
df_results = pd.DataFrame(results)
df_results.to_csv("../results/flag_patterns_strict_labeled.csv", index=False)