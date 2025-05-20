import pandas as pd
import matplotlib.pyplot as plt
import os

# 데이터 불러오기
df_all = pd.read_csv("../results/stock_daily_2year.csv")
df_all['datetime'] = pd.to_datetime(df_all['datetime'])

df_flag = pd.read_csv("../results/flag_patterns_labeled.csv")
df_flag['pattern_date'] = pd.to_datetime(df_flag['pattern_date'])

# 저장 폴더 생성
output_dir = "../results/flag_pattern_charts"
os.makedirs(output_dir, exist_ok=True)

# 시각화 매개변수
pole_days = 5

# 전체 루프 실행
for i, sample in df_flag.iterrows():
    symbol = sample['symbol']
    pattern_date = sample['pattern_date']
    flag_days = int(sample['flag_days'])

    df_symbol = df_all[df_all['symbol'] == symbol].sort_values('datetime').reset_index(drop=True)

    try:
        idx = df_symbol[df_symbol['datetime'] == pattern_date].index[0]
    except IndexError:
        continue

    start_idx = max(0, idx - pole_days)
    end_idx = min(len(df_symbol) - 1, idx + flag_days + 1)
    df_plot = df_symbol.loc[start_idx:end_idx]

    # 그래프 그리기
    plt.figure(figsize=(10, 5))
    plt.plot(df_plot['datetime'], df_plot['close'], marker='o', 
label='Close Price')
    plt.axvspan(df_plot['datetime'].iloc[0], df_plot['datetime'].iloc[pole_days], color='orange', alpha=0.3, label='Pole')
    plt.axvspan(df_plot['datetime'].iloc[pole_days+1], df_plot['datetime'].iloc[-1], color='blue', alpha=0.2, label='Flag')
    plt.axvline(pattern_date, color='red', linestyle='--', label='Pattern Date')
    plt.title(f"Flag Pattern - {symbol} - {pattern_date.date()}")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # 이미지 저장
    filename = f"{symbol}_{pattern_date.date()}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath)
    plt.close()
