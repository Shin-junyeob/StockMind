from twelvedata import TDClient
import pandas as pd
import time
import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 
'../../')))
from privacy import twelve_data_api_key as api_key

td = TDClient(apikey=api_key)

symbols = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]

now = datetime.datetime.now()
start_date = now - datetime.timedelta(days=2*365); 
start_date.strftime('%Y-%m-%d')
end_date = now.strftime('%Y-%m-%d')

all_data = []

for symbol in symbols:
    try:
        ts = td.time_series(
            symbol=symbol,
            interval="1day",
            start_date=start_date,
            end_date=end_date,
            outputsize=500
        ).as_pandas()

        ts = ts.reset_index()
        ts['symbol'] = symbol
        all_data.append(ts)

        print(f"{symbol} 수집 완료 ({len(ts)} rows)")
        time.sleep(1)  # API 호출 간격 제한

    except Exception as e:
        print(f"❌ {symbol} 수집 실패: {e}")
        time.sleep(1)

os.makedirs("../results", exist_ok=True)

df_all = pd.concat(all_data, ignore_index=True)
df_all.to_csv("../results/stock_daily_2year.csv", index=False)

print(f"\n✅ 전체 데이터 수집 완료: {len(df_all)} rows 저장됨.")
