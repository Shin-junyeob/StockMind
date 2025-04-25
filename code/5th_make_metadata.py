import pandas as pd
import yfinance as yf
import datetime
import os

def create_metadata(ticker, output_file):
    today = datetime.datetime.now().date()
    date_str = today.strftime('%Y-%m-%d')
    feature_file = f"{ticker}_{date_str}.csv"

    # 주가 데이터 가져오기 (7일 중 가장 최근 2일)
    end_date = today + datetime.timedelta(days=1)
    start_date = today - datetime.timedelta(days=7)

    df = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

    if len(df) < 2:
        print("⚠️ 주가 데이터 부족")
        return

    df = df.sort_index()
    price_today = df.iloc[-1]['Close']
    price_yesterday = df.iloc[-2]['Close']
    price_today = float(round(price_today, 2))
    rate = float(round((price_today - price_yesterday) / price_yesterday * 100, 2))

    metadata_entry = {
        'date': date_str,
        'feature_file': feature_file,
        'price': price_today,
        'rate': rate
    }

    # metadata.csv 업데이트
    if os.path.exists(output_file):
        metadata_df = pd.read_csv(output_file)
        metadata_df = pd.concat([metadata_df, pd.DataFrame([metadata_entry])], ignore_index=True)
    else:
        metadata_df = pd.DataFrame([metadata_entry])

    metadata_df.to_csv(output_file, index=False)
    print(f"✅ metadata.csv 업데이트 완료: {date_str}")

if __name__ == "__main__":
    ticker = 'AAPL'
    metadata = '../metadata.csv'
    create_metadata(ticker, metadata)