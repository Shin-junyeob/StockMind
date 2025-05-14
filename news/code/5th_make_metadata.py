import requests
import datetime
import json
import os
from privacy import twelve_data_api_key as api_key

def fetch_price_cache(ticker, cache_file="../metadata.json"):
    today = datetime.datetime.now().date()
    one_month_ago = today - datetime.timedelta(days=30)

    start_date = one_month_ago.strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    url = (
        f"https://api.twelvedata.com/time_series?apikey={api_key}"
        f"&symbol={ticker}&interval=1day&start_date={start_date}&end_date={end_date}"
        f"&timezone=UTC&dp=2&format=JSON"
    )

    response = requests.get(url)
    data = response.json()

    if "values" not in data:
        print("❗ 데이터 수신 실패:", data.get("message", "알 수 없음"))
        return None

    # 날짜순으로 정렬
    values = sorted(data["values"], key=lambda x: x["datetime"])
    price_dict = {}
    previous_price = None

    for entry in values:
        date = entry["datetime"]
        close_price = float(entry["close"])
        if previous_price is not None:
            rate = round((close_price - previous_price) / previous_price * 100, 2)
        else:
            rate = None
        price_dict[date] = {
            "price": close_price,
            "rate": rate,
            "news": f'{ticker}_{date}.csv'
        }
        previous_price = close_price

    with open(cache_file, "w") as f:
        json.dump(price_dict, f, indent=2)

    print(f"✅ {len(price_dict)}일치 종가 및 변동률 데이터를 {cache_file}에 저장 완료")
    return price_dict

if __name__ == "__main__":
    ticker = "AAPL"
    fetch_price_cache(ticker)