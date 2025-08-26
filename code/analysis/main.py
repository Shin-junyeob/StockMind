import argparse
import os

from .io_utils import list_tickers, list_dates, input_path
from .processor import process_one_day


def run(ticker: str | None, date: str | None) -> None:
    tickers = [ticker] if ticker else list_tickers()
    for t in tickers:
        dates = [date] if date else list_dates(t)
    for d in dates:
        ipath = input_path(t, d)
        try:
            out = process_one_day(t, d, ipath)
            print(f"📁 저장 완료: {out}")
        except FileNotFoundError:
            print(f"⚠️ 입력 없음: {ipath}")
        except Exception as e:
            print(f"❗ 오류: {t} {d} → {e}")

def main():
    p = argparse.ArgumentParser(description="StockMind Step 2: Analysis")
    p.add_argument("--ticker", type=str, default=None, help="특정 티커만 처리")
    p.add_argument("--date", type=str, default=None, help="특정 날짜만 처리 (YYYY-MM-DD)")
    args = p.parse_args()
    
    run(args.ticker, args.date)

if __name__ == "__main__":
    main()
