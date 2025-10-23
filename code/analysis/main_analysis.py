import argparse
import os

from .io_utils import list_tickers, list_dates, input_path, pending_dates
from .processor import process_one_day


def run(ticker: str | None, date: str | None, mode: str) -> None:
    tickers = [ticker] if ticker else list_tickers()
    print(f"tickers : {tickers}")
    for t in tickers:
        if date:
            dates = [date]
        else:
            dates = pending_dates(t) if mode == "pending" else list_dates(t)

        if not dates:
            print(f"⏭️ {t}: 처리할 날짜가 없습니다(mode={mode}).")
            continue

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
    p.add_argument("--mode", choices=["all", "pending"], default="pending", help="처리 범위: 전체(all) 또는 미처리/갱신 필요만(pending)")
    args = p.parse_args()
    
    # 디버그 로그
    print(f"args: ticker={args.ticker}. date={args.date}, mode={args.mode}")

    run(args.ticker, args.date, args.mode)

if __name__ == "__main__":
    main()
