import os
import glob
from typing import List

import pandas as pd

from .settings import BASE_DIR, DATA_DIR, RAW_DIR, RESULTS_DIR, INPUT_FILENAME, OUTPUT_COLUMNS


def list_tickers() -> List[str]:
    # data/raw 하위 폴더명을 티커로 간주
    if not os.path.isdir(RAW_DIR):
        return []
    return [d for d in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, d))]


def list_dates(ticker: str) -> List[str]:
    troot = os.path.join(RAW_DIR, ticker)
    if not os.path.isdir(troot):
        return []
    # YYYY-MM-DD 형태의 디렉토리만
    return sorted([d for d in os.listdir(troot) if os.path.isdir(os.path.join(troot, d))])


def input_path(ticker: str, date:str) -> str:
    return os.path.join(RAW_DIR, ticker, date, INPUT_FILENAME)


def output_path(ticker: str, date:str) -> str:
    return os.path.join(RESULTS_DIR, ticker, f"{date}.csv")

def is_pending(ticker: str, date: str) -> bool:
    """입력 존재 & (출력 없음 or 입력이 더 최신 or 출력이 비어있음) → 처리 필요"""
    ip = input_path(ticker, date)
    op = output_path(ticker, date)

    # 입력 없으면 처리 불가 → pending 아님
    if not os.path.exists(ip):
        return False

    # 출력이 없으면 처리 필요
    if not os.path.exists(op):
        return True

    try:
        # 입력이 더 최근이면 재처리
        if os.path.getmtime(ip) > os.path.getmtime(op):
            return True

        # 출력이 비어 있으면 재처리
        try:
            out_head = pd.read_csv(op, nrows=1)
            if out_head.empty:
                return True
        except Exception:
            # 손상/파싱 오류 시 재처리
            return True
    except Exception:
        # mtime 접근 실패 등 예외 시 재처리
        return True

    return False


def pending_dates(ticker: str) -> list[str]:
    """해당 티커에서 처리 필요 날짜만 반환"""
    return [d for d in list_dates(ticker) if is_pending(ticker, d)]


def read_news_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    # content 필드 유효성 보정
    if "content" not in df.columns:
        df["content"] = ""
    else:
        df["content"] = df["content"].map(lambda x: x.strip() if isinstance(x, str) else "")
    return df


def ensure_dir_for_file(fpath: str) -> None:
    os.makedirs(os.path.dirname(fpath), exist_ok = True)


def write_results(path: str, df: pd.DataFrame) -> None:
    ensure_dir_for_file(path)
    out = df[OUTPUT_COLUMNS].copy()
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = out[c].map(lambda x: x.strip() if isinstance(x, str) else "")
    out.to_csv(path, index=False)

