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


def read_news_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    # content 필드 유효성 보정
    if "content" not in df.columns:
        # title + body 조합 등으로 대체하고싶다면 여기에서 구현
        # 일단은 content 없으면 빈 문자열 처리
        df["content"] = ""
    # date 컬럼은 상위 폴더명 기준으로 processor에서 지정
    return df


def ensure_dir_for_file(fpath: str) -> None:
    os.makedirs(os.path.dirname(fpath), exist_ok = True)


def write_results(path: str, df: pd.DataFrame) -> None:
    ensure_dir_for_file(path)
    out = df[OUTPUT_COLUMNS].copy()
    out.to_csv(path, index=False)

