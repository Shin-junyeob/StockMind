import os
import glob
from typing import List

import pandas as pd

from .settings import BASE_DIR, RESULTS_DIR, INPUT_FILENAME, OUTPUT_COLUMNS


def list_tickers() -> List[str]:
    if not os.path.isdir(BASE_DIR):
        return []
    return [d for d in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, d))]


def list_dates(ticker: str) -> List[str]:
    troot = os.path.join(BASE_DIR, ticker)
    if not os.path.isdir(troot):
        return []
    return sorted([d for d in os.listdir(troot) if os.path.isdir(os.path.join(troot, d))])


def input_path(ticker: str, date:str) -> str:
    return os.path.join(BASE_DIR, ticker, date, INPUT_FILENAME)


def output_path(ticker: str, date:str) -> str:
    return os.path.join(RESULTS_DIR, ticker, f"{date}.csv")


def read_news_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if "content" not in df.columns:
        df["content"] = ""
    return df


def ensure_dir_for_file(fpath: str) -> None:
    os.makedirs(os.path.dirname(fpath), exist_ok = True)


def write_results(path: str, df: pd.DataFrame) -> None:
    ensure_dir_for_file(path)
    out = df[OUTPUT_COLUMNS].copy()
    out.to_csv(path, index=False)

