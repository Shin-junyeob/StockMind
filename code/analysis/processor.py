import os

import pandas as pd

from .summarizer import summarize_long
from .sentiment import infer_sentiment
from .keywords import extract_keywords
from .io_utils import read_news_csv, output_path, write_results
from .settings import OUTPUT_COLUMNS, DEDUP_SUBSET




def process_one_day(ticker: str, date: str, input_csv_path: str) -> str:
    """
    단일 티커/날짜 입력 CSV를 읽어 summary/sentiment/keywords로 변환 후 저장.
    반환값: 저장된 결과 파일 경로
    """
    df = read_news_csv(input_csv_path)


    # 이미 존재하는 결과와 병합을 위해 기존 파일 로드
    out_path = output_path(ticker, date)
    if os.path.exists(out_path):
        prev = pd.read_csv(out_path)
    else:
        prev = pd.DataFrame(columns=OUTPUT_COLUMNS)


# 처리
    rows = []
    for _, row in df.iterrows():
        content = (row.get("content") or "").strip()
        if not content:
            # 비어있으면 스킵(필요 시 summary="", sentiment="unknown" 등으로 기록 가능)
            continue
        summary = summarize_long(content)
        senti = infer_sentiment(summary if summary else content)
        kw = extract_keywords(summary if summary else content)
        rows.append({
            "summary": summary,
            "sentiment": senti,
            "keywords": kw,
        })


    cur = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


    # 병합 및 중복 제거
    merged = pd.concat([prev, cur], ignore_index=True)
    if DEDUP_SUBSET:
        merged.drop_duplicates(subset=DEDUP_SUBSET, inplace=True)


    write_results(out_path, merged)
    return out_path
