import pandas as pd
from transformers import pipeline
from transformers.utils import logging
import textwrap
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import datetime

now = datetime.datetime.now(); now = now.strftime('%Y-%m-%d')

logging.set_verbosity_error()

def summarize_articles(df, text_col='content', chunk_size=1000, max_len=130, min_len=30):
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
    def summarize_text(text):
        if pd.isna(text) or len(text.strip()) == 0:
            return ""
        chunks = textwrap.wrap(text, chunk_size)
        summaries = []
        for chunk in chunks:
            try:
                summary = summarizer(chunk, max_length=max_len, min_length=min_len, do_sample=False)[0]['summary_text']
                summaries.append(summary)
            except Exception as e:
                summaries.append(f"[요약 오류: {e}]")
        return " ".join(summaries)
    df['summary'] = df[text_col].apply(summarize_text)
    return df

def analyze_sentiment(df, summary_col='summary'):
    sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert")
    def get_sentiment(text):
        if pd.isna(text) or len(text.strip()) == 0:
            return "unknown"
        try:
            result = sentiment_pipeline(text[:512])[0]
            return result['label'].lower()
        except Exception as e:
            return f"error: {e}"
    df['sentiment'] = df[summary_col].apply(get_sentiment)
    return df

def extract_keywords(df, summary_col='summary', top_k=5):
    def clean_text(text):
        text = re.sub(r'[^\w\s]', '', text.lower())
        return text
    cleaned_summaries = df[summary_col].fillna("").apply(clean_text)
    vectorizer = TfidfVectorizer(stop_words='english', max_features=1000)
    tfidf_matrix = vectorizer.fit_transform(cleaned_summaries)
    feature_names = vectorizer.get_feature_names_out()
    keywords_list = []
    for row in tfidf_matrix:
        sorted_indices = row.toarray().flatten().argsort()[::-1]
        top_keywords = [feature_names[i] for i in sorted_indices[:top_k]]
        keywords_list.append(", ".join(top_keywords))
    df['keywords'] = keywords_list
    return df

def process_news_file(input_file, output_file):
    df = pd.read_csv(input_file)
    df.reset_index(drop=True, inplace=True)

    for col in ['summary', 'sentiment', 'keywords']:
        if col not in df.columns:
            df[col] = ""

    i = 0
    while i < len(df):
        row = df.iloc[i]
        if isinstance(row['summary'], str) and row['summary'].strip():
            print(f"🛑 {i}번째 이후는 이미 요약/분석 완료 → 중단")
            break

        print(f"✅ {i}번째 뉴스 summary, sentiment, keyword 분석 ...", end = ' ')
        row_df = pd.DataFrame([row])
        row_df = summarize_articles(row_df, text_col="content")
        row_df = analyze_sentiment(row_df, summary_col="summary")
        row_df = extract_keywords(row_df, summary_col="summary")

        for col in ['summary', 'sentiment', 'keywords']:
            df.at[i, col] = row_df.iloc[0][col]

        print("완료")
        i += 1

    # ✅ 필요한 컬럼만 저장
    df[['summary', 'sentiment', 'keywords']].to_csv(output_file, index=False)
    print(f"✅ 총 {i}건 처리 완료 후 저장됨")

    return df[['summary', 'sentiment', 'keywords']]


if __name__ == "__main__":
    ticker = 'AAPL'
    input_file = f'../news/{ticker}_{now}.csv'
    output_file = f'../features/{ticker}_{now}.csv'
    process_news_file(input_file, output_file)