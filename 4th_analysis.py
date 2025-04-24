'''
가상환경 venv310으로 변경
'''

import pandas as pd
from transformers import pipeline
from transformers.utils import logging
import textwrap
import re
from sklearn.feature_extraction.text import TfidfVectorizer

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
    sentiment_pipeline = pipeline("sentiment-analysis")
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

def process_news_file(file_name):
    df = pd.read_csv(file_name)
    df = summarize_articles(df, text_col="content")
    df = analyze_sentiment(df, summary_col="summary")
    df = extract_keywords(df, summary_col="summary")

    df.to_csv(file_name, index=False)

    return df[['url', 'content', 'summary', 'sentiment', 'keywords']]


result_df = process_news_file("news_AAPL.csv")