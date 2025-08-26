from transformers import pipeline
from transformers.utils import logging
from keybert import KeyBERT

from .settings import SUMMARIZER_MODEL, SENTIMENT_MODEL

logging.set_verbosity_error()

# Lazy-loaded 싱글톤 파이프라인/모델
_summarizer = None
_sentiment = None
_kw_model = None

def get_summarizer():
    global _summarizer
    if _summarizer is None:
        _summarizer = pipeline("summarization", model=SUMMARIZER_MODEL)
    return _summarizer

def get_sentiment():
    global _sentiment
    if _sentiment is None:
        _sentiment = pipeline("sentiment-analysis", model=SENTIMENT_MODEL)
    return _sentiment

def get_kw_model():
    global _kw_model
    if _kw_model is None:
        _kw_model = KeyBERT()
    return _kw_model

