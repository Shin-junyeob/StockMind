from .nlp_models import get_sentiment
def infer_sentiment(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return "unknown"
    try:
        res = get_sentiment()(text[:512])[0] # FinBERT: 앞부분만으로도 충분
        return res["label"].lower()
    except Exception as e:
        return f"error: {e}"

