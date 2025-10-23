from .nlp_models import get_sentiment
def infer_sentiment(text: str) -> str:
    def _s(x): return x.strip() if isinstance(x, str) else ""
    text = _s(text)
    if not text:
        return "unknown"
    try:
        res = get_sentiment()(text[:512])[0] # FinBERT: 앞부분만으로도 충분
        return _s(res.get("label", "unknown")).lower()
    except Exception as e:
        return f"error: {e}"

