from .nlp_models import get_kw_model
from .settings import KEYWORDS_TOPK

def extract_keywords(text: str, top_k: int = KEYWORDS_TOPK) -> str:
    def _s(x): return x.strip() if isinstance(x, str) else ""
    text = _s(text)
    if not text:
        return ""
    try:
        pairs = get_kw_model().extract_keywords(text, top_n=top_k)
        return ", ".join([_s(w) for w, _ in pairs])
    except Exception as e:
        return f"error: {e}"

