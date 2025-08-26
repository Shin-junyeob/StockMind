import re
import textwrap

from .nlp_models import get_summarizer
from .settings import CHUNK_CHARS, SUM_MAX_LEN, SUM_MIN_LEN

def _clean_text(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = x.strip()
    x = re.sub(r"\s+", " ", x)
    return x

def summarize_long(text: str) -> str:
    text = _clean_text(text)
    if not text:
        return ""
    chunks = textwrap.wrap(text, CHUNK_CHARS)
    s = get_summarizer()
    outputs = []
    for ch in chunks:
        try:
            out = s(ch, max_length=SUM_MAX_LEN, min_length=SUM_MIN_LEN, do_sample=False)[0]["summary_text"]
        except Exception as e:
            out = f"[요약 오류: {e}]"
        outputs.append(out)
    return " ".join(outputs).strip()

