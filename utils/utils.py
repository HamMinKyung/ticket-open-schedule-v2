import re

def normalize_date_string(date_text: str) -> str:
    # 괄호 내부 제거 · 다중 공백 축소
    text = re.sub(r"\([^)]+\)", "", date_text)
    return " ".join(text.split())
