import re

def normalize_date_string(date_text: str) -> str:
    # 1. 괄호 내부 제거
    text = re.sub(r"\([^)]+\)", "", date_text)

    # 2. 다중 공백 정리
    text = " ".join(text.split())

    return text

def normalize_title_key(text: str) -> str:
    # 공연 제목에서 "뮤지컬", "연극", "콘서트" 등의 키워드를 제거합니다.
    text = re.sub(r'[〈《<〔【]', '<', text)
    text = re.sub(r'[〉》>〕】]', '>', text)
    text = re.sub(r'\s+', '', text)
    return text.strip()