import re

def normalize_date_string(date_text: str) -> str:
    # 1. 괄호 내부 제거
    text = re.sub(r"\([^)]+\)", "", date_text)

    # 2. 다중 공백 정리
    text = " ".join(text.split())

    return text

def normalize_title(text: str) -> str:
    # 괄호와 그 안의 내용 제거 (e.g., (서울), [앵콜])
    text = re.sub(r'\s*[\[(].*?[\])]\s*', ' ', text)
    # 특수 문자나 구분자를 공백으로 변환
    text = re.sub(r'[〈<《〔【]', '〈', text)
    text = re.sub(r'[>》〕】〉]', '〉', text)
    # '티켓오픈' 관련 문구 제거
    text = re.sub(r'\d*차?\s*티켓\s*오?픈(?:\s*안내)?', ' ', text, flags=re.IGNORECASE)
    # 여러 공백을 하나로
    text = ' '.join(text.split())
    return text.strip()