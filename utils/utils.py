import re

def normalize_date_string(date_text: str) -> str:
    # 1. 괄호 내부 제거
    text = re.sub(r"\([^)]+\)", "", date_text)

    # 2. 다중 공백 정리
    text = " ".join(text.split())

    return text

def normalize_title(text: str) -> str:
    # 소괄호는 보통 부가 정보로 보고 제거한다.
    text = re.sub(r'\s*\(.*?\)\s*', ' ', text)

    def normalize_square_bracket(match: re.Match) -> str:
        inner = match.group(1).strip()
        drop_words = (
            "서울", "경기", "부산", "울산", "인천", "대구", "대전", "광주", "세종",
            "수원", "성남", "평택", "군포", "앵콜", "단독", "선예매",
        )
        if not inner or any(word == inner for word in drop_words):
            return " "
        return f" 〈{inner}〉 "

    # 대괄호 안 작품명은 보존하고, 지역/판매 수식어만 제거한다.
    text = re.sub(r'\s*[\[［](.*?)[\]］]\s*', normalize_square_bracket, text)
    # 특수 문자나 구분자를 공백으로 변환
    text = re.sub(r'[〈<《〔【]', '〈', text)
    text = re.sub(r'[>》〕】〉]', '〉', text)
    # '티켓오픈' 관련 문구 제거
    text = re.sub(
        r'(?:(?:\d+\s*차\s*팀|\d+\s*차|추가\s*회차|마지막|앵콜)\s*)*티켓\s*오?픈(?:\s*안내)?',
        ' ',
        text,
        flags=re.IGNORECASE,
    )
    # 실제 작품명 괄호 뒤 수식어만 제거한다. 제목 안 화살표로 쓰인 '〉'는 보존한다.
    text = re.sub(r'(〈[^〉]+〉)\s*(?:마지막|앵콜|추가|선예매|단독)?\s*$', r'\1', text)
    # 여러 공백을 하나로
    text = ' '.join(text.split())
    return text.strip()

def normalize_title_for_merge(text: str) -> str:
    text = normalize_title(text)

    # 지역/연도/장르/오픈 회차처럼 사이트별 제목 앞뒤에 붙는 수식어를 병합 키에서 제거한다.
    text = re.sub(r'^\s*\d{4}\s+', ' ', text)
    text = re.sub(r'\b(뮤지컬|연극|콘서트|클래식|오페라|전시|공연)\b', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\b\d+\s*차(?:팀)?\b', ' ', text)
    text = re.sub(r'\b(마지막|앵콜|패키지|하반기|상반기)\b', ' ', text)
    text = re.sub(r'\b(티켓오픈|티켓\s*오픈|오픈\s*안내|티켓\s*오픈\s*안내)\b', ' ', text, flags=re.IGNORECASE)

    # 작품명이 꺾쇠 안에 있으면 그 값을 병합 키로 우선 사용한다.
    bracketed = re.findall(r'〈([^〉]+)〉', text)
    if bracketed:
        text = max(bracketed, key=len)

    text = re.sub(r'[^\w가-힣]+', ' ', text)
    return ' '.join(text.casefold().split())

CAST_HEADER_PATTERN = re.compile(
    r"^\s*(?:[\[［]?\s*)?(출연|출연진|캐스팅|캐스트|배우|CAST|Casting|Line\s*up|라인업)(?:\s*[\]］]?)?\s*[:：-]?\s*$",
    re.I,
)
CAST_INLINE_PATTERN = re.compile(
    r"(?:출연진?|캐스팅|캐스트|배우|CAST|Casting|Line\s*up|라인업)\s*[:：-]\s*(.+)",
    re.I,
)
NEXT_CAST_SECTION_PATTERN = re.compile(
    r"(공연\s*개요|공연\s*정보|공연\s*소개|공연\s*내용|작품\s*소개|시놉시스|줄거리|프로그램|"
    r"할인|기획사|제작|주최|주관|문의|티켓|가격|관람|일시|장소|CREATIVE|STAFF)",
    re.I,
)


def clean_cast_text(text: str | None) -> str:
    if not text:
        return "-"

    text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", text)
    text = re.split(r"[\[［]?\s*(?:CREATIVE|Creative|creative|STAFF|Staff|staff)\s*(?:TEAM|Team|team)?\s*[\]］]?", text, maxsplit=1)[0]
    text = CAST_INLINE_PATTERN.sub(r"\1", text)

    parts = []
    for raw in re.split(r"[\n,;/|｜]+", text):
        part = raw.strip(" \t\r\n※•-*·ㆍ:：")
        if not part or CAST_HEADER_PATTERN.match(part):
            continue
        if NEXT_CAST_SECTION_PATTERN.search(part):
            break
        parts.append(part)

    deduped = []
    seen = set()
    for part in parts:
        key = re.sub(r"\s+", " ", part).casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(part)

    return ", ".join(deduped) if deduped else "-"


def extract_cast_from_lines(lines: list[str]) -> str:
    normalized_lines = [line.strip() for line in lines if line and line.strip()]

    for idx, line in enumerate(normalized_lines):
        inline = CAST_INLINE_PATTERN.search(line)
        if inline:
            cast = clean_cast_text(inline.group(1))
            if cast != "-":
                return cast

        if not CAST_HEADER_PATTERN.match(line) and not re.search(r"(출연진|캐스팅|캐스트|CAST|Casting|라인업)", line, re.I):
            continue

        cast_lines = []
        for nxt in normalized_lines[idx + 1:]:
            if not nxt:
                break
            if CAST_HEADER_PATTERN.match(nxt):
                continue
            if NEXT_CAST_SECTION_PATTERN.search(nxt) or re.match(r"^[\[［].+[\]］]$", nxt):
                break
            cast_lines.append(nxt)

        cast = clean_cast_text("\n".join(cast_lines))
        if cast != "-":
            return cast

    return "-"

SUPPORTED_REGIONS = ("서울", "경기", "부산", "울산")
UNSUPPORTED_REGION_KEYWORDS = (
    "인천", "대구", "광주", "대전", "세종", "강원", "강원도", "충북", "충청북도",
    "충남", "충청남도", "전북", "전라북도", "전남", "전라남도", "경북", "경상북도",
    "경남", "경상남도", "제주", "제주도", "포항", "경주", "구미", "창원", "김해",
    "진주", "전주", "여수", "순천", "목포", "청주", "천안", "아산", "당진",
    "춘천", "원주", "강릉", "서귀포", "음성",
)
REGION_PATTERNS = {
    "경기": r"(경기|수원|용인|성남|안산|의왕|안양|평촌|고양|파주|부천|하남|과천|광명|평택|군포|서울랜드)",
    "부산": r"(부산|Busan|사직실내체육관)",
    "울산": r"(울산|HD아트센터|울산북구문화예술회관)",
    "서울": r"(서울|Seoul|예스24라이브홀|예스24스테이지|예스24아트원|스카이아트홀|구름아래소극장|장충체육관|KBS아레나|예술의전당|홍익대 대학로|대학로)",
}


def resolve_region(*values: str, default_region: str = "서울") -> str | None:
    corpus = " ".join(str(value or "") for value in values)

    unsupported_pattern = r"(?:%s)" % "|".join(map(re.escape, UNSUPPORTED_REGION_KEYWORDS))
    if re.search(unsupported_pattern, corpus, re.I):
        return None

    for region, pattern in REGION_PATTERNS.items():
        if re.search(pattern, corpus, re.I):
            return region

    return default_region if default_region in SUPPORTED_REGIONS else None


def normalize_open_round(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"\s+", " ", str(text)).strip()
    if not text or text == "-":
        return None

    patterns = [
        r"(\d+\s*차\s*(?:티켓\s*)?오픈)",
        r"(\d+\s*차\s*오픈)",
        r"(\d+\s*회차\s*오픈)",
        r"(\d+\s*차\s*팀\s*(?:마지막\s*)?(?:티켓\s*)?오픈)",
        r"((?:마지막|앵콜)\s*(?:티켓\s*)?오픈)",
        r"((?:상반기|하반기)\s*패키지)",
        r"(선예매)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()
    return None


def extract_open_round(*values: str) -> str | None:
    for value in values:
        open_round = normalize_open_round(value)
        if open_round:
            return open_round
    return None
