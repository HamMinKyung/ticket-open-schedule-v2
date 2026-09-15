import re

def normalize_date_string(date_text: str) -> str:
    # 1. 괄호 내부 제거
    text = re.sub(r"\([^)]+\)", "", date_text)

    # 2. 다중 공백 정리
    text = " ".join(text.split())

    return text

def normalize_title(text: str) -> str:
    # 제목 앞의 연도는 표기용으로 제거한다.
    text = re.sub(r'^\s*\d{4}\s+', ' ', text)
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

    # 지역/장르/오픈 회차처럼 사이트별 제목 앞뒤에 붙는 수식어를 병합 키에서 제거한다.
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
    "경기": r"(경기도|경기(?!장)|수원|용인|성남|안산|의왕|안양|평촌|고양|파주|부천|하남|과천|광명|평택|군포|의정부|양주|동두천|구리|남양주|오산|시흥|이천|안성|김포|포천|여주|가평|양평|연천)",
    "부산": r"(부산|\bBusan\b)",
    "울산": r"(울산|\bUlsan\b)",
    "서울": r"(서울(?!랜드)|\bSeoul\b)",
}
VENUE_REGION_PATTERNS = {
    "경기": r"서울랜드",
    "부산": r"사직실내체육관",
    "울산": r"HD아트센터|울산북구문화예술회관",
    "서울": r"예스24라이브홀|예스24스테이지|예스24아트원|스카이아트홀|구름아래소극장|장충체육관|KBS아레나|예술의전당|홍익대 대학로|대학로|세종문화회관",
}


def _location_region(text: str, default_region: str = "") -> tuple[bool, str | None]:
    """판별 불가와 명시적인 제외 지역을 구분한다."""
    text = re.sub(r"\s+", " ", text or "").strip()
    if not text:
        return False, None
    # NOL은 이 공연장을 서울로 분류하기도 하므로 실제 소재지를 우선한다.
    if re.search(r"인스파이어\s*아레나|\bINSPIRE\s*ARENA\b", text, re.I):
        return True, None
    # 경기도 광주시와 광주광역시를 구분한다.
    if re.search(r"광주광역시", text):
        return True, None
    if "광주" in text and ("경기" in text or default_region == "경기"):
        text = text.replace("광주", "")
        if not re.search("경기", text):
            text = "경기도 " + text
    unsupported = "|".join(
        r"세종(?!문화회관|대학교|대왕)" if kw == "세종" else re.escape(kw)
        for kw in UNSUPPORTED_REGION_KEYWORDS
    )
    if re.search(unsupported, text, re.I):
        return True, None
    for region, pattern in REGION_PATTERNS.items():
        if re.search(pattern, text, re.I):
            return True, region
    return False, None


def resolve_region(
    venue: str = "", title: str = "", address: str = "", *, default_region: str = ""
) -> str | None:
    """주소 → 공연장 지역 → 제목의 지역 표기 → 공연장 별칭 → 제공 지역 순으로 판별."""
    for value in (address, venue):
        matched, region = _location_region(value, default_region)
        if matched:
            return region

    # 작품명이나 출연자 이름에 포함된 지명은 지역 근거로 사용하지 않는다.
    labels = re.findall(r"[\[［(（]([^\]］)）]+)[\]］)）]", title or "")
    labels += re.findall(r"\s[-–—]\s*([^–—]+)$|\bin\s+([A-Za-z]+)\s*$", title or "", re.I)
    for label in labels:
        if isinstance(label, tuple):
            label = next((part for part in label if part), "")
        # '서울의 별', '음성' 같은 작품 제목을 지명으로 부분 매칭하지 않는다.
        label = label.strip()
        region_label = re.sub(r"(?:공연|앵콜|콘서트)\s*$", "", label).strip()
        tokens = set(SUPPORTED_REGIONS) | set(UNSUPPORTED_REGION_KEYWORDS)
        tokens.update({"Seoul", "Busan", "Ulsan", "SEOUL", "BUSAN", "ULSAN"})
        tokens.update({"의정부", "수원", "용인", "성남", "고양", "부천", "하남", "안양", "평택", "군포"})
        if region_label in tokens:
            matched, region = _location_region(region_label, default_region)
            if matched:
                return region

    for region, pattern in VENUE_REGION_PATTERNS.items():
        if re.search(pattern, venue or "", re.I):
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


def normalize_performance_period(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", str(text))
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n-–—•·ㆍ:：")
    return text or None


def extract_performance_period(*values: str) -> str | None:
    for value in values:
        if not value:
            continue
        text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", str(value))
        for raw_line in text.splitlines():
            line = re.sub(r"\s+", " ", raw_line).strip()
            if not line:
                continue
            compact = re.sub(r"\s+", "", line)
            if any(word in compact for word in ("티켓오픈", "티켓오픈일", "오픈일시", "예매일시")):
                continue
            match = re.search(
                r"(?:^|[-–—•·ㆍ]\s*)공연\s*(?:기간|일시)\s*[:：\-–—·ㆍ]?\s*(.+)$",
                line,
                re.I,
            )
            if match:
                period = normalize_performance_period(match.group(1))
                if period:
                    return period
    return None


def extract_open_round_period(*values: str) -> str | None:
    """"오픈 회차", "오픈기간", "N차 티켓오픈 기간"처럼 오픈(회차)에 붙은 값을 추출한다.

    공연 자체의 일정("공연기간"/"공연일시")은 extract_performance_period()가
    담당하므로 여기서는 다루지 않는다.
    """
    for value in values:
        if not value:
            continue
        text = re.sub(r"[​-‏‪-‮]", "", str(value))
        lines = [line.strip("※•-* \t\r") for line in text.splitlines()]
        for idx, line in enumerate(lines):
            if not line:
                continue
            normalized = re.sub(r"\s+", " ", line)

            # 예: "오픈 회차 : ...", "오픈기간: ...", "3차 티켓오픈 기간: ...",
            #     "마지막 티켓 오픈 기간: ...", "4차 오픈기간: ..."
            # "오픈일시"/"예매일시"(단순 오픈 시각)는 "회차"/"기간"이 아니므로 매칭되지 않는다.
            match = re.search(
                r"(?:\d+\s*차\s*)?(?:마지막|앵콜)?\s*(?:티켓\s*)?오픈\s*(?:공연\s*)?(?:회차|기간)\s*[:：]?\s*(.*)$",
                normalized,
                flags=re.I,
            )
            if not match:
                continue

            value_part = re.sub(r"\s*공연\s*$", "", match.group(1).strip())
            if value_part:
                return value_part

            # 값이 다음 줄에 있는 경우
            if idx + 1 < len(lines):
                next_line = re.sub(r"\s+", " ", lines[idx + 1]).strip()
                if next_line:
                    return re.sub(r"\s*공연\s*$", "", next_line)
    return None
