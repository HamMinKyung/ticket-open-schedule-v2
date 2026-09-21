import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import unquote

import aiohttp
from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, extract_open_round_period, normalize_title, resolve_region

logger = logging.getLogger(__name__)


class LGArtCrawler(AsyncCrawlerBase):
    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS["lg_art"]
        self.base_url = self.cfg["base_url"]
        self.list_url = f"{self.base_url}{self.cfg['list_endpoint']}"
        self.headers = {**self.headers, **self.cfg["headers"]}

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        async with session.get(self.list_url, headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()

        data = self._extract_vue_data(html, "ArticleTitles")
        category_id = next(
            (category["Value"] for category in data.get("Categories", [])
             if category.get("Name") == "티켓"), None
        )
        if category_id is None or not isinstance(data.get("Filter"), dict):
            raise ValueError("LG아트센터 티켓 필터를 찾을 수 없습니다")

        # 사이트의 clickTab → createHistoryBackUrl 동작을 그대로 수행합니다.
        filters = {**data["Filter"], "CategoryID": category_id, "PageIndex": 1}
        async with session.post(
            f"{self.base_url}/api/historyBack/create",
            data={"value": json.dumps(filters, ensure_ascii=False)},
            headers={**self.headers, "Referer": self.list_url},
        ) as resp:
            resp.raise_for_status()
            result = await resp.json()
        if result.get("Code") != 0 or not result.get("Tag"):
            raise ValueError("LG아트센터 티켓 필터 주소 생성 실패")

        async with session.get(
            self.list_url, params={"q": unquote(result["Tag"])}, headers=self.headers
        ) as resp:
            resp.raise_for_status()
            html = await resp.text()
        data = self._extract_vue_data(html, "ArticleTitles")
        applied_filter = data.get("Filter", {})
        if (applied_filter.get("CategoryID") != category_id
                or applied_filter.get("PageIndex") != 1):
            raise ValueError("LG아트센터 티켓 1페이지 필터가 적용되지 않았습니다")

        items: List[Dict[str, Any]] = []
        for article in data.get("ArticleTitles", []):
            title = article.get("Title", "")
            if article.get("CategoryID") != category_id or "티켓" not in title:
                continue
            detail_path = article.get("DetailsUrl")
            if not detail_path:
                continue
            items.append({
                "article_id": article.get("ArticleID"),
                "title": title,
                "detail_url": f"{self.base_url}{detail_path}",
            })
        return items

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict[str, Any]) -> List[TicketInfo]:
        async with session.get(item["detail_url"], headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()

        data = self._extract_vue_data(html, "Article")
        article = data.get("Article", {})
        raw_title = article.get("Title") or item["title"]
        content_html = article.get("Contents") or ""
        text = self._content_text(content_html)

        published = datetime.fromisoformat(article["CreateDate"]) if article.get("CreateDate") else self.start
        openings = self._extract_open_datetimes(text, published)
        if not openings:
            return []

        title = self._extract_field(text, "공연명") or raw_title
        title = self._strip_notice_title(title)
        venue = self._extract_field(text, "공연장소") or self._extract_field(text, "장소") or "LG아트센터 서울"
        region = resolve_region(venue, title)
        if not region:
            logger.debug(f"[LGArtCrawler] 지역 필터 제외: title={title!r}, venue={venue!r}")
            return []

        performance_period = self._performance_period(text)
        round_info = extract_open_round_period(text) or extract_open_round(raw_title, text) or "-"
        cast = self._extract_cast(text)
        detail_url = item["detail_url"]

        return [TicketInfo(
            title=normalize_title(title),
            open_datetime=open_dt,
            round_info=round_info,
            performance_period=performance_period,
            cast=cast,
            detail_url=detail_url,
            category=self._category_from_title(raw_title),
            open_type=open_type,
            venue=venue,
            providers={"LG 아트센터"},
            solo_sale=bool(re.search(r"단독\s*판매", text)),
            content={"공지": text},
            source="LG 아트센터",
            regions=region,
        ) for open_type, open_dt in openings]

    @staticmethod
    def _extract_vue_data(html: str, required_key: str) -> Dict[str, Any]:
        decoder = json.JSONDecoder()
        for match in re.finditer(r"data:\s*", html):
            start = match.end()
            while start < len(html) and html[start].isspace():
                start += 1
            if start >= len(html) or html[start] != "{":
                continue
            try:
                data, _ = decoder.raw_decode(html[start:])
            except json.JSONDecodeError:
                continue
            if required_key in data:
                return data
        return {}

    @staticmethod
    def _strip_notice_title(title: str) -> str:
        text = re.sub(r"^\s*\[[^\]]*티켓[^\]]*\]\s*", " ", title)
        text = re.sub(r"\s*티켓\s*오?픈\s*안내\s*$", " ", text, flags=re.I)
        text = re.sub(
            r"\s*(?:[-–—]\s*)?(?:(?:\d+층|발코니석)(?:\([^)]*\))?\s*)?"
            r"(?:좌석\s*추가|추가\s*좌석|추가)\s*(?:오픈\s*)?(?:안내\s*)?$",
            "", text,
        )
        text = re.sub(r"\s+(?:\d+\s*차|마지막|최종)\s*$", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _extract_field(text: str, key: str) -> str | None:
        key_pattern = r"\s*".join(map(re.escape, key))
        pattern = re.compile(rf"^[ \t]*[-*•·]?[ \t]*{key_pattern}\s*[:：][ \t]*(.+)$", re.M)
        match = pattern.search(text)
        return match.group(1).strip() if match else None

    @staticmethod
    def _content_text(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("br"):
            tag.replace_with("\n")
        for tag in soup.find_all(["p", "div", "li", "tr", "h1", "h2", "h3"]):
            tag.insert_before("\n")
            tag.append("\n")
        return "\n".join(" ".join(line.split()) for line in soup.get_text().splitlines() if line.strip())

    @classmethod
    def _performance_period(cls, text: str) -> str:
        for key in ("공연기간", "공연일시", "공연일정", "공연일자"):
            value = cls._extract_field(text, key)
            if value:
                return value
        section = re.search(r"\[\s*공연\s*(?:정보|개요)\s*\]([^\[]*)", text)
        if section:
            for key in ("기간", "일시"):
                value = cls._extract_field(section[1], key)
                if value:
                    return value
        return "-"

    def _extract_open_datetimes(self, text: str, published: datetime) -> List[Tuple[str, datetime]]:
        result = []
        in_open_section = False
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if re.fullmatch(r"\[.*\]", compact):
                in_open_section = "오픈" in compact or "예매" in compact
                continue
            if any(word in compact for word in ("공연기간", "공연일", "중단", "마감", "취소", "할인")):
                continue
            if not (re.search(r"(?:티켓.*오픈|선오픈|선예매|일반.*(?:오픈|예매))[^:：]*[:：]", line)
                    or (in_open_section and re.match(r"^[•※*\- ]*일시\s*[:：]", line))):
                continue
            candidate = line
            if line.rstrip().endswith((":", "：")) and idx + 1 < len(lines):
                candidate += " " + lines[idx + 1]
            dt = self._parse_korean_datetime(candidate, published)
            if dt and self.start <= dt <= self.end:
                label = "선예매" if re.search(r"선예매|선오픈", line) else "일반예매"
                if (label, dt) not in result:
                    result.append((label, dt))
        return result

    @staticmethod
    def _extract_cast(text: str) -> str:
        # '캐스팅 스케줄은 별도 공지' 같은 본문 문장을 출연진 헤더로 오인하지 않습니다.
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if re.match(r"^[-•* ]*(?:출연진|캐스팅|캐스트)\s*[:：]", line):
                return extract_cast_from_lines([line])
            if re.fullmatch(r"\[\s*(?:출연진|캐스팅(?:정보)?|캐스트)\s*\]", line):
                cast_lines = []
                for following in lines[idx + 1:]:
                    if re.match(r"^\[", following):
                        break
                    cast_lines.append(following)
                return "\n".join(cast_lines) or "-"
        return "-"

    @staticmethod
    def _parse_korean_datetime(text: str, published: datetime | None = None) -> datetime | None:
        date_match = re.search(
            r"(?<!\d)(?:(?P<year>\d{4}|\d{2})\s*(?:년|[./-])\s*)?"
            r"(?P<month>\d{1,2})\s*(?:월|[./-])\s*(?P<day>\d{1,2})\s*일?", text
        )
        if not date_match:
            return None
        year_text = date_match['year']
        if not year_text and published is None:
            return None
        year = int(year_text) if year_text else published.year
        if year < 100:
            year += 2000
        # Only a time immediately following this date belongs to the opening.
        tail = text[date_match.end():]
        time_match = re.match(
            r"\s*(?:[.(（]?\s*[월화수목금토일](?:요일)?\s*[)）]?\s*)?"
            r"(?P<prefix>오전|오후|낮|저녁|밤)?\s*"
            r"(?P<hour>\d{1,2})(?:\s*[:：]\s*(?P<colon_min>\d{2})|"
            r"\s*시(?:\s*(?P<kor_min>\d{1,2})\s*분?)?|(?=\s*[ap]m))"
            r"\s*(?P<suffix>[ap]m)?", tail, re.I
        )
        if not time_match:
            return None
        hour = int(time_match['hour'])
        minute = int(time_match['colon_min'] or time_match['kor_min'] or 0)
        ampm = time_match['prefix'] or (time_match['suffix'] or '').lower()
        if ampm and not 1 <= hour <= 12:
            return None
        if ampm in ('오후', '낮', '저녁', '밤', 'pm') and hour < 12:
            hour += 12
        elif ampm in ('오전', 'am') and hour == 12:
            hour = 0
        try:
            dt = datetime(year, int(date_match['month']), int(date_match['day']), hour, minute)
            # December announcements may omit the year of a January opening.
            if not year_text and published.month == 12 and dt.month == 1:
                dt = dt.replace(year=year + 1)
            return dt
        except ValueError:
            return None

    @staticmethod
    def _category_from_title(title: str) -> str:
        for category in ["뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시", "무용", "판소리"]:
            if category in title:
                return category
        return "공연"
