import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

import aiohttp
from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, extract_open_round_period, extract_performance_period, normalize_title, resolve_region

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
        items: List[Dict[str, Any]] = []
        for article in data.get("ArticleTitles", []):
            title = article.get("Title", "")
            if article.get("CategoryID") != 17 or "티켓" not in title:
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
        text = BeautifulSoup(content_html, "html.parser").get_text("\n", strip=True)

        open_dt = self._extract_open_datetime(text)
        if not open_dt or not (self.start <= open_dt <= self.end):
            return []

        title = self._extract_field(text, "공연명") or raw_title
        title = self._strip_notice_title(title)
        venue = self._extract_field(text, "공연장소") or "LG아트센터 서울"
        region = resolve_region(venue, title)
        if not region:
            logger.debug(f"[LGArtCrawler] 지역 필터 제외: title={title!r}, venue={venue!r}")
            return []

        performance_period = extract_performance_period(text) or "-"
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
            category=self._category_from_title(title),
            open_type="티켓오픈",
            venue=venue,
            providers={"LG 아트센터"},
            solo_sale=False,
            content={"공지": text},
            source="LG 아트센터",
            regions=region,
        )]

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
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _extract_field(text: str, key: str) -> str | None:
        key_pattern = r"\s*".join(map(re.escape, key))
        pattern = re.compile(rf"^\s*[-*]?\s*{key_pattern}\s*[:：]\s*(.+)$", re.M)
        match = pattern.search(text)
        return match.group(1).strip() if match else None

    def _extract_open_datetime(self, text: str) -> datetime | None:
        candidates = []
        for line in text.splitlines():
            if "티켓" in line and ("오픈" in line or "예매" in line) and "일시" in line:
                candidates.append(line)
        candidates.append(text)

        for candidate in candidates:
            dt = self._parse_korean_datetime(candidate)
            if dt:
                return dt
        return None

    @staticmethod
    def _extract_cast(text: str) -> str:
        cast = extract_cast_from_lines(text.splitlines())
        if not cast or cast == "-" or re.fullmatch(r"[\[\]［］()（）\s]+", cast):
            return "-"
        return cast

    @staticmethod
    def _parse_korean_datetime(text: str) -> datetime | None:
        date_match = re.search(
            r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일(?:\([^)]*\))?",
            text,
        )
        if not date_match:
            date_match = re.search(r"(\d{4})[.\-/]\s*(\d{1,2})[.\-/]\s*(\d{1,2})", text)
        if not date_match:
            return None

        year, month, day = map(int, date_match.groups())
        tail = text[date_match.end():]
        ampm = None
        if "오전" in tail:
            ampm = "AM"
        elif "오후" in tail:
            ampm = "PM"

        time_match = re.search(r"(?:(?:오전|오후)\s*)?(\d{1,2})(?:\s*[:시]\s*(\d{1,2}))?", tail)
        if not time_match:
            return None

        hour = int(time_match.group(1))
        minute = int(time_match.group(2) or 0)
        if ampm == "PM" and hour < 12:
            hour += 12
        if ampm == "AM" and hour == 12:
            hour = 0
        return datetime(year, month, day, hour, minute)

    @staticmethod
    def _category_from_title(title: str) -> str:
        for category in ["뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시"]:
            if category in title:
                return category
        return "공연"
