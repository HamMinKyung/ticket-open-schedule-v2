import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from crawler.lgart import LGArtCrawler
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, extract_open_round_period, extract_performance_period, normalize_title

logger = logging.getLogger(__name__)


class CaciCrawler(AsyncCrawlerBase):
    """충무아트센터(중구문화재단) 티켓공지 크롤러.

    사이트 목록의 페이지 링크를 따라가지 않고 목록 URL 자체만 요청하므로 1페이지만
    수집한다. 상세 공지 하나에서 선예매/일반예매가 모두 발견되면 각각 티켓으로 만든다.
    """

    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS["caci"]
        self.list_url = self.cfg["list_url"]
        self.headers = {**self.headers, **self.cfg["headers"]}

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        async with session.get(self.list_url, headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()
        data = LGArtCrawler._extract_vue_data(html, "ArticleTitles")
        if "ArticleTitles" not in data:
            raise ValueError("충무아트센터 공지 목록 데이터가 없습니다")
        items = []
        seen = set()
        for article in data["ArticleTitles"]:
            href = article.get("DetailsUrl")
            if not href or article.get("CategoryID") != 17:
                continue
            url = urljoin(self.cfg["base_url"], href)
            if url in seen:
                continue
            text = article.get("Title", "")
            if not re.search(r"티켓\s*오픈", text):
                continue
            seen.add(url)
            items.append({"detail_url": url, "title": text})
        return items

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict[str, Any]) -> List[TicketInfo]:
        async with session.get(item["detail_url"], headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()
        article = LGArtCrawler._extract_vue_data(html, "Article").get("Article")
        if not article:
            raise ValueError("충무아트센터 상세 공지 데이터가 없습니다")
        soup = BeautifulSoup(article.get("Contents") or "", "html.parser")
        text = soup.get_text("\n", strip=True)
        raw_title = article.get("Title") or item["title"]
        title = raw_title
        title = normalize_title(title)
        title = re.sub(r"\s+프리뷰\s*$", "", title)
        period = extract_performance_period(text) or "-"
        cast = extract_cast_from_lines(text.splitlines())
        round_info = extract_open_round_period(text) or extract_open_round(raw_title) or "-"
        tickets = []
        for open_type, open_dt in self._extract_open_datetimes(text):
            tickets.append(TicketInfo(
                title=title, open_datetime=open_dt, round_info=round_info,
                performance_period=period, cast=cast, detail_url=item["detail_url"],
                category=self._category_from_title(title), open_type=open_type,
                venue="충무아트센터", providers={"충무아트센터"}, solo_sale=False,
                content={"공지": text}, source="충무아트센터", regions="서울",
            ))
        return tickets

    def _extract_open_datetimes(self, text: str) -> List[Tuple[str, datetime]]:
        result = []
        for line in text.splitlines():
            if not ("예매" in line or "오픈" in line) or "기간" in line:
                continue
            dt = self._parse_datetime(line)
            if dt and self.start <= dt <= self.end:
                label = "선예매" if "선예매" in line else "일반예매" if "일반예매" in line else "티켓오픈"
                if (label, dt) not in result:
                    result.append((label, dt))
        return result

    @staticmethod
    def _parse_datetime(text: str) -> datetime | None:
        match = re.search(r"(\d{4})\s*[.년/-]\s*(\d{1,2})\s*[.월/-]\s*(\d{1,2})\s*[.일]?\s*(?:\([^)]*\))?\s*(오전|오후)?\s*(\d{1,2})(?:시(?:\s*(\d{1,2})분)?|[:：](\d{2}))", text)
        if not match:
            return None
        year, month, day = map(int, match.group(1, 2, 3))
        hour = int(match.group(5))
        minute = int(match.group(6) or match.group(7) or 0)
        if match.group(4) == "오후" and hour < 12:
            hour += 12
        if match.group(4) == "오전" and hour == 12:
            hour = 0
        try:
            return datetime(year, month, day, hour, minute)
        except ValueError:
            return None

    @staticmethod
    def _extract_value(text: str, label: str) -> str | None:
        match = re.search(rf"{label}\s*[:：]?\s*([^\n]+)", text)
        return match.group(1).strip() if match else None

    @staticmethod
    def _extract_title(soup: BeautifulSoup, fallback: str) -> str:
        heading = soup.select_one("h1, h2, .view_title, .board_view_title")
        return heading.get_text(" ", strip=True) if heading else fallback

    @staticmethod
    def _strip_notice_title(title: str) -> str:
        title = re.sub(r"^\s*(?:알림\s*)?(?:티켓공지\s*)?", "", title)
        return re.sub(r"\s*티켓\s*오픈\s*안내\s*$", "", title, flags=re.I).strip()

    @staticmethod
    def _category_from_title(title: str) -> str:
        for category in ("뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시"):
            if category in title:
                return category
        return "공연"
