import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, normalize_title

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
        soup = BeautifulSoup(html, "html.parser")
        items = []
        seen = set()
        for link in soup.select("a[href*='/community/notice/']"):
            href = link.get("href", "")
            if not re.search(r"/community/notice/\d+", href):
                continue
            url = urljoin(self.cfg["base_url"], href)
            if url in seen:
                continue
            text = " ".join(link.get_text(" ", strip=True).split())
            if "티켓공지" not in text and "티켓오픈" not in text and "티켓 오픈" not in text:
                continue
            seen.add(url)
            items.append({"detail_url": url, "title": text})
        return items

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict[str, Any]) -> List[TicketInfo]:
        async with session.get(item["detail_url"], headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n", strip=True)
        title = self._extract_title(soup, item.get("title", ""))
        title = normalize_title(self._strip_notice_title(title))
        period = self._extract_value(text, r"오픈\s*공연\s*기간") or "-"
        cast = extract_cast_from_lines(text.splitlines())
        round_info = extract_open_round(title, text) or "-"
        tickets = []
        for open_type, open_dt in self._extract_open_datetimes(text):
            tickets.append(TicketInfo(
                title=title, open_datetime=open_dt, round_info=round_info,
                performance_period=period, cast=cast, detail_url=item["detail_url"],
                category=self._category_from_title(title), open_type=open_type,
                venue="충무아트센터", providers={"충무아트센터"}, solo_sale=True,
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
                result.append((label, dt))
        return result

    @staticmethod
    def _parse_datetime(text: str) -> datetime | None:
        match = re.search(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2}).*?(오전|오후)?\s*(\d{1,2})(?:시|[:：](\d{2}))?", text)
        if not match:
            return None
        year, month, day = map(int, match.group(1, 2, 3))
        hour = int(match.group(5))
        minute = int(match.group(6) or 0)
        if match.group(4) == "오후" and hour < 12:
            hour += 12
        return datetime(year, month, day, hour, minute)

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
