import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

import aiohttp
from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, normalize_title, resolve_region

logger = logging.getLogger(__name__)


class Yes24Crawler(AsyncCrawlerBase):
    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS["yes24"]
        self.base_url = self.cfg["base_url"]
        self.list_url = f"{self.base_url}{self.cfg['list_endpoint']}"
        self.detail_url = f"{self.base_url}{self.cfg['detail_endpoint']}"
        self.headers = {**self.headers, **self.cfg["headers"]}

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []

        for page in self.cfg["pages"]:
            payload = {**self.cfg["params"], "page": str(page)}
            async with session.post(self.list_url, data=payload, headers=self.headers) as resp:
                resp.raise_for_status()
                html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            rows = soup.select("div.noti-tbl table tbody tr")
            if len(rows) <= 1:
                break

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                notice_type = cells[0].get_text(strip=True)
                if "티켓오픈" not in notice_type:
                    continue

                title_link = cells[1].find("a", href=True)
                if not title_link:
                    continue

                notice_id = self._extract_notice_id(title_link["href"])
                if not notice_id:
                    continue

                raw_title = title_link.get_text(" ", strip=True)
                solo_sale = "단독판매" in raw_title
                title_for_region = raw_title.replace("단독판매", "").strip()
                title = normalize_title(title_for_region)

                for open_type, open_dt in self._extract_open_entries(cells[2]):
                    if self.start <= open_dt <= self.end:
                        results.append({
                            "notice_id": notice_id,
                            "title": title,
                            "raw_title": title_for_region,
                            "open_datetime": open_dt,
                            "open_type": open_type,
                            "solo_sale": solo_sale,
                            "notice_url": f"{self.base_url}/Notice?#id={notice_id}",
                        })

        return results

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict[str, Any]) -> List[TicketInfo]:
        payload = {
            "bId": item["notice_id"],
            "genre": "",
            "province": "",
            "order": self.cfg["params"].get("order", "2"),
        }
        async with session.post(self.detail_url, data=payload, headers=self.headers) as resp:
            resp.raise_for_status()
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        content = self._extract_sections(soup)
        overview = self._pick_first_section(content, "공연 개요", "공연개요", "개요")

        title = self._pick_first_overview_value(overview, "공연 제목", "공연명") or item["title"]
        round_info = (
            extract_open_round(item.get("open_type", ""), item.get("raw_title", ""), overview)
            or self._build_round_info(overview)
        )
        venue = self._pick_first_overview_value(overview, "공연 장소", "공연장소", "장소") or "-"
        cast = self._extract_cast(content) or "-"
        category = self._category_from_title(title)
        region = resolve_region(venue, item.get("raw_title", title))
        if not region:
            logger.debug(f"[Yes24Crawler] 지역 필터 제외: title={title!r}, venue={venue!r}")
            return []

        solo_sale = item["solo_sale"] or bool(soup.select_one(".noti-vt-tit span"))
        product_url = self._extract_product_url(soup) or item["notice_url"]

        return [TicketInfo(
            title=normalize_title(title),
            open_datetime=item["open_datetime"],
            round_info=round_info,
            cast=cast,
            detail_url=product_url,
            category=category,
            open_type=item["open_type"],
            venue=venue,
            providers={"YES24"},
            solo_sale=solo_sale,
            content=content,
            source="YES24",
            regions=region,
        )]

    @staticmethod
    def _extract_notice_id(href: str) -> str | None:
        match = re.search(r"id=(\d+)", href or "")
        return match.group(1) if match else None

    def _extract_open_entries(self, date_cell) -> List[Tuple[str, datetime]]:
        entries: List[Tuple[str, datetime]] = []
        main_text = next(date_cell.stripped_strings, "")
        main_dt = self._parse_datetime(main_text)
        if main_dt:
            entries.append(("티켓오픈", main_dt))

        for pop in date_cell.select("a.noti-btn-pop"):
            for idx in (1, 2):
                open_type = (pop.get(f"presaletit{idx}") or pop.get(f"presaleTit{idx}") or "").strip()
                open_time = (pop.get(f"presaletime{idx}") or pop.get(f"presaleTime{idx}") or "").strip()
                dt = self._parse_datetime(open_time)
                if open_type and dt:
                    entries.append((open_type, dt))

        deduped: List[Tuple[str, datetime]] = []
        seen = set()
        for open_type, dt in entries:
            key = (open_type, dt)
            if key not in seen:
                seen.add(key)
                deduped.append((open_type, dt))
        return deduped

    @staticmethod
    def _parse_datetime(raw: str) -> datetime | None:
        if not raw:
            return None
        text = re.sub(r"\([^)]*\)", "", raw)
        text = text.replace("오전", "AM").replace("오후", "PM")
        text = re.sub(r"\s+", " ", text).strip()

        for fmt in ("%Y.%m.%d %H:%M", "%Y.%m.%d %p %I:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        logger.debug(f"[Yes24Crawler] 날짜 파싱 실패: {raw!r}")
        return None

    @staticmethod
    def _extract_sections(soup: BeautifulSoup) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        for box in soup.select(".noti-view-coment"):
            title_tag = box.select_one(".noti-view-comen-tit")
            text_tag = box.select_one(".noti-view-comen-txt")
            if not title_tag or not text_tag:
                continue
            key = title_tag.get_text(strip=True)
            value = text_tag.get_text("\n", strip=True)
            if key and value:
                sections[key] = value
        return sections

    @staticmethod
    def _pick_overview_value(text: str, key: str) -> str | None:
        if not text:
            return None
        compact_key = re.sub(r"\s+", "", key)
        key_pattern = r"\s*".join(map(re.escape, compact_key))
        pattern = re.compile(rf"^\s*{key_pattern}\s*[:：]\s*(.+)$", re.MULTILINE)
        match = pattern.search(text)
        return match.group(1).strip() if match else None

    @staticmethod
    def _pick_first_section(content: Dict[str, str], *keys: str) -> str:
        compact_map = {re.sub(r"\s+", "", key): value for key, value in content.items()}
        for key in keys:
            value = compact_map.get(re.sub(r"\s+", "", key))
            if value:
                return value
        return ""

    def _pick_first_overview_value(self, text: str, *keys: str) -> str | None:
        for key in keys:
            value = self._pick_overview_value(text, key)
            if value:
                return value
        return None

    def _build_round_info(self, overview: str) -> str:
        period = self._pick_first_overview_value(overview, "공연 기간", "공연기간", "기간", "일시")
        time = self._pick_first_overview_value(overview, "공연 시간", "공연시간", "시간")
        return " ".join(part for part in [period, time] if part) or "-"

    @staticmethod
    def _extract_cast(content: Dict[str, str]) -> str | None:
        for text in content.values():
            cast = extract_cast_from_lines(text.splitlines())
            if cast != "-":
                return cast
        return None

    @staticmethod
    def _extract_product_url(soup: BeautifulSoup) -> str | None:
        for link in soup.select(".noti-vt-btns a[href]"):
            href = link.get("href", "")
            match = re.search(r"/Perf/(\d+)", href)
            if match:
                return f"https://ticket.yes24.com/Perf/{match.group(1)}"
        return None

    @staticmethod
    def _category_from_title(title: str) -> str:
        cats = ["뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시"]
        for category in cats:
            if category in title:
                return category
        return "공연"
