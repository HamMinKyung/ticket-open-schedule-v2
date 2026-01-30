from typing import Dict, List, Tuple
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
import re

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import normalize_title


class TicketLinkCrawler(AsyncCrawlerBase):
    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['ticket_link']
        self.list_url = f"{self.cfg['base_url']}{self.cfg['list_endpoint']}"
        self.headers = {**self.headers, **self.cfg['headers']}

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict]:
        results: List[Dict] = []
        page = 1
        print("Start fetching list from TicketLink.")
        while True:
            print(f"Fetching page {page}...")
            params = {**self.cfg["params"], "page": page}
            async with session.get(self.list_url, params=params, headers=self.headers) as res:
                res.raise_for_status()
                data = await res.json()

                result_data = data.get("result", {})
                if not result_data:
                    print("No 'result' in response data. Stopping.")
                    break

                items = result_data.get("result", [])
                print(f"Found {len(items)} items on page {page}.")
                if not items:
                    print("No more items found. Stopping.")
                    break

                for item in items:
                    open_date_ts = item.get("ticketOpenDatetime")
                    if not open_date_ts:
                        continue

                    try:
                        open_time = datetime.fromtimestamp(open_date_ts / 1000)
                        if self.start <= open_time <= self.end:
                            results.append(item)
                    except (ValueError, TypeError) as e:
                        print(f"Could not parse timestamp for item {item.get('noticeId')}: {e}")
                        continue

                paging_info = result_data.get("paging", {})
                current_page = paging_info.get("currentPage", 1)
                total_pages = paging_info.get("pageCount", 1)

                if current_page >= total_pages:
                    break
                page += 1
        print(f"Finished fetching list. Total items collected: {len(results)}")
        return results

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict) -> List[TicketInfo]:
        notice_id = item.get("noticeId")
        if not notice_id:
            return []
        detail_url = f"{self.cfg['base_url']}{self.cfg['detail_endpoint']}{notice_id}"

        try:
            async with session.get(detail_url, headers=self.headers) as res:
                res.raise_for_status()
                data = await res.json()
        except Exception as e:
            print(f"Error fetching detail JSON for {item.get('title')}: {e}")
            return []

        notice = data.get("notice", {}) or {}

        raw_title = notice.get("title") or item.get("title") or "-"
        title_text = BeautifulSoup(raw_title, "html.parser").get_text(separator=" ", strip=True)
        title_text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", title_text)
        is_exclusive = "단독판매" in title_text or "단독 판매" in title_text

        venue = notice.get("placeName") or item.get("placeName") or "-"
        region = self._extract_region(venue, title_text)

        print(f"지역 정보 org {venue}. conversion {region}")
        
        if not region:
            print(f"[Skip] Region not allowed/matched: title='{title_text}', venue='{venue}'")
            return []

        category = self._category_from_title(title_text)
        if category == "-":
            category = notice.get("noticeCategoryName") or "티켓오픈"

        content_html = notice.get("content") or ""
        body_soup = BeautifulSoup(content_html, "html.parser")
        body_text = body_soup.get_text("\n", strip=True)
        period = self._pick_performance_period(body_text) or "-"
        open_round = period

        reserveWebUrl = notice.get("reserveWebUrl") or item.get("reserveWebUrl") or ""
        open_type = "일반예매" if reserveWebUrl else "-"

        cast_str = self.extract_cast_from_body(body_soup)

        sections = self._extract_sections_from_body(body_soup)
        if period and period != "-":
            if sections.get("공연정보"):
                if period not in sections["공연정보"]:
                    sections["공연정보"] = (sections["공연정보"].rstrip() + "\n" + period).strip()
            else:
                sections["공연정보"] = period

        product_url = ""
        if reserveWebUrl:
            product_url = f"{self.cfg['base_url']}{reserveWebUrl}"

        ts = notice.get("ticketOpenDatetime") or data.get("ticketOpenDatetime") or item.get("ticketOpenDatetime")
        if not ts:
            print(f"Missing ticketOpenDatetime for noticeId={notice_id}")
            return []
        open_dt = datetime.fromtimestamp(int(ts) / 1000)

        ticket = TicketInfo(
            title=normalize_title(title_text),
            open_datetime=open_dt,
            round_info=open_round,
            cast=cast_str,
            detail_url=product_url or "-",
            category=str(category).strip(),
            open_type=open_type,
            venue=venue.strip() if isinstance(venue, str) else "-",
            providers={"티켓링크"},
            solo_sale=is_exclusive,
            content=sections,
            source="티켓링크",
            regions=region,
        )
        return [ticket]

    @staticmethod
    def _normalize_text(s: str) -> str:
        s = re.sub(r"\r\n?", "\n", s or "")
        s = re.sub(r"[ \t]+", " ", s)
        return s.strip()

    @staticmethod
    def _extract_sections_from_body(body_html: BeautifulSoup) -> dict:
        if not body_html: return {}
        text = body_html.get_text("\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        pat = re.compile(
            r"^(?P<hdr>(?:공연\s*정보|할인\s*정보|공연\s*내용|기획사\s*정보))\s*[:：]?\s*$",
            re.MULTILINE
        )
        positions = []
        for m in pat.finditer(text):
            hdr_raw = m.group("hdr")
            hdr_norm = hdr_raw.replace(" ", "")
            key = ""
            if "공연정보" in hdr_norm:
                key = "공연정보"
            elif "할인정보" in hdr_norm:
                key = "할인정보"
            elif "공연내용" in hdr_norm:
                key = "공연내용"
            elif "기획사정보" in hdr_norm:
                key = "기획사정보"
            else:
                continue
            positions.append((key, m.start(), m.end()))

        _SECTION_KEYS = ["공연정보", "할인정보", "공연내용", "기획사정보"]
        sections = {k: "" for k in _SECTION_KEYS}
        if not positions:
            sections["공연정보"] = text
            return sections

        positions.sort(key=lambda x: x[1])
        for (idx, (key, _s, e)) in enumerate(positions):
            start = e
            end = positions[idx + 1][1] if idx + 1 < len(positions) else len(text)
            chunk = text[start:end].strip()
            sections[key] = chunk
        return sections

    @staticmethod
    def _extract_region(venue: str, title_text: str, default_region: str = "서울") -> str | None:
        DISALLOWED_KEYWORDS = ["인천", "대구", "광주", "대전", "세종", "강원", "강원도", "충북", "충청북도", "충남", "충청남도", "전북", "전라북도",
                               "전남", "전라남도", "경북", "경상북도", "경남", "경상남도", "제주", "제주도", "포항", "경주", "구미", "창원", "김해",
                               "진주", "전주", "여수", "순천", "목포", "청주", "천안", "아산", "당진", "춘천", "원주", "강릉", "서귀포"]
        REGION_MAP = {"서울": r"(서울|Seoul)", "경기": r"(경기|수원|용인|성남|안산|의왕|안양|평촌|고양|파주|부천|하남|과천|광명)", "부산": r"(부산|Busan)",
                      "울산": r"(울산)"}
        corpus = " ".join(filter(None, [venue, title_text]))
        EXCLUDE_PATTERNS = re.compile(r"(?:%s)" % "|".join(map(re.escape, DISALLOWED_KEYWORDS)), re.I)
        if EXCLUDE_PATTERNS.search(corpus):
            return None
        for region, pat in REGION_MAP.items():
            if re.search(pat, corpus, re.I):
                return region
        return default_region

    @staticmethod
    def _pick_performance_period(text: str) -> str | None:
        if not text: return None
        text = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", text)
        m = re.search(r"^\s*[-–—•]?\s*공연\s*기간\s*[:：]\s*(.+)$", text, re.MULTILINE)
        if m: return m.group(1).strip()
        m2 = re.search(r"^\s*[-–—•]?\s*공연\s*기간\s*[-–—]\s*(.+)$", text, re.MULTILINE)
        if m2: return m2.group(1).strip()
        return None

    @staticmethod
    def _category_from_title(title: str) -> str:
        if not title: return "-"
        cats = ["뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시"]
        t = title.strip()
        for c in cats:
            if re.search(rf"(?:\s*\[?{re.escape(c)}\]?|\b{re.escape(c)}\b)", t, re.IGNORECASE):
                return c
        return "-"

    CAST_HEADER_PAT = re.compile(r"(출연|출연진|캐스팅|CAST|Cast|Casting|배우|\[\s*CAST\s*\])\s*$", re.I)
    NEXT_SECTION_PAT = re.compile(r"(공연\s*정보|할인\s*정보|공연\s*내용|기획사\s*정보|\[\s*CREATIVE\s*\])\s*$", re.I)

    @staticmethod
    def _norm_txt(el) -> str:
        return el.get_text(" ", strip=True) if el else ""

    @staticmethod
    def extract_cast_from_body(body_soup: BeautifulSoup) -> str:
        if not body_soup: return "-"
        found = False
        lines: List[str] = []
        for el in body_soup.find_all(["p", "div"]):
            txt = TicketLinkCrawler._norm_txt(el)
            if not found and ("캐스팅" in txt or "CAST" in txt or "출연진" in txt):
                found = True
                continue
            if found:
                if txt == "" or txt.startswith("[") or txt.startswith("※") or txt.startswith("기획사정보"):
                    break
                lines.append(txt)

        return ", ".join(lines)
