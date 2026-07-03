# crawler/melon.py
import logging
import re

logger = logging.getLogger(__name__)

import asyncio
import aiohttp
from bs4 import BeautifulSoup, NavigableString
from datetime import datetime
from typing import List, Dict, Any, Tuple
from crawler.base import AsyncCrawlerBase
from utils.config import settings
from models.ticket import TicketInfo
from utils.utils import clean_cast_text, extract_cast_from_lines, extract_open_round, extract_performance_period, normalize_date_string, normalize_title, resolve_region
import random


class MelonCrawler(AsyncCrawlerBase):
    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['melon']
        self.list_url = self.cfg['list_endpoint']

    def _get_headers(self) -> Dict[str, str]:
        # 설정된 리스트에서 랜덤 추출
        ua_list = self.cfg.get('user_agents')
        return {
            "Referer": self.cfg['Referer'],
            "User-Agent": random.choice(ua_list)
        }

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        # 장르 코드별·페이지별 리스트 수집
        for code, genre_name in self.cfg['genre_map'].items():
            for page in self.cfg['pages']:
                payload = {
                    "schGcode": code,
                    "orderType": "2",
                    "pageIndex": str(page)
                }
                await asyncio.sleep(random.uniform(1.0, 3.0))
                headers = self._get_headers()
                for attempt in range(3):
                    async with session.post(self.list_url, headers=headers, data=payload) as resp:
                        if resp.status == 423:
                            wait = 10 * (attempt + 1)
                            logger.warning(f"[MelonCrawler] 423 Locked - {wait}초 후 재시도 ({attempt + 1}/3)")
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        html = await resp.text()
                        break
                else:
                    logger.error(f"[MelonCrawler] 423 Locked 재시도 초과: genre={genre_name}, page={page}")
                    continue
                soup = BeautifulSoup(html, 'html.parser')

                for li in soup.select("ul.list_ticket_cont li"):
                    title_tag = li.select_one("a.tit")
                    date_tag = li.select_one("span.date")
                    if not title_tag or not date_tag:
                        continue
                    raw_date = date_tag.get_text(strip=True)
                    pass_check = "오픈일정 보기" in raw_date
                    open_date = None

                    # 날짜 문구이면서 범위 내 항목만 추가
                    if not pass_check:
                        try:
                            norm = normalize_date_string(raw_date)
                            dt = datetime.strptime(norm, "%Y.%m.%d %H:%M")
                            if not (self.start <= dt <= self.end):
                                continue
                            open_date = dt
                        except (ValueError, AttributeError) as e:
                            logger.debug(f"날짜 파싱 실패: {raw_date!r} - {e}")
                            continue

                    items.append({
                        "title_tag": title_tag,
                        "pass_date_check": pass_check,
                        "open_date": open_date,
                        "genre": genre_name
                    })
            # 필터링된 항목만 반환
        return items

    async def _fetch_detail(
            self,
            session: aiohttp.ClientSession,
            item: Dict[str, Any]
    ) -> List[TicketInfo]:
        cfg = self.cfg
        # 상세 페이지 URL
        href = item['title_tag']['href'].lstrip("./")
        detail_url = f"{cfg['base_url']}/csoon/{href}"

        headers = self._get_headers()
        async with session.get(detail_url, headers=headers) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, 'html.parser')

        # 기본 정보 파싱
        title_tag = soup.select_one("p.tit_consert")
        if not title_tag:
            logger.debug(f"[MelonCrawler] 상세 제목 없음: {detail_url}")
            return []
        title = title_tag.get_text(strip=True).strip()
        round_info, venue, performance_period = self._parse_base_box(soup)
        round_info = extract_open_round(title, round_info) or "-"
        only_sale = bool(soup.select_one(cfg['detail_selectors']['solo_icon']))
        content = self._parse_content(soup)
        if not performance_period or performance_period == "-":
            performance_period = extract_performance_period(*content.values()) or "-"
        if venue == "-":
            venue = self._extract_venue_from_content(content)
        cast = self._parse_cast_info(soup, content.get("출연진", "-"))
        regions = resolve_region(venue, title)
        if not regions:
            logger.debug(f"[MelonCrawler] 지역 필터 제외: title={title!r}, venue={venue!r}")
            return []

        logger.debug(f"지역 정보 org {venue}. conversion {regions}")
        
        tickets: List[TicketInfo] = []

        # “오픈일정 보기”인 경우, 상세 여러 일정 파싱
        if item['pass_date_check']:
            for label, od in self._parse_open_dates(soup):
                if self.start <= od <= self.end:
                    tickets.append(TicketInfo(
                        title=normalize_title(title.strip()),
                        open_datetime=od,
                        round_info=round_info,
                        performance_period=performance_period,
                        cast=cast,
                        detail_url=detail_url,
                        category=item['genre'].strip(),
                        open_type=label.strip(),
                        venue=venue,
                        providers={"멜론티켓"},
                        solo_sale=only_sale,
                        content=content,
                        source="멜론티켓",
                        regions=regions,
                    ))
        # “티켓오픈” 한 건만
        else:
            tickets.append(TicketInfo(
                title=normalize_title(title.strip()),
                open_datetime=item['open_date'],
                round_info=round_info,
                performance_period=performance_period,
                cast=cast,
                detail_url=detail_url,
                category=item['genre'].strip(),
                open_type="티켓오픈".strip(),
                venue=venue,
                providers={"멜론티켓"},
                solo_sale=only_sale,
                content=content,
                source="멜론티켓",
                regions=regions,
            ))

        return tickets

    def _parse_cast_info(self, soup: BeautifulSoup, default_cast: str) -> str:
        info_box = soup.select_one("div.box_concert_info")
        if not info_box:
            return "-"
        found = False
        lines: List[str] = []
        for span in info_box.select("span"):
            txt = span.get_text(strip=True)
            if not found and ("[캐스팅]" in txt or "라 인 업" in txt):
                found = True
                continue
            if found:
                if txt == "" or txt.startswith("[") or txt.startswith("※"):
                    break
                bold = span.find("b")
                if bold:
                    label = bold.get_text(strip=True)
                    rest = "".join(
                        sib.strip() if isinstance(sib, NavigableString)
                        else sib.get_text(strip=True)
                        for sib in bold.next_siblings
                    )
                    lines.append(f"{label} - {rest.strip()}")
                else:
                    lines.append(txt)
        cast = clean_cast_text("\n".join(lines))
        if cast != "-":
            return cast
        return clean_cast_text(default_cast)

    def _parse_base_box(self, soup: BeautifulSoup) -> Tuple[str, str, str]:
        base = soup.select_one("div.box_concert_time")
        round_info = "-"
        place = "-"
        performance_period = "-"
        if base:
            for tag in base.find_all(['span', 'p', 'div']):
                txt = tag.get_text(strip=True)
                if not txt:
                    continue
                if "오픈기간" in txt:
                    round_info = txt.split("오픈기간")[-1].strip(":：· ").strip()
                elif "공연 장소" in txt or "공연장소" in txt:
                    place = txt.split(":")[-1].strip()
                else:
                    performance_period = extract_performance_period(txt) or performance_period
        return round_info, place, performance_period

    @staticmethod
    def _extract_venue_from_content(content: Dict[str, str]) -> str:
        for text in content.values():
            for line in text.splitlines():
                match = re.search(r"-?\s*공\s*연\s*장\s*소\s*[:：]\s*(.+)", line)
                if match:
                    venue = match.group(1).strip(" \t\r\n-·ㆍ")
                    if venue:
                        return venue
                if re.search(r"(?:공\s*연\s*)?일\s*시", line) and "@" in line:
                    at_match = re.search(r"@\s*([^@\n\r|/]+)", line)
                    if at_match:
                        venue = at_match.group(1).strip(" \t\r\n-·ㆍ,，.。")
                        if venue:
                            return venue
        return "-"

    def _parse_open_dates(self, soup: BeautifulSoup) -> List[Tuple[str, datetime]]:
        results: List[Tuple[str, datetime]] = []
        for dt_tag, dd_tag in zip(
                soup.select("dt.tit_type"),
                soup.select("dd.txt_date")
        ):
            label = dt_tag.get_text(strip=True).rstrip(":")
            raw = dd_tag.get_text(strip=True).split(":", 1)[-1].strip()
            try:
                norm = normalize_date_string(raw)
                od = datetime.strptime(norm, "%Y년 %m월 %d일 %H:%M")
                results.append((label, od))
            except (ValueError, AttributeError) as e:
                logger.debug(f"오픈일정 날짜 파싱 실패: {e}")
                continue
        return results

    def _parse_content(self, soup: BeautifulSoup) -> Dict[str, str]:
        result: Dict[str, str] = {}
        wrap = soup.find('div', class_=self.cfg["detail_selectors"]["content_wrap"])
        if not wrap:
            return result

        # ✅ 기본정보: - 키 : 값 형식
        info_box = wrap.select_one('.box_concert_time .data_txt')
        if info_box:
            lines = []
            for p in info_box.find_all('p'):
                text = p.get_text(strip=True)
                if text and text.startswith("-"):
                    lines.append(text)
            if lines:
                result["기본정보"] = "\n".join(lines)

        # ✅ 공연소개: 공연소개 전체 텍스트 블럭
        intro_box = wrap.select_one('.box_concert_info .concert_info_txt')
        if intro_box:
            intro_text = intro_box.get_text(separator="\n", strip=True)
            if intro_text:
                result["공연소개"] = intro_text

        # ✅ 기획사 정보: 줄바꿈 포함 텍스트
        agency_box = wrap.select_one('.box_agency .txt')
        if agency_box:
            agency_text = agency_box.get_text(separator="\n", strip=True)
            if agency_text:
                result["기획사 정보"] = agency_text

        # 기본 출연진 블럭 (단일 구조 우선)
        cast_tag = wrap.select_one('.box_artist_checking .singer')
        if cast_tag:
            result["출연진"] = cast_tag.get_text(strip=True)

        # 복수 출연진이 존재하는 경우 (캐릭터 소개 이후)
        if "출연진" not in result and intro_box:
            found = False
            cast_lines = []
            for p in intro_box.find_all("p"):
                text = p.get_text(strip=True)
                if not text:
                    continue
                if "캐릭터 소개" in text:
                    found = True
                    continue
                if found:
                    cast_lines.append(text)
            if cast_lines:
                result["출연진"] = extract_cast_from_lines(cast_lines)

        return result
