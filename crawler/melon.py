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
from utils.notice import MAX_PAGES, PageGuard, extract_venue, OpeningWindow


class MelonCrawler(AsyncCrawlerBase):
    detail_concurrency = 1

    def __init__(self, date_range: Tuple[datetime, datetime]):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['melon']
        self.list_url = self.cfg['list_endpoint']
        self.locked = False
        self.headers = {
            'Referer': self.cfg['Referer'],
            'User-Agent': self.cfg['user_agents'][0],
            'Accept-Language': 'ko-KR,ko;q=0.9',
        }

    def _get_headers(self) -> Dict[str, str]:
        return dict(self.headers)

    def _is_locked(self, resp, context):
        if resp.status != 423:
            return False
        self.locked = True
        logger.warning('[MelonCrawler] 423 Locked - 현재 실행의 추가 요청 중단: %s', context)
        return True

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        self.locked = False
        items: List[Dict[str, Any]] = []
        # 같은 세션으로 진입 페이지의 쿠키를 받은 뒤 AJAX 목록을 요청한다.
        async with session.get(self.cfg['Referer'], headers=self._get_headers()) as resp:
            if self._is_locked(resp, '진입 페이지'):
                return []
            resp.raise_for_status()
            await resp.text()
        # 장르 코드별·페이지별 리스트 수집
        seen_notices = set()
        for code, genre_name in self.cfg['genre_map'].items():
            guard = PageGuard(f'MelonCrawler/{genre_name}')
            window = OpeningWindow(self.start, self.end)
            for page in range(1, MAX_PAGES + 1):
                payload = {
                    "schGcode": code,
                    "orderType": "2",
                    # Melon goPage uses a 1-based row offset: 1, 11, 21, ...
                    "pageIndex": str((page - 1) * 10 + 1)
                }
                await asyncio.sleep(random.uniform(1.0, 3.0))
                headers = self._get_headers()
                headers["X-Requested-With"] = "XMLHttpRequest"
                async with session.post(self.list_url, headers=headers, data=payload) as resp:
                    if self._is_locked(resp, f'genre={genre_name}, page={page}'):
                        return []
                    resp.raise_for_status()
                    html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                rows = soup.select("ul.list_ticket_cont li")
                if not guard.accept(a.get('href') for row in rows for a in row.select('a.tit[href]')):
                    break
                page_dates = []
                for li in rows:
                    title_tag = li.select_one("a.tit")
                    date_tag = li.select_one("span.date")
                    if not title_tag or not date_tag:
                        page_dates.append(None)
                        continue
                    notice_url = title_tag.get('href')
                    if not notice_url or notice_url in seen_notices:
                        page_dates.append(None)
                        continue
                    seen_notices.add(notice_url)
                    raw_date = date_tag.get_text(strip=True)
                    pass_check = "오픈일정 보기" in raw_date
                    open_date = None
                    detail_html = None

                    # 날짜 문구이면서 범위 내 항목만 추가
                    if not pass_check:
                        try:
                            norm = normalize_date_string(raw_date)
                            dt = datetime.strptime(norm, "%Y.%m.%d %H:%M")
                            page_dates.append(dt)
                            if dt < self.start:
                                continue
                            open_date = dt
                        except (ValueError, AttributeError) as e:
                            page_dates.append(None)
                            logger.debug(f"날짜 파싱 실패: {raw_date!r} - {e}")
                            continue

                    if pass_check:
                        detail_url = f"{self.cfg['base_url']}/csoon/{notice_url.lstrip('./')}"
                        await asyncio.sleep(random.uniform(1.0, 3.0))
                        async with session.get(detail_url, headers=self._get_headers()) as resp:
                            if self._is_locked(resp, f'detail={detail_url}'):
                                return []
                            resp.raise_for_status()
                            detail_html = await resp.text()
                        openings = self._parse_open_dates(BeautifulSoup(detail_html, 'html.parser'))
                        page_dates.append(min((dt for _, dt in openings), default=None))
                        if openings and not any(self.start <= dt <= self.end for _, dt in openings):
                            continue
                    items.append({
                        "detail_html": detail_html,
                        "title_tag": title_tag,
                        "pass_date_check": pass_check,
                        "open_date": open_date,
                        "genre": genre_name
                    })
                if window.beyond(page_dates):
                    break
            else:
                guard.limit()
        return items

    async def _fetch_detail(
            self,
            session: aiohttp.ClientSession,
            item: Dict[str, Any]
    ) -> List[TicketInfo]:
        if self.locked:
            return []
        cfg = self.cfg
        # 상세 페이지 URL
        href = item['title_tag']['href'].lstrip("./")
        detail_url = f"{cfg['base_url']}/csoon/{href}"

        html = item.get('detail_html')
        if html is None:
            headers = self._get_headers()
            await asyncio.sleep(random.uniform(1.0, 3.0))
            async with session.get(detail_url, headers=headers) as resp:
                if self._is_locked(resp, f'detail={detail_url}'):
                    return []
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
        # "오픈기간/오픈 회차" 원문에 "N차 티켓오픈" 패턴이 없으면(날짜만 있는 경우 등)
        # 값을 버리지 않고 원문 그대로 보존한다.
        round_info = extract_open_round(title, round_info) or round_info or "-"
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

        # 상세 일정이 있으면 목록의 단일 날짜보다 우선한다.
        schedules = self._parse_open_dates(soup)
        if not schedules and item.get('open_date'):
            schedules = [('티켓오픈', item['open_date'])]
        for label, od in schedules:
            if self.start <= od <= self.end:
                tickets.append(TicketInfo(
                    title=normalize_title(title.strip()), open_datetime=od,
                    round_info=round_info, performance_period=performance_period,
                    cast=cast, detail_url=detail_url, category=item['genre'].strip(),
                    open_type=label.strip(), venue=venue, providers={'멜론티켓'},
                    solo_sale=only_sale, content=content, source='멜론티켓', regions=regions,
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
            lines = [tag.get_text(strip=True) for tag in base.find_all(['span', 'p', 'div'])]
            lines = [line for line in lines if line]
            for idx, txt in enumerate(lines):
                round_label = "오픈기간" if "오픈기간" in txt else ("오픈 회차" if "오픈 회차" in txt else None)
                if round_label:
                    value = txt.split(round_label, 1)[-1].strip(":：· ").strip()
                    if not value and idx + 1 < len(lines):
                        value = lines[idx + 1].strip(":：· ").strip()
                    if value:
                        round_info = value
                elif extract_venue(txt):
                    place = extract_venue(txt)
                else:
                    performance_period = extract_performance_period(txt) or performance_period
        if base:
            place = extract_venue(base.get_text('\n', strip=True)) or place
        return round_info, place, performance_period

    @staticmethod
    def _extract_venue_from_content(content: Dict[str, str]) -> str:
        for text in content.values():
            for line in text.splitlines():
                venue = extract_venue(line)
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
        for dt_tag in soup.select("dl.schedule_info dt.tit_type"):
            dd_tag = dt_tag.find_next_sibling()
            if dd_tag is None or dd_tag.name != 'dd' or 'txt_date' not in dd_tag.get('class', []):
                continue
            label = dt_tag.get_text(strip=True).rstrip(":")
            raw = dd_tag.get_text(" ", strip=True).lstrip(":： ")
            try:
                norm = normalize_date_string(raw)
                od = datetime.strptime(norm, "%Y년 %m월 %d일 %H:%M")
                if (label, od) not in results:
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
