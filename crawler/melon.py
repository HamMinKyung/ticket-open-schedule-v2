# crawler/melon.py
import logging

import aiohttp
from bs4 import BeautifulSoup, NavigableString, Tag
from datetime import datetime
from typing import List, Dict, Any, Tuple
from crawler.base import AsyncCrawlerBase
from utils.config import settings
from models.ticket import TicketInfo
from utils.utils import normalize_date_string
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
                headers = self._get_headers()
                async with session.post(self.list_url, headers=headers, data=payload) as resp:
                    resp.raise_for_status()
                    html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')

                for li in soup.select("ul.list_ticket_cont li"):
                    title_tag = li.select_one("a.tit")
                    if not title_tag:
                        continue
                    raw_date    = li.select_one("span.date").get_text(strip=True)
                    pass_check  = "오픈일정 보기" in raw_date
                    open_date   = None

                    # 날짜 문구이면서 범위 내 항목만 추가
                    if not pass_check:
                        try:
                            norm    = normalize_date_string(raw_date)
                            dt      = datetime.strptime(norm, "%Y.%m.%d %H:%M")
                            if not (self.start <= dt <= self.end):
                                continue
                            open_date = dt
                        except:
                            continue

                    items.append({
                        "title_tag":      title_tag,
                        "pass_date_check": pass_check,
                        "open_date":      open_date,
                        "genre":          genre_name
                    })
            # 필터링된 항목만 반환
        logging.info(f"🔍 멜론티켓 공연 수집 완료: {len(items)}건")
        return items

    async def _fetch_detail(
            self,
            session: aiohttp.ClientSession,
            item: Dict[str, Any]
    ) -> List[TicketInfo]:
        cfg = self.cfg
        # 상세 페이지 URL
        href       = item['title_tag']['href'].lstrip("./")
        detail_url = f"{cfg['base_url']}/csoon/{href}"

        headers = self._get_headers()
        async with session.get(detail_url, headers=headers) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, 'html.parser')

        # 기본 정보 파싱
        title       = soup.select_one("p.tit_consert").get_text(strip=True)
        round_info, venue = self._parse_base_box(soup)
        cast        = self._parse_cast_info(soup)
        only_sale   = bool(soup.select_one(cfg['detail_selectors']['solo_icon']))
        content     = self._parse_content(soup)

        tickets: List[TicketInfo] = []

        # “오픈일정 보기”인 경우, 상세 여러 일정 파싱
        if item['pass_date_check']:
            for label, od in self._parse_open_dates(soup):
                if self.start <= od <= self.end:
                    tickets.append(TicketInfo(
                        title          = title,
                        open_datetime  = od.replace(tzinfo=settings.user_timezone),
                        round_info     = round_info,
                        cast           = cast,
                        detail_url     = detail_url,
                        category       = item['genre'],
                        open_type      = label,
                        venue          = venue,
                        providers      = {"멜론티켓"},
                        solo_sale      = only_sale,
                        content        = content,
                        source         = "멜론티켓"
                    ))
        # “티켓오픈” 한 건만
        else:
            tickets.append(TicketInfo(
                title          = title,
                open_datetime  = item['open_date'].replace(tzinfo=settings.user_timezone),
                round_info     = round_info,
                cast           = cast,
                detail_url     = detail_url,
                category       = item['genre'],
                open_type      = "티켓오픈",
                venue          = venue,
                providers      = {"멜론티켓"},
                solo_sale      = only_sale,
                content        = content,
                source         = "멜론티켓"
            ))

        return tickets

    def _parse_cast_info(self, soup: BeautifulSoup) -> str:
        info_box = soup.select_one("div.box_concert_info")
        if not info_box:
            return "-"
        found = False
        lines: List[str] = []
        for span in info_box.select("span"):
            txt = span.get_text(strip=True)
            if not found and "[캐스팅]" in txt:
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
        return ", ".join(lines) if lines else "-"

    def _parse_base_box(self, soup: BeautifulSoup) -> Tuple[str, str]:
        base = soup.select_one("div.box_concert_time")
        round_info = "-"
        place      = "-"
        if base:
            for tag in base.find_all(['span','p','div']):
                txt = tag.get_text(strip=True)
                if not txt:
                    continue
                if "오픈기간" in txt:
                    round_info = txt.split("오픈기간")[-1].strip(":：· ").strip()
                elif "공연 장소" in txt or "공연장소" in txt:
                    place = txt.split(":")[-1].strip()
        return round_info, place

    def _parse_open_dates(self, soup: BeautifulSoup) -> List[Tuple[str, datetime]]:
        results: List[Tuple[str, datetime]] = []
        for dt_tag, dd_tag in zip(
                soup.select("dt.tit_type"),
                soup.select("dd.txt_date")
        ):
            label = dt_tag.get_text(strip=True).rstrip(":")
            raw   = dd_tag.get_text(strip=True).split(":",1)[-1].strip()
            try:
                norm = normalize_date_string(raw)
                od   = datetime.strptime(norm, "%Y년 %m월 %d일  %H:%M")
                results.append((label, od))
            except:
                continue
        return results

    def _parse_content(self, soup: BeautifulSoup) -> Dict[str,str]:
        result: Dict[str,str] = {}
        wrap = soup.find('div', class_='wrap_detailview_cont')
        if not wrap:
            return result
        current = None
        for elem in wrap.find_all(['p','div'], recursive=True):
            if elem.name=='p' and 'tit_sub_float' in elem.get('class',[]):
                current = elem.get_text(strip=True)
                result[current] = ""
            elif current:
                txt = elem.get_text(separator="\n", strip=True)
                if txt:
                    result[current] = (
                        result[current] + "\n" + txt
                        if result[current] else txt
                    )
        return result
