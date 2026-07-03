import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict
import logging

from utils.utils import extract_cast_from_lines, extract_open_round, normalize_date_string, normalize_performance_period, normalize_title
from models.ticket import TicketInfo
from crawler.base import AsyncCrawlerBase
from utils.config import settings
import re

logger = logging.getLogger(__name__)


class SacCrawler(AsyncCrawlerBase):
    def __init__(self, date_range):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['sac']
        self.base_url = self.cfg['base_url']
        self.list_url = f"{self.base_url}{self.cfg['list_endpoint']}"

    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict]:
        results = []
        page = 1

        while True:
            params = {**self.cfg["params"], "cp": page}
            async with session.get(self.list_url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("result") != 'success':
                    break;

                paging = data.get('paging', {})
                items = paging.get('result', [])

                # 필터링: TICKET_OPEN_DATE가 self.start와 self.end 사이에 있는 항목만
                items = [
                    item for item in items
                    if item.get("TICKET_OPEN_DATE") and self.start <= datetime.fromisoformat(
                        item["TICKET_OPEN_DATE"]) <= self.end
                ]

                results.extend(items)

                total_page = paging.get("totalPage", 1)
                if page >= total_page:
                    break

                page += 1
        return results

    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict) -> List[TicketInfo]:
        # SN, PLACE_NAME, PRICE_INFO
        url = f"{self.base_url}{self.cfg['detail_endpoint']}{item['SN']}"
        # SN 값을 URL에 추가
        async with session.get(url) as resp:
            resp.raise_for_status()
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            title_tag = soup.find("p", class_="title")
            top_box = soup.find("div", class_="cwa-top")
            info_list = top_box.find("ul") if top_box else None
            if not title_tag or not info_list:
                logger.debug(f"[SacCrawler] 필수 상세 영역 없음: SN={item.get('SN')}")
                return []

            title = title_tag.get_text(strip=True)
            info_tags = info_list.find_all("li")

            # Opening date
            round_info = ""
            venue = None
            contents  = {}
            for info_box in info_tags:
                info = info_box.find_all("span")
                key =  info[0].get_text(strip=True) if len(info) > 0 else ""
                value = info[1].get_text(strip=True) if len(info) > 1 else ""
                if key == "장소":
                    venue = value
                else :
                    contents[key] = value  # value가 없으면 빈 문자열이 들어감


            tab_box  = soup.find_all("div", class_="ctl-sub")
            if len(tab_box) < 4:
                logger.debug(f"[SacCrawler] 상세 탭 부족: SN={item.get('SN')}, tabs={len(tab_box)}")
                return []
            # tab_box = 0: 관람 연령, 1: 공지- 티켓오픈, 2: 작품소개 - 출연진, 3: 할인정보-기타
            schedules = self._parse_schedule(tab_box[1])
            if not schedules:
                logger.debug(f"[SacCrawler] 오픈 일정 없음: SN={item.get('SN')}")
                return []
            # 출연진
            p_tags = tab_box[2].find_all("p")
            lines = [p.get_text(strip=True) for p in p_tags if p.get_text(strip=True)]
            cast = extract_cast_from_lines(lines)

            contents["소개"] = "\n".join(lines)

            # 할인정보
            contents["할인정보"] = tab_box[3].get_text(separator="\n", strip=True)

            # 공연기간: 상세페이지 정보 목록의 "기간" 항목을 그대로 사용한다.
            performance_period = normalize_performance_period(contents.get("기간")) or "-"

            tickets: List[TicketInfo] = []
            for schedule in schedules:
                if not (self.start <= schedule["datetime"] <= self.end):
                    continue
                normalized_round_info = (
                    extract_open_round(schedule["type"], title, contents.get("소개", ""))
                    or "-"
                )
                # 티켓 정보 생성
                tickets.append(TicketInfo(
                    title=normalize_title(title),
                    open_datetime=schedule["datetime"],
                    round_info=normalized_round_info,
                    performance_period=performance_period,
                    cast=cast,
                    detail_url=url,
                    category="공연",
                    open_type=schedule["type"],
                    venue=venue or "-",
                    providers={'예술의전당'},
                    solo_sale=schedule["solo_sale"],
                    content=contents,
                    source="예술의전당",
                    regions="서울",
                ))

            return tickets


    def _extract_datetime_string(self, raw: str) -> datetime | None:
        raw = re.sub(r'\(.*?\)', '', raw)  # 괄호 제거
        raw = raw.replace('오전', 'AM').replace('오후', 'PM')
        raw = re.sub(r'\s+', ' ', raw.strip())

        match = re.search(r'(\d{1,2})월\s*(\d{1,2})일\s*(AM|PM)?\s*(\d{1,2})시', raw)
        if match:
            month, day, ampm, hour = match.groups()
            hour = int(hour)
            if ampm == "PM" and hour != 12:
                hour += 12
            elif ampm == "AM" and hour == 12:
                hour = 0
            return datetime(settings.current_year, int(month), int(day), hour, 0)
        return None


    def _parse_schedule(self, html) -> List[Dict[str, str]]:
        """일정 파싱"""
        schedule = []
        text = html.get_text(separator="\n")

        patterns = [
            ("선예매", re.search(r"(유료회원.*?|선예매).*?(\d{1,2}월\s*\d{1,2}일.*?시)", text)),
            ("일반예매", re.search(r"(일반회원.*?|일반예매).*?(\d{1,2}월\s*\d{1,2}일.*?시)", text))
        ]

        for label, match in patterns:
            if match:
                dt = self._extract_datetime_string(match.group(2))
                if dt is None:
                    continue
                schedule.append({
                    "type": label,
                    "solo_sale": label == "선예매",
                    "datetime": dt
                })
        return schedule
