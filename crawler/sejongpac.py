import asyncio
import random
from typing import Dict, Any, List

from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils import extract_cast_from_lines, extract_open_round, extract_performance_period, normalize_date_string, normalize_title
from utils.config import settings
from html import unescape
from datetime import datetime
import re

import logging

logger = logging.getLogger(__name__)


class SejongPac(AsyncCrawlerBase):
    def __init__(self, date_range):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['sejong_pac']
        self.list_url = self.cfg['list_endpoint']
        self.BASE_URL = self.cfg['base_url']

    def parse_td_with_paragraphs_or_list(self, td_tag):
        """<p> 또는 <li> 기준으로 줄바꿈 정리. 빈 줄 제거"""
        lines = []

        # <p> 태그
        for p in td_tag.find_all("p"):
            text = p.get_text(strip=True)
            if text:
                lines.append(text)

        # <li> 태그 (항상 추가. <p>와 병존 가능)
        for li in td_tag.find_all("li"):
            text = li.get_text(strip=True)
            if text:
                lines.append(text)

        # <br> 처리 (위 둘 모두 없을 때만)
        if not lines:
            brs = td_tag.find_all("br")
            if brs:
                raw = td_tag.get_text(separator="\n")
                for part in raw.split("\n"):
                    part = part.strip()
                    if part:
                        lines.append(part)

        # 아무 것도 없을 경우 fallback
        if not lines:
            text = td_tag.get_text(strip=True)
            if text:
                lines.append(text)

        return lines

    async def _fetch_list(self, session) -> List[Dict]:
        items = []
        for page in self.cfg['pages']:
            await asyncio.sleep(random.uniform(1.0, 2.0))
            payload = {**self.cfg["params"], "pageIndex": str(page)}

            async with session.get(self.list_url, params=payload) as response:
                response.raise_for_status()
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                rows = soup.select("div.tbl_list > table > tbody > tr")
                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) < 6:
                        continue

                    title_tag = cols[1].find("a")
                    if not title_tag or not title_tag.get("href"):
                        continue
                    title = unescape(title_tag.get_text(strip=True))
                    link = self.BASE_URL + title_tag["href"]

                    open_date = cols[3].get_text(strip=True)
                    try:
                        norm = normalize_date_string(open_date)
                        dt = datetime.strptime(norm, "%Y-%m-%d %H:%M")
                    except ValueError as e:
                        logger.debug(f"[SejongPac] 날짜 파싱 실패: {open_date!r} - {e}")
                        continue
                    if not (self.start <= dt <= self.end):
                        continue

                    items.append({
                        "title": title,
                        "link": link,
                        "open_date": dt,
                    })
        return items

    async def _fetch_detail(self, session, item: Dict[str, Any]) -> List[TicketInfo]:
        tickets: List[TicketInfo] = []

        content = {}
        async with session.get(item["link"]) as response:
            response.raise_for_status()
            detail_html = await response.text()
        soup = BeautifulSoup(detail_html, "html.parser")
        category = venue = cast = performance_period = None;
        open_type = "일반예매"
        title = item["title"]
        solo_sale = False

        # (1) content 채우기
        table = soup.find("table")
        if table:
            for row in table.select("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    key = th.get_text(strip=True)
                    value = "\n".join(self.parse_td_with_paragraphs_or_list(td))
                    content[key] = value

        # (5) 티켓오픈일
        open_section = soup.find('th', string='티켓오픈일')
        open_entries = []
        if open_section:
            open_td = open_section.find_next_sibling("td")
            if not open_td:
                return []
            open_lines = self.parse_td_with_paragraphs_or_list(open_td)
            for line in open_lines:
                # 먼저 날짜 문자열을 표준 포맷(YYYY년 MM월 DD일 HH:MM)으로 정규화
                nds = normalize_date_string(line)
                m = re.search(
                    r'(?:^|:)\s*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*(?:오전|오후)?\s*\d{1,2}(?:시|:\d{2})(?:\s*\d{1,2}분)?)',
                    nds
                )

                if not m:
                    continue

                date_str = m.group(1)

                if "오전" in date_str or "오후" in date_str:
                    # 🧠 오전/오후 있는 경우는 직접 파싱
                    d = re.search(
                        r'(?P<year>\d{4})년\s*(?P<month>\d{1,2})월\s*(?P<day>\d{1,2})일\s*'
                        r'(?P<ampm>오전|오후)?\s*(?P<hour>\d{1,2})(시|:)(\s*(?P<minute>\d{1,2})분?)?',
                        date_str
                    )
                    if d:
                        year = int(d.group("year"))
                        month = int(d.group("month"))
                        day = int(d.group("day"))
                        hour = int(d.group("hour"))
                        minute = int(d.group("minute") or 0)
                        ampm = d.group("ampm")

                        if ampm == "오후" and hour < 12:
                            hour += 12
                        if ampm == "오전" and hour == 12:
                            hour = 0

                        open_time = datetime(year, month, day, hour, minute)
                    else:
                        continue  # 예외 처리: 파싱 실패 시 skip
                else:
                    # ✅ 오전/오후 없는 경우는 그대로 파싱
                    open_time = datetime.strptime(date_str, "%Y년 %m월 %d일 %H:%M")

                # 날짜 뒤에 붙은 텍스트를 잘라내고, 없으면 "일반예매"로
                open_target = nds[m.end():].strip()

                if not open_target:
                    open_target = nds[:m.start()].strip().rstrip(":").replace("-", "")

                if not open_target:
                    open_target = "일반예매"

                # 결과를 리스트에 모아두기
                open_entries.append({
                    "time": open_time,
                    "target": open_target
                })

        # (2) 티켓오픈회차 추출
        # 이 사이트는 "티켓오픈회차" 값이 "1차/2차" 라벨이 아니라 해당 회차가 커버하는
        # 공연 날짜(오픈기간)로 표기되는 경우가 있어, round_info와 별도로 보관한다.
        round_raw = None
        round_section = soup.find('th', string='티켓오픈회차')
        if round_section:
            round_td = round_section.find_next_sibling("td")
            if round_td:
                round_raw = round_td.get_text(strip=True)
        round_label_from_raw = extract_open_round(round_raw) if round_raw else None

        # (3) 공연정보 항목 상세 파싱
        info_section = soup.find('th', string='공연정보')
        if info_section:
            info_td = info_section.find_next_sibling("td")
            if not info_td:
                info_lines = []
            else:
                info_lines = self.parse_td_with_paragraphs_or_list(info_td)

            for line in info_lines:
                if "공연명" in line:
                    title = normalize_title(line.split("공연명")[-1].strip(": ： ·").strip())
                    if "연극" in title:
                        category = "연극"
                    elif "뮤지컬" in title:
                        category = "뮤지컬"
                    elif "콘서트" in title:
                        category = "콘서트"
                    elif "클래식" in title:
                        category = "클래식"
                    else:
                        category = "기타"
                elif "공연장소" in line:
                    venue = line.split("공연장소")[-1].strip(": ： ·").strip()
                elif "공연기간" in line or "공연일시" in line:
                    value = extract_performance_period(line)
                    if not value:
                        parts = re.split(r"[:：]", line, maxsplit=1)
                        value = parts[1].strip() if len(parts) > 1 else line.strip()
                    performance_period = value
                elif "선예매" in line:
                    open_type = "선예매"
                if "세종문화티켓에서만" in line:
                    solo_sale = True

        # (4) 출연진
        intro_section = soup.find('th', string='공연소개')
        if intro_section:
            intro_td = intro_section.find_next_sibling("td")
            if intro_td:
                cast = self.extract_cast_from_td(intro_td)

        # (6) 티켓정보 생성
        for open_item in open_entries:
            open_dt = open_item["time"]
            if not (self.start <= open_dt <= self.end):
                continue

            tickets.append(TicketInfo(
                title=title,  # 공연 제목
                open_datetime=open_dt,  # 오픈 일시
                round_info=extract_open_round(open_item["target"], title) or round_label_from_raw or "-",  # 오픈 회차
                performance_period=(
                    performance_period
                    or extract_performance_period(*content.values())
                    or (round_raw if round_raw and not round_label_from_raw else None)
                    or "-"
                ),  # 공연 기간
                cast=cast or "-",  # 출연진
                detail_url=item["link"],  # 상세 링크
                category=category or "-",  # 구분
                open_type=open_item["target"],  # 오픈 타입
                venue=venue or "-",  # 공연 장소
                providers={"세종문화회관"},  # 예매처
                solo_sale=solo_sale,  # 단독 판매
                content=content,  # 내용
                source="세종문화회관",  # 예매처(원본)
                regions= "서울"  # 지역 (세종문화회관은 서울)
            ))

        return tickets


    def extract_cast_from_td(self, td_tag):
        # 1) <p> 태그별로 텍스트를 한 줄씩 뽑아서 리스트로
        lines = self.parse_td_with_paragraphs_or_list(td_tag)

        return extract_cast_from_lines(lines)
