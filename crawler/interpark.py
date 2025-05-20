from typing import List, Dict, Any, Optional
from datetime import datetime

from bs4 import BeautifulSoup
from crawler.base import AsyncCrawlerBase
from utils.config import settings
from models.ticket import TicketInfo
from utils.utils import normalize_date_string
import logging
import re


class InterParkCrawler(AsyncCrawlerBase):
    headers = {"User-Agent": settings.USER_AGENT}
    cfg = settings.CRAWLERS["inter_park"]
    BASE_URL = cfg["base_url"]

    async def _fetch_list(self, session) -> List[Dict]:
        result: List[Dict] = []
        for region in self.cfg["regions"]:
            params = {**self.cfg["params"], "goodsRegion": region}
            async with session.get(
                    f"{self.BASE_URL}{self.cfg['list_endpoint']}",
                    params=params
            ) as res:
                res.raise_for_status()
                data = await res.json()
            # openDateStr 있고 self.start <=  <= self.end  항목만 필터링
            result.extend(
                filter(
                    lambda item: (
                        # openDateStr이 비어있지 않고
                            (d := item.get("openDateStr")) and
                            # openDateStr을 datetime으로 변환하고
                            (open_time := datetime.fromisoformat(d)) and
                            (self.start <= open_time <= self.end)
                    ), data
                )
            )
        logging.info("Crawling InterPark API ticket openings... result count: %d", len(result))
        return result

    def _parse_perf(self, perf_text: str, key: str) -> Optional[str]:
        """performance_info에서 key에 해당하는 값을 반환"""
        # for line in perf_text.split("\n"):
        #     if key in line:
        #         return line.split(":", 1)[1].strip()

        lines = perf_text.split("\n")

        for idx, line in enumerate(lines):
            normalized = line.strip("※•-* ").strip()

            if key in normalized:
                # 1) 같은 줄 안에 값이 있는 경우
                for sep in [":", "：", "·", "-", "~"]:
                    if sep in normalized:
                        parts = normalized.split(sep, 1)
                        if len(parts) > 1 and parts[1].strip():
                            return parts[1].strip()

                # 2) 다음 줄이 값인 경우
                if idx + 1 < len(lines):
                    next_line = lines[idx + 1].strip("※•-* ").strip()
                    if next_line:
                        return next_line

        return None

    async def _fetch_detail(self, session, item: Dict[str, Any]) -> List[TicketInfo]:
        cfg    = settings.CRAWLERS['inter_park']
        notice = item["noticeId"]
        url    = f"{cfg['base_url']}{cfg['detail_endpoint']}{notice}"
        html   = await (await session.get(url)).text()
        soup   = BeautifulSoup(html, "html.parser")

        # 상세 URL 결정
        detail_url = (
            f"{cfg['base_url']}/goods/{item.get('goodsCode')}"
            if item.get("goodsCode") else url
        )

        # 콘텐츠 수집
        content = {}
        for title_tag in soup.select(cfg["selectors"]["info_title"]):
            key = title_tag.get_text(strip=True)
            sibling = title_tag.find_next_sibling(
                cfg["sibling"]["name"],
                class_=cfg["sibling"]["class"]
            )
            content[key] = (
                sibling.get_text(separator="\n", strip=True)
                if sibling else ""
            )

        # 공연 정보 파싱
        perf_info  = content.get(cfg["contents"]["performance_info"], "")
        venue      = self._parse_perf(perf_info, cfg["contents"]["venue"]) or item.get("venueName", "")
        round_info = (
                self._parse_perf(perf_info, cfg["contents"]["open_period"])
                or self._parse_perf(perf_info, cfg["contents"]["period"])
                or "-"
        )
        cast       = re.split( r'\[?［?creative team|creative］?\]?', content.get(cfg["contents"]["cast"], "-"), maxsplit=1, flags=re.IGNORECASE)[0].strip()
        solo_sale  = item.get("goodsSeatTypeStr") == "단독판매"

        # 일정 추출 및 필터링
        schedules = []
        for box in soup.select(cfg["selectors"]["schedule_box"]):
            title = box.select_one(cfg["selectors"]["schedule_title"]).get_text(strip=True)
            raw   = normalize_date_string(box.select_one(cfg["selectors"]["schedule_date"]).get_text(strip=True))
            dt    = datetime.strptime(
                f"{settings.current_year}.{raw}",
                "%Y.%m.%d %H:%M"
            )

            if self.start <= dt <= self.end:
                schedules.append((title, dt))

        # 유효 일정이 없으면 빈 리스트 반환
        if not schedules:
            return []

        # 모든 유효 일정에 대해 TicketInfo 생성
        tickets: List[TicketInfo] = []
        for open_type, open_dt in schedules:
            tickets.append(TicketInfo(
                title         = item.get("title", "-").strip(),     # 공연 제목
                open_datetime = open_dt,                # 오픈 일시
                round_info    = round_info,             # 오픈 회차
                cast          = cast,                   # 출연진
                detail_url    = detail_url,                # 상세 링크
                category      = item.get("goodsGenreStr", "-").strip(), # 구분
                open_type     = open_type.strip(),                  # 오픈 타입
                venue         = venue.strip(),          # 공연 장소
                providers     = {"인터파크"},   # 예매처
                solo_sale     = solo_sale,      # 단독 판매
                content       = content,        # 내용
                source        = "인터파크" # 예매처(원본)
            ))

        return tickets