from typing import List, Dict, Any, Optional
from datetime import datetime
import re

from bs4 import BeautifulSoup
from crawler.base import AsyncCrawlerBase
from utils.config import settings
from models.ticket import TicketInfo
from utils.utils import clean_cast_text, extract_open_round, normalize_date_string, normalize_title, resolve_region
import logging

logger = logging.getLogger(__name__)


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
                map(
                    lambda item: {**item, "region": region},
                    filter(
                        lambda item: (
                                (d := item.get("openDateStr")) and
                                (open_time := datetime.strptime(d, "%Y-%m-%d %H:%M:%S")) and
                                (self.start <= open_time <= self.end)
                        ),
                        data,
                    ),
                )
            )
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

    @staticmethod
    def _extract_open_period(perf_text: str) -> Optional[str]:
        if not perf_text:
            return None

        lines = [line.strip("※•-* \t\r") for line in perf_text.splitlines()]
        for idx, line in enumerate(lines):
            if not line:
                continue
            normalized = re.sub(r"\s+", " ", line)
            if "티켓오픈" not in normalized and "티켓 오픈" not in normalized:
                continue
            if "공연기간" not in normalized and "공연 기간" not in normalized:
                continue

            # 3차 티켓오픈 공연기간: 2026년 8월 11일(화) ~ 8월 30일(일)
            match = re.search(
                r"티켓\s*오픈\s*공연\s*기간\s*[:：]?\s*(.+)$",
                normalized,
                flags=re.I,
            )
            if match:
                return re.sub(r"\s*공연\s*$", "", match.group(1).strip())

            # 2차 티켓오픈 공연 기간 / 8월 25일(화) ~ 9월 17일(목) 공연
            if idx + 1 < len(lines):
                next_line = re.sub(r"\s+", " ", lines[idx + 1]).strip()
                if next_line:
                    return re.sub(r"\s*공연\s*$", "", next_line)

        return None

    async def _fetch_detail(self, session, item: Dict[str, Any]) -> List[TicketInfo]:
        cfg = settings.CRAWLERS['inter_park']
        notice = item["noticeId"]
        url = f"{cfg['base_url']}{cfg['detail_endpoint']}{notice}"
        async with session.get(url) as resp:
            resp.raise_for_status()
            html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")

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
        perf_info = content.get(cfg["contents"]["performance_info"], "")
        venue = self._parse_perf(perf_info, cfg["contents"]["venue"]) or item.get("venueName", "")
        round_info = (
                self._extract_open_period(perf_info)
                or self._parse_perf(perf_info, cfg["contents"]["open_period"])
                or self._parse_perf(perf_info, cfg["contents"]["open_period2"])
                or self._parse_perf(perf_info, cfg["contents"]["period"])
                or self._parse_perf(perf_info, cfg["contents"]["period2"])
                or self._parse_perf(perf_info, cfg["contents"]["datetime"])
                or extract_open_round(item.get("title", ""), perf_info)
                or "-"
        )
        cast = clean_cast_text(content.get(cfg["contents"]["cast"], "-"))
        solo_sale = item.get("goodsSeatTypeStr") == "단독판매"

        # 일정 추출 및 필터링
        schedules = []
        for box in soup.select(cfg["selectors"]["schedule_box"]):
            title_tag = box.select_one(cfg["selectors"]["schedule_title"])
            date_tag = box.select_one(cfg["selectors"]["schedule_date"])
            if not title_tag or not date_tag:
                logger.debug(f"[InterParkCrawler] 일정 selector 누락: notice={notice}")
                continue
            title = title_tag.get_text(strip=True)
            raw = normalize_date_string(date_tag.get_text(strip=True))
            try:
                dt = datetime.strptime(
                    f"{settings.current_year}.{raw}",
                    "%Y.%m.%d %H:%M"
                )
            except ValueError as e:
                logger.debug(f"[InterParkCrawler] 일정 날짜 파싱 실패: {raw!r} - {e}")
                continue

            if self.start <= dt <= self.end:
                schedules.append((title, dt))

        # 유효 일정이 없으면 빈 리스트 반환
        if not schedules:
            return []

        # 지역 설정
        CONVERT_REGIONS = {"SEOUL": "서울", "GYEONGGI": "경기", "BUSAN": "부산", "ULSAN": "울산"}
        fallback_region = CONVERT_REGIONS.get(item.get("region", "SEOUL"), "서울")
        regions = resolve_region(venue, item.get("title", ""), default_region=fallback_region)
        if not regions:
            logger.debug(f"[InterParkCrawler] 지역 필터 제외: title={item.get('title')!r}, venue={venue!r}")
            return []
        
        # 모든 유효 일정에 대해 TicketInfo 생성
        tickets: List[TicketInfo] = []
        for open_type, open_dt in schedules:
            tickets.append(TicketInfo(
                title=normalize_title(item.get("title", "-").strip()),  # 공연 제목
                open_datetime=open_dt,  # 오픈 일시
                round_info=round_info,  # 오픈 회차
                cast=cast,  # 출연진
                detail_url=detail_url,  # 상세 링크
                category=item.get("goodsGenreStr", "-").strip(),  # 구분
                open_type=open_type.strip(),  # 오픈 타입
                venue=venue.strip(),  # 공연 장소
                providers={"놀티켓"},  # 예매처
                solo_sale=solo_sale,  # 단독 판매
                content=content,  # 내용
                source="놀티켓",  # 예매처(원본)
                regions= regions,  # 지역
            ))

        return tickets
