from typing import List, Dict, Any, Optional
from datetime import datetime
import re
import json

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
    def _compact(text: str) -> str:
        return re.sub(r"\s+", "", text or "")

    def _find_next_sibling(self, tag, name: str, class_prefix: str):
        sibling = tag.find_next_sibling(name)
        while sibling:
            classes = sibling.get("class") or []
            if not class_prefix:
                return sibling
            if any(class_prefix in cls for cls in classes):
                return sibling
            sibling = sibling.find_next_sibling(name)
        return None

    def _extract_detail_sections(self, soup: BeautifulSoup) -> Dict[str, str]:
        content: Dict[str, str] = {}
        selectors = self.cfg["selectors"]
        for title_tag in soup.select(selectors["info_title"]):
            key = title_tag.get_text(strip=True)
            sibling = self._find_next_sibling(
                title_tag,
                self.cfg["sibling"]["name"],
                self.cfg["sibling"]["class"],
            )
            content[key] = (
                sibling.get_text(separator="\n", strip=True)
                if sibling else ""
            )

        if content:
            return content

        # NOL 상세페이지처럼 기존 클래스가 바뀐 경우, 전체 텍스트에서 주요 섹션을 재추출한다.
        page_text = soup.get_text("\n", strip=True)
        if not page_text:
            return content

        labels = (
            self.cfg["contents"]["performance_info"],
            self.cfg["contents"]["cast"],
        )
        for label in labels:
            block = self._extract_labeled_block(page_text, label, labels)
            if block:
                content[label] = block

        return content

    def _extract_labeled_block(self, text: str, label: str, boundary_labels: tuple[str, ...]) -> str:
        lines = [line.strip() for line in text.splitlines()]
        compact_label = self._compact(label)
        boundary_set = {self._compact(item) for item in boundary_labels if item}

        for idx, line in enumerate(lines):
            normalized = re.sub(r"^[※•-* \t]+", "", line).strip()
            compact_line = self._compact(normalized)
            if compact_label not in compact_line:
                continue

            block: List[str] = []
            inline = self._parse_perf(line, label)
            if inline:
                block.append(inline)

            for next_line in lines[idx + 1:]:
                normalized_next = re.sub(r"^[※•-* \t]+", "", next_line).strip()
                if not normalized_next:
                    if block:
                        break
                    continue
                compact_next = self._compact(normalized_next)
                if any(
                    boundary != compact_label and boundary in compact_next
                    for boundary in boundary_set
                ):
                    break
                block.append(normalized_next)

            result = "\n".join(block).strip()
            if result:
                return result

        return ""

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
            if (
                "공연기간" not in normalized
                and "공연 기간" not in normalized
                and "오픈기간" not in normalized
                and "오픈 기간" not in normalized
            ):
                continue

            # 3차 티켓오픈 공연기간: 2026년 8월 11일(화) ~ 8월 30일(일)
            # #3차 티켓 오픈 기간 : 2026년 7월 28일(화) - 8월 17일(월)
            match = re.search(
                r"티켓\s*오픈\s*(?:공연\s*)?기간\s*[:：]?\s*(.+)$",
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
        page_text = soup.get_text("\n", strip=True)
        content = self._extract_detail_sections(soup)
        if page_text:
            labels = (
                cfg["contents"]["performance_info"],
                cfg["contents"]["cast"],
            )
            for label in labels:
                if content.get(label):
                    continue
                block = self._extract_labeled_block(page_text, label, labels)
                if block:
                    content[label] = block

        # 공연 정보 파싱
        perf_info = content.get(cfg["contents"]["performance_info"], "")
        cast_info = content.get(cfg["contents"]["cast"], "")
        venue = (
            self._parse_perf(perf_info, cfg["contents"]["venue"])
            or self._parse_perf(page_text, cfg["contents"]["venue"])
            or item.get("venueName", "")
        )
        round_info = (
                extract_open_round(item.get("title", ""), perf_info, page_text)
                or self._extract_open_period(perf_info)
                or self._extract_open_period(page_text)
                or self._parse_perf(perf_info, cfg["contents"]["open_period"])
                or self._parse_perf(page_text, cfg["contents"]["open_period"])
                or self._parse_perf(perf_info, cfg["contents"]["open_period2"])
                or self._parse_perf(page_text, cfg["contents"]["open_period2"])
                or self._parse_perf(perf_info, cfg["contents"]["period"])
                or self._parse_perf(page_text, cfg["contents"]["period"])
                or self._parse_perf(perf_info, cfg["contents"]["period2"])
                or self._parse_perf(page_text, cfg["contents"]["period2"])
                or self._parse_perf(perf_info, cfg["contents"]["datetime"])
                or self._parse_perf(page_text, cfg["contents"]["datetime"])
                or "-"
        )
        cast = clean_cast_text(cast_info or "-")
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

        if not schedules:
            schedules = self._extract_ticket_dates_from_html(html)

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

    @staticmethod
    def _extract_ticket_dates_from_html(html: str) -> List[tuple[str, datetime]]:
        patterns = [
            r'"ticketDates"\s*:\s*(\[[\s\S]*?\])\s*,\s*"relatedNotices"',
            r'"ticketDates"\s*:\s*(\[[\s\S]*?\])\s*,\s*"recommendedNotices"',
            r'"ticketDates"\s*:\s*(\[[\s\S]*?\])',
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if not match:
                continue
            try:
                raw_items = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue

            entries: List[tuple[str, datetime]] = []
            for item in raw_items:
                open_name = (item.get("openName") or item.get("name") or "").strip()
                open_date_str = (item.get("openDateStr") or "").strip()
                if not open_date_str:
                    continue
                try:
                    dt = datetime.strptime(open_date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                if open_name:
                    entries.append((open_name, dt))
            if entries:
                return entries

        # ticketDates 구조가 아니라도 openName/openDateStr 조합이 그대로 박혀 있는 경우를 처리한다.
        fallback_patterns = [
            r'"openName"\s*:\s*"(?P<name>[^"]+)"\s*,\s*"openDateStr"\s*:\s*"(?P<date>[^"]+)"',
            r'"openDateStr"\s*:\s*"(?P<date>[^"]+)"\s*,\s*"openName"\s*:\s*"(?P<name>[^"]+)"',
        ]
        seen = set()
        for pattern in fallback_patterns:
            for match in re.finditer(pattern, html):
                open_name = (match.group("name") or "").strip()
                open_date_str = (match.group("date") or "").strip()
                if not open_name or not open_date_str:
                    continue
                try:
                    dt = datetime.strptime(open_date_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                key = (open_name, dt)
                if key in seen:
                    continue
                seen.add(key)
                entries.append(key)

        if entries:
            return entries
        return []
