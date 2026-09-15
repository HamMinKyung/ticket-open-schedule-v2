"""NOL 티켓의 오픈 예정 API를 사용하는 크롤러."""

import logging
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import quote

from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import (
    clean_cast_text,
    extract_open_round,
    extract_open_round_period,
    extract_performance_period,
    normalize_title,
    resolve_region,
)

logger = logging.getLogger(__name__)


class NolCrawler(AsyncCrawlerBase):
    cfg = settings.CRAWLERS["nol"]
    headers = {
        "User-Agent": settings.USER_AGENT,
        "Referer": f"{cfg['base_url']}/ticket/display/upcoming",
        "Accept": "application/json",
    }

    def _schedules(self, item: Dict[str, Any]) -> List[tuple[str, datetime]]:
        schedules = []
        seen = set()
        for entry in item.get("ticket_dates") or []:
            if not isinstance(entry, dict):
                continue
            raw = entry.get("ticket_open_date")
            if not isinstance(raw, str) or not raw.strip():
                continue
            try:
                dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
            except ValueError:
                logger.debug("[NolCrawler] 날짜 파싱 실패: %r", raw)
                continue
            # API의 오프셋 없는 날짜는 한국 현지 시각이다.
            if dt.tzinfo is not None:
                dt = dt.astimezone(settings.user_timezone).replace(tzinfo=None)
            if not self.start <= dt <= self.end:
                continue
            name = (
                entry.get("ticket_other_open_name")
                or entry.get("ticket_open_type_name")
                or item.get("open_type_name")
                or "티켓오픈"
            ).strip()
            key = (name, dt)
            if key not in seen:
                schedules.append(key)
                seen.add(key)
        return schedules

    async def _fetch_list(self, session) -> List[Dict[str, Any]]:
        result = []
        seen_ids = set()
        seen_cursors = set()
        cursor = None
        while True:
            payload = {"sort": "open"}
            if cursor:
                payload["cursor"] = cursor
            async with session.post(
                f"{self.cfg['base_url']}{self.cfg['list_endpoint']}",
                json=payload,
            ) as response:
                response.raise_for_status()
                data = await response.json()
            if not isinstance(data, dict) or not isinstance(data.get("notices"), list):
                raise ValueError("NOL 오픈 예정 API 응답에 notices 목록이 없습니다")
            notices = data["notices"]
            for item in notices:
                if not isinstance(item, dict) or not item.get("id"):
                    continue
                notice_id = item["id"]
                if notice_id not in seen_ids and self._schedules(item):
                    result.append(item)
                    seen_ids.add(notice_id)
            cursor = (data.get("summary") or {}).get("next_cursor")
            if not notices or not cursor:
                break
            if cursor in seen_cursors:
                raise ValueError("NOL 오픈 예정 API가 같은 페이지 커서를 반복합니다")
            seen_cursors.add(cursor)
        return result

    @staticmethod
    def _text(value: str | None) -> str:
        return BeautifulSoup(value or "", "html.parser").get_text("\n", strip=True)

    async def _fetch_detail(self, session, item: Dict[str, Any]) -> List[TicketInfo]:
        # 목록 API에 공지 본문과 모든 오픈 일정이 포함되어 별도 상세 요청은 필요 없다.
        schedules = self._schedules(item)
        goods_code = str(item.get("goods_code") or "").strip()
        if not schedules or not goods_code:
            return []
        title = (item.get("title") or item.get("goods_name") or "-").strip()
        venue = (item.get("venue_name") or "").strip()
        regions = resolve_region(
            venue, title, item.get("venue_address") or "",
            default_region=item.get("goods_region_name") or "",
        )
        if not regions:
            return []
        content = {
            "공연정보": self._text(item.get("goods_info")),
            "공연소개": self._text(item.get("goods_introduce")),
            "캐스팅": self._text(item.get("casting_info")),
        }
        perf_info = content["공연정보"]
        round_info = (
            extract_open_round_period(perf_info, content["공연소개"])
            or extract_open_round(item.get("open_name") or "", title, perf_info)
            or "-"
        )
        period = extract_performance_period(perf_info)
        if not period:
            start = item.get("goods_start_date")
            end = item.get("goods_end_date")
            if start and end:
                period = f"{start} ~ {end}" if start != end else start
        return [
            TicketInfo(
                title=normalize_title(title),
                open_datetime=dt,
                open_type=name,
                round_info=round_info,
                performance_period=period or "-",
                cast=clean_cast_text(content["캐스팅"] or "-"),
                detail_url=f"{self.cfg['base_url']}/ticket/products/{quote(goods_code, safe='')}",
                category=item.get("goods_genre_name") or "-",
                venue=venue,
                providers={"놀티켓"},
                source="놀티켓",
                solo_sale=item.get("goods_seat_type") == 1,
                content=content,
                regions=regions,
            )
            for name, dt in schedules
        ]
