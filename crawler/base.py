import aiohttp
import asyncio
from datetime import datetime
from typing import List, Dict, Tuple
from abc import ABC, abstractmethod

from models.ticket import TicketInfo
from utils.config import settings


class AsyncCrawlerBase(ABC):
    headers: Dict[str, str] = {}
    timeout: aiohttp.ClientTimeout

    def __init__(self, date_range: Tuple[datetime, datetime]):
        self.timeout = aiohttp.ClientTimeout(total=settings.HTTP_TIMEOUT)
        self.start, self.end = date_range

    @abstractmethod
    async def _fetch_list(self, session: aiohttp.ClientSession) -> List[Dict]:
        pass

    @abstractmethod
    async def _fetch_detail(self, session: aiohttp.ClientSession, item: Dict) -> Dict:
        pass

    async def crawl(self) -> List[TicketInfo]:
        async with aiohttp.ClientSession(
            headers=self.headers,
            timeout=self.timeout
        ) as session:
            try:
                items = await self._safe_fetch_list(session)
            except Exception as e:
                print(f"[ERROR][{self.__class__.__name__}] List fetch critical error: {type(e).__name__} - {e}")
                return []

            detail_results = await asyncio.gather(
                *(self._safe_fetch_detail(session, item) for item in items),
                return_exceptions=False  # 각 fetch_detail에서 내부 처리
            )

        tickets: List[TicketInfo] = []
        for result in detail_results:
            if result:
                if isinstance(result, list):
                    tickets.extend(result)
                else:
                    tickets.append(result)
        return tickets

    async def _safe_fetch_list(self, session: aiohttp.ClientSession) -> List[Dict]:
        try:
            return await self._fetch_list(session)
        except Exception as e:
            print(f"[ERROR][{self.__class__.__name__}] _fetch_list 실패: {type(e).__name__} - {e}")
            return []

    async def _safe_fetch_detail(self, session: aiohttp.ClientSession, item: Dict) -> List[TicketInfo] | None:
        try:
            return await self._fetch_detail(session, item)
        except Exception as e:
            print(f"[ERROR][{self.__class__.__name__}] _fetch_detail 실패: {type(e).__name__} - {e}")
            return None
