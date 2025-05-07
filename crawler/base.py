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
            items = await self._fetch_list(session)
            # _fetch_detail이 List[TI] 또는 None 반환
            detail_results = await asyncio.gather(
                *(self._fetch_detail(session, item) for item in items)
            )

        # Flatten & filter out None/empty
        tickets: List[TicketInfo] = []
        for result in detail_results:
            if not result:
                continue
            # result이 list라면 extend, 단일 객체라면 append
            if isinstance(result, list):
                tickets.extend(result)
            else:
                tickets.append(result)

        return tickets