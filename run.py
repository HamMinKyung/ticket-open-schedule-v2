import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')

import asyncio
import logging
import logging.config
import yaml
from typing import Tuple

from crawler.interpark import InterParkCrawler
from crawler.melon import MelonCrawler
from crawler.sac import SacCrawler
from crawler.sejongpac import SejongPac
from crawler.ticketlink import TicketLinkCrawler
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository
from datetime import datetime, timedelta
from collections import Counter

with open("logging.yaml", "r", encoding="utf-8") as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger(__name__)


def calc_date_range() -> Tuple[datetime, datetime]:
    today = datetime.now()

    # 당일 00:00
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 7일 뒤 23:59
    end = (start + timedelta(days=7)).replace(hour=23, minute=59, second=0, microsecond=0)

    return start, end


async def main():
    dr = calc_date_range()
    logger.info(f"크롤링 기간: {dr[0]} ~ {dr[1]}")
    crawlers = [
        InterParkCrawler(dr), MelonCrawler(dr), SejongPac(dr), SacCrawler(dr), TicketLinkCrawler(dr)
    ]

    # ✅ 예외가 발생해도 전체 실행 유지
    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_tickets = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"[CRAWLER ERROR] {type(result).__name__}: {result}")
            continue
        all_tickets.extend(result)

    logger.info(f"총 티켓 수: {len(all_tickets)}")

    merged = merge_ticket_sources(all_tickets)

    provider_list = [provider for ticket in merged for provider in ticket.providers]
    counter = Counter(provider_list)

    for provider, count in counter.items():
        logger.info(f"site {provider}: {count}")

    repo = NotionRepository()
    await repo.write_all(merged)


if __name__ == "__main__":
    asyncio.run(main())
