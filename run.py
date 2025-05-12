import asyncio
from datetime import datetime, timedelta
from typing import Tuple
import logging

from crawler.interpark import InterParkCrawler
from crawler.melon import MelonCrawler
from crawler.sejongpac import SejongPac
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository


def calc_date_range() -> Tuple[datetime, datetime]:
    from datetime import datetime, timedelta

    today = datetime.now()

    # 기준: 차주 월요일 00:00
    days_until_next_monday = (7-today.weekday()) % 7 + 0
    # days_until_next_monday = (today.weekday()) % 7 + 0
    start = (today + timedelta(days=days_until_next_monday)).replace(hour=0, minute=0, second=0, microsecond=0)

    # 금요일 23:59
    end = start + timedelta(days=4, hours=23, minutes=59)

    print(f"실행 일자 {start} - {end}")
    return start, end


async def main():
    dr = calc_date_range()
    crawlers = [InterParkCrawler(dr), MelonCrawler(dr), SejongPac(dr)]
    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks)

    all_tickets = [t for sub in results for t in sub]
    merged = merge_ticket_sources(all_tickets)

    repo = NotionRepository()
    await repo.write_all(merged)

    print("Crawling and writing finished.")

if __name__ == "__main__":
    asyncio.run(main())
