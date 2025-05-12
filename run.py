import asyncio
from datetime import datetime, timedelta
from typing import Tuple
import logging

from crawler.interpark import InterParkCrawler
from crawler.melon import MelonCrawler
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository


def calc_date_range() -> Tuple[datetime, datetime]:
    today = datetime.now()
    start = today + timedelta(days=(7 - today.weekday()), hours=0, minutes=0)
    end = start + timedelta(days=6, hours=23, minutes=59)
    print(f"실행 일자 {start} - {end}")
    return start, end


async def main():
    dr = calc_date_range()
    crawlers = [InterParkCrawler(dr), MelonCrawler(dr)]
    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks)

    all_tickets = [t for sub in results for t in sub]
    merged = merge_ticket_sources(all_tickets)

    repo = NotionRepository()
    await repo.write_all(merged)

    print("Crawling and writing finished.")

if __name__ == "__main__":
    asyncio.run(main())
