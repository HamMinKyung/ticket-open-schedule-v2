import asyncio
from datetime import datetime, timedelta
from typing import Tuple
import logging

from crawler.interpark import InterParkCrawler
from crawler.melon import MelonCrawler
from crawler.sejongpac import SejongPac
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository
from datetime import datetime, timedelta

def calc_date_range() -> Tuple[datetime, datetime]:

    today = datetime.now()

    # 기준: 차주 월요일 00:00
    # 실행일이 일요일이므로, 차주 월요일은 오늘 + 1일
    #start = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # 금요일 23:59
    #end = (start + timedelta(days=4)).replace(hour=23, minute=59, second=0, microsecond=0)

    # 당일 00:00
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 7일 뒤 23:59
    end = (start + timedelta(days=7)).replace(hour=23, minute=59, second=0, microsecond=0)

    print(f"실행 일자 {start} - {end}")
    return start, end


async def main():
    dr = calc_date_range()
    crawlers = [InterParkCrawler(dr), MelonCrawler(dr), SejongPac(dr)]
    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks)

    all_tickets = [t for sub in results for t in sub]
    merged = merge_ticket_sources(all_tickets)
    print("Merged tickets:", len(merged))

    repo = NotionRepository()
    await repo.write_all(merged)

    print("Crawling and writing finished.")

if __name__ == "__main__":
    asyncio.run(main())
