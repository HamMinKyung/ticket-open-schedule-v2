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
    # start = today + timedelta(days=(7 - today.weekday()), hours=0, minutes=0)
    start = today + timedelta(days=(today.weekday()), hours=0, minutes=0)
    end   = start + timedelta(days=6, hours=23, minutes=59)
    logging.info("Date range for crawling: %s ~ %s", start, end)
    return start, end

async def main():
    dr         = calc_date_range()
    crawlers = [InterParkCrawler(dr)
        , MelonCrawler(dr)
                ]
    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks)

    all_tickets = [t for sub in results for t in sub]

    print(f"Total tickets collected: {len(all_tickets)}")
    merged = merge_ticket_sources(all_tickets)

    print(f"Merged tickets: {len(merged)}")


    repo = NotionRepository()
    repo.write_all(merged)

    logging.info("Crawling and writing finished.")

if __name__ == "__main__":
    asyncio.run(main())
