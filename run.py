import asyncio
from datetime import datetime, timedelta
from typing import Tuple
import logging

from crawler.interpark import InterParkCrawler
from crawler.melon import MelonCrawler
from crawler.sac import SacCrawler
from crawler.sejongpac import SejongPac
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository
from datetime import datetime, timedelta
from collections import Counter


def calc_date_range() -> Tuple[datetime, datetime]:
    today = datetime.now()

    # 기준: 차주 월요일 00:00
    # 실행일이 일요일이므로, 차주 월요일은 오늘 + 1일
    # start = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # 금요일 23:59
    # end = (start + timedelta(days=4)).replace(hour=23, minute=59, second=0, microsecond=0)

    # 당일 00:00
    start = today.replace(hour=0, minute=0, second=0, microsecond=0)

    # 7일 뒤 23:59
    end = (start + timedelta(days=7)).replace(hour=23, minute=59, second=0, microsecond=0)

    print(f"실행 일자 {start} - {end}")
    return start, end


async def main():
    dr = calc_date_range()
    crawlers = [InterParkCrawler(dr), MelonCrawler(dr), SejongPac(dr), SacCrawler(dr)]

    tasks = [crawler.crawl() for crawler in crawlers]
    results = await asyncio.gather(*tasks)

    all_tickets = [t for sub in results for t in sub]
    merged = merge_ticket_sources(all_tickets)

    # providers는 set이므로, 각 티켓의 provider를 하나씩 꺼내서 카운트
    provider_list = [provider for ticket in merged for provider in ticket.providers]
    counter = Counter(provider_list)

    for provider, count in counter.items():
        print(f"{provider}: {count}")

    # repo = NotionRepository()
    # await repo.write_all(merged)

    for ticket in merged:
        print(f"제목: {ticket.title}")
        print(f"오픈일시: {ticket.open_datetime}")
        print(f"회차: {ticket.round_info}")
        print(f"출연진: {ticket.cast}")
        print(f"상세링크: {ticket.detail_url}")
        print(f"구분: {ticket.category}")
        print(f"오픈타입: {ticket.open_type}")
        print(f"공연장소: {ticket.venue}")
        print(f"예매처: {', '.join(ticket.providers)}")
        print(f"단독판매: {ticket.solo_sale}")
        print(f"내용: {ticket.content}")
        print(f"원본구분: {ticket.source}")
        print("-" * 40)
    print("Crawling and writing finished.")


if __name__ == "__main__":
    asyncio.run(main())
