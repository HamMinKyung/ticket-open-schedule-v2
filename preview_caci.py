"""Read page one without a date limit; export a Notion preview without API writes."""
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

# The preview does not access Notion databases or credentials.
os.environ.setdefault("NOTION_TITLE_DB_ID", "preview-only")

import aiohttp
from crawler.caci import CaciCrawler
from merge.merge import merge_ticket_sources
from notion_writer.writer import NotionRepository


async def main():
    crawler = CaciCrawler((datetime.min, datetime.max))
    async with aiohttp.ClientSession(headers=crawler.headers, timeout=crawler.timeout) as session:
        items = await crawler._fetch_list(session)
        tickets = []
        for item in items:
            tickets.extend(await crawler._fetch_detail(session, item))
    tickets = merge_ticket_sources(tickets)
    # Build payloads without initializing the API client or fetching relation maps.
    repo = object.__new__(NotionRepository)
    repo.actor_name_map = {}
    repo.title_name_map = {}
    result = {
        "notice_count": len(items),
        "ticket_count": len(tickets),
        "scope": "page 1 only, no date limit, no Notion writes",
        "notes": "Relations and calendar links are not resolved in this preview.",
        "tickets": [
            {
                "data": ticket.model_dump(mode="json", by_alias=True),
                "properties": repo._build_properties(ticket),
                "children": repo._build_contents(ticket.content, ""),
            }
            for ticket in tickets
        ],
    }
    output = Path("caci_notion_preview.json")
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{len(items)} notices, {len(tickets)} tickets: {output.resolve()}")
    for ticket in tickets:
        print(ticket.title, ticket.open_type, ticket.open_datetime, ticket.round_info)


if __name__ == "__main__":
    asyncio.run(main())
