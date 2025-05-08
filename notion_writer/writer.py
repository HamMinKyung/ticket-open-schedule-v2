# notion_db_writer.py
import asyncio
import logging
from typing import Optional, List
from notion_client import Client
from utils.config import settings
from models.ticket import TicketInfo


class NotionRepository:
    """
    Notion API를 통한 데이터베이스 CRUD를 담당합니다.
    """

    def __init__(
            self,
            client: Optional[Client] = None,
            database_id: Optional[str] = None
    ):
        self.client = client or Client(auth=settings.NOTION_TOKEN) # log_level=logging.DEBUG
        self.database_id = database_id or settings.NOTION_DB_ID

    def _find_page(self, ticket: TicketInfo) -> Optional[dict]:
        """
        동일 제목 및 오픈일시의 페이지가 이미 존재하는지 조회합니다.
        """
        local_dt = ticket.open_datetime.astimezone(settings.user_timezone).replace(tzinfo=settings.DEFAULT_TIMEZONE)
        iso_date = local_dt.isoformat(timespec="seconds")
        response = self.client.databases.query(
            database_id=self.database_id,
            filter={
                "and": [
                    {"property": "공연 제목", "title": {"equals": ticket.title}},
                    {"property": "오픈 일시", "date": {"equals": iso_date}},
                ]
            }
        )
        results = response.get("results", [])
        if not results:
            print(f"❌ 페이지 없음: {ticket.title} (오픈일시={ticket.open_datetime})")
        else:
            print(f"✅ 페이지 존재: {ticket.title} (page_id={results[0]['id']})")
        return results[0] if results else None

    def _build_properties(self, ticket: TicketInfo) -> dict:
        """
        TicketInfo 모델을 Notion 페이지 속성(JSON)으로 변환합니다.
        """
        local_dt = ticket.open_datetime.astimezone(settings.user_timezone).replace(tzinfo=settings.DEFAULT_TIMEZONE)
        iso_date = local_dt.isoformat(timespec="seconds")
        return {
            "공연 제목": {
                "title": [{"type": "text", "text": {"content": ticket.title}}]
            },
            "구분": {
                "rich_text": [{"type": "text", "text": {"content": ticket.category}}]
            },
            "오픈 일시": {
                "date": {"start": iso_date}
            },
            "오픈 회차": {
                "rich_text": [{"type": "text", "text": {"content": ticket.round_info}}]
            },
            "오픈 타입": {"select": {"name": ticket.open_type}},
            "공연 장소": {
                "rich_text": [{"type": "text", "text": {"content": ticket.venue}}]
            },
            "상세 링크": {"url": ticket.detail_url},
            "출연진": {
                "rich_text": [{"type": "text", "text": {"content": ticket.cast}}]
            },
            "예매처": {
                "multi_select": [{"name": name} for name in ticket.providers]
            },
            "단독 판매": {"checkbox": ticket.solo_sale},
        }

    def _build_contents(self, content: dict) -> list[dict]:
        """
        TicketInfo.content 딕셔너리를 Notion 블록 리스트로 변환합니다.
        긴 텍스트(value)는 2000자씩 잘라 여러 paragraph 블록으로 분할 삽입합니다.
        """

        def chunk_text(text: str, size: int = 2000) -> list[str]:
            return [text[i: i + size] for i in range(0, len(text), size)]

        children: list[dict] = []
        for key, value in content.items():
            # 섹션 헤딩
            children.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": key}}]
                }
            })
            # 본문(2000자 단위로 분할)
            for chunk in chunk_text(value):
                children.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": chunk}}],
                        "color": "default"
                    }
                })
        return children

    def upsert_ticket(self, ticket: TicketInfo) -> None:
        try:
            existing = self._find_page(ticket)
            props = self._build_properties(ticket)
            contents = self._build_contents(ticket.content)

            if existing:
                page_id = existing["id"]
                # 1) 속성 업데이트
                self.client.pages.update(page_id=page_id, properties=props)

                # 2) 기존 블록 전부 삭제
                cursor = None
                while True:
                    if cursor:
                        resp = self.client.blocks.children.list(
                            block_id=page_id,
                            start_cursor=cursor,
                            page_size=100
                        )
                    else:
                        resp = self.client.blocks.children.list(
                            block_id=page_id,
                            page_size=100
                        )

                    for block in resp.get("results", []):
                        self.client.blocks.delete(block_id=block["id"])
                    if not resp.get("has_more"):
                        break
                    cursor = resp.get("next_cursor")

                # 3) 새 블록 추가
                self.client.blocks.children.append(
                    block_id=page_id,
                    children=contents
                )
                print(f"🔁 업데이트 및 블록 교체 완료: {ticket.title} (page_id={page_id})")

            else:
                # 생성 시 children 옵션으로 한 번에 삽입
                created = self.client.pages.create(
                    parent={"database_id": self.database_id},
                    properties=props,
                    children=contents
                )
                page_id = created["id"]
                print(f"🆕 생성 및 블록 삽입 완료: {ticket.title} (page_id={page_id})")

        except Exception as ex:
            print(f"❌ Notion 처리 실패: {ticket.title}", ex)

    # def write_all(self, tickets: List[TicketInfo]) -> None:
    #     """
    #     다수의 티켓 정보를 순차적으로 처리합니다.
    #     """
    #     for ticket in tickets:
    #         self.upsert_ticket(ticket)

    async def write_all(self, tickets: List[TicketInfo]) -> None:
        task= [
            asyncio.to_thread(self.upsert_ticket, ticket)
            for ticket in tickets
        ]

        result = await asyncio.gather(*task, return_exceptions=True)
        for ticket, result in zip(tickets, result):
            if isinstance(result, Exception):
                logging.error(f"❌ 티켓 처리 실패: {ticket.title}", exc_info=result)

