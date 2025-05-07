# notion_db_writer.py

import logging
from typing import Optional, List
from notion_client import Client
from utils.config import settings
from models.ticket import TicketInfo

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

class NotionRepository:
    """
    Notion API를 통한 데이터베이스 CRUD를 담당합니다.
    """
    def __init__(
            self,
            client: Optional[Client] = None,
            database_id: Optional[str] = None
    ):
        self.client = client or Client(auth=settings.NOTION_TOKEN)
        self.database_id = database_id or settings.NOTION_DB_ID

    def _find_page(self, ticket: TicketInfo) -> Optional[dict]:
        """
        동일 제목 및 오픈일시의 페이지가 이미 존재하는지 조회합니다.
        """
        iso_date = ticket.open_datetime.astimezone(settings.user_timezone).isoformat()
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
            logger.debug(f"❌ 페이지 없음: {ticket.title} (오픈일시={ticket.open_datetime})")
        else:
            logger.debug(f"✅ 페이지 존재: {ticket.title} (page_id={results[0]['id']})")
        return results[0] if results else None

    def _build_properties(self, ticket: TicketInfo) -> dict:
        """
        TicketInfo 모델을 Notion 페이지 속성(JSON)으로 변환합니다.
        """
        iso_date = ticket.open_datetime.astimezone(settings.user_timezone).isoformat()
        logger.debug("오픈일시 변환: %s -> %s", ticket.open_datetime, iso_date)
        return {
            "공연 제목": {
                "title": [{"type": "text", "text": {"content": ticket.title}}]
            },
            "구분": {
                "rich_text": [{"type": "text", "text": {"content": ticket.category}}]
            },
            "오픈 일시": {
                "date": {"start": iso_date, "time_zone": str(settings.DEFAULT_TIMEZONE)}
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

    def upsert_ticket(self, ticket: TicketInfo) -> None:
        try:
            existing = self._find_page(ticket)
            props    = self._build_properties(ticket)

            logger.debug("업데이트할 속성: %s", props)

            if existing:
                # 업데이트 시에도 블록 콘텐츠를 덮어쓰려면 children.replace도 활용 가능
                self.client.pages.update(page_id=existing["id"], properties=props)
                page_id = existing["id"]
                logger.info(f"🔁 업데이트: {ticket.title} (page_id={page_id})")
            else:
                logger.debug("?????????????????????????????")
                created = self.client.pages.create(
                    parent={"database_id": self.database_id},
                    properties=props
                )
                page_id = created["id"]
                logger.info(f"🆕 생성: {ticket.title} (page_id={page_id})")

                # 페이지 본문에 content 텍스트 추가
                self.client.blocks.children.append(
                    block_id=page_id,
                    children=[
                        {
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [
                                    {"type":"text","text":{"content": ticket.content}}
                                ]
                            }
                        }
                    ]
                )
                logger.info(f"✏️ 본문 추가: {len(ticket.content)}자")
        except Exception:
            logger.exception(f"❌ Notion 처리 실패: {ticket.title}")


    def write_all(self, tickets: List[TicketInfo]) -> None:
        """
        다수의 티켓 정보를 순차적으로 처리합니다.
        """
        for ticket in tickets:
            self.upsert_ticket(ticket)
