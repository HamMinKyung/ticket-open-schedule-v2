# notion_db_writer.py
import asyncio
import logging
from typing import Optional, List

from ics.grammar.parse import ContentLine
from notion_client import Client
from utils.config import settings
from models.ticket import TicketInfo
from ics import Calendar, Event
import re
import os
from datetime import timedelta
import glob


class NotionRepository:
    """
    Notion API를 통한 데이터베이스 CRUD를 담당합니다.
    """

    def __init__(
            self,
            client: Optional[Client] = None,
            database_id: Optional[str] = None
    ):
        self.client = client or Client(auth=settings.NOTION_TOKEN)  # log_level=logging.DEBUG
        self.database_id = database_id or settings.NOTION_DB_ID
        self.actor_db_id = settings.NOTION_ACT_DB_ID
        self.actor_name_map = self._load_actor_name_map()
        self.output_dir = settings.GB_ICAL_DIR
        self.ical_url = settings.GB_ICAL_URL

        os.makedirs(self.output_dir, exist_ok=True)

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
        # if not results:
        #     print(f"❌ 페이지 없음: {ticket.title} (오픈일시={ticket.open_datetime})")
        # else:
        #     print(f"✅ 페이지 존재: {ticket.title} (page_id={results[0]['id']})")
        return results[0] if results else None

    def _build_properties(self, ticket: TicketInfo) -> dict:
        """
        TicketInfo 모델을 Notion 페이지 속성(JSON)으로 변환합니다.
        """
        local_dt = ticket.open_datetime.astimezone(settings.user_timezone).replace(tzinfo=settings.DEFAULT_TIMEZONE)
        iso_date = local_dt.isoformat(timespec="seconds")

        # 상세 링크
        for idx, url in enumerate(ticket.detail_url_all):
            props = {
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
                "오픈 타입": {
                    "multi_select": [{"name": name} for name in ticket.open_type_all]
                },
                "공연 장소": {
                    "rich_text": [{"type": "text", "text": {"content": ticket.venue}}]
                },
                # "상세 링크": {"url": ticket.detail_url},

                "출연진": {
                    "rich_text": [{"type": "text", "text": {"content": ticket.cast}}]
                },
                "예매처": {
                    "multi_select": [{"name": name} for name in ticket.providers]
                },
                "단독 판매": {"checkbox": ticket.solo_sale},
                "출연 배우": {
                    "relation": [
                        {"id": self.actor_name_map[name]}
                        for name in set(
                            self._extract_names_from_cast(ticket.cast) +
                            self._extract_names_from_cast(ticket.title)
                        )
                        if name in self.actor_name_map
                    ]
                },
                "등록 링크": {"url": ticket.ical_url}
            }
            key = "상세 링크" if idx == 0 else f"상세 링크{idx + 1}"
            props[key] = {"url": url}

        return props;

    def _build_contents(self, content: dict, ical_url: str) -> list[dict]:
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

            ical_url = self._generate_ics_and_push(ticket)
            ticket.ical_url = ical_url
            props = self._build_properties(ticket)
            contents = self._build_contents(ticket.content, ticket.ical_url)

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

    def _load_actor_name_map(self) -> dict:
        results = self._get_all_pages(self.actor_db_id)
        return {
            p["properties"]["이름"]["title"][0]["plain_text"]: p["id"]
            for p in results
            if p["properties"]["이름"]["title"]
        }

    def _extract_names_from_cast(self, cast_text: str) -> list[str]:
        matched_names = []
        for name in self.actor_name_map.keys():
            # 경계 처리: 이름 앞뒤가 (시작/끝/공백/쉼표/개행/구두점) 중 하나일 때만 매칭
            pattern = rf'(?<!\w){re.escape(name)}(?!\w)'
            if re.search(pattern, cast_text):
                matched_names.append(name)

        return matched_names

    async def write_all(self, tickets: List[TicketInfo]) -> None:
        task = [
            asyncio.to_thread(self.upsert_ticket, ticket)
            for ticket in tickets
        ]

        result = await asyncio.gather(*task, return_exceptions=True)
        for ticket, result in zip(tickets, result):
            if isinstance(result, Exception):
                logging.error(f"❌ 티켓 처리 실패: {ticket.title}", exc_info=result)

        ics_files = glob.glob(f"{self.output_dir}/*.ics")
        print(f"📁 {self.output_dir} 내 .ics 파일 수: {len(ics_files)}개")

    def sync_existing_ticket_relations(self):
        pages = self._get_all_pages(self.database_id)
        print(" 🔄 기존 티켓 DB에서 출연진 필드 기반으로 출연 배우 Relation 갱신 시작", len(pages))
        for page in pages:
            page_id = page["id"]
            title = page["properties"].get("공연 제목", {}).get("title", [])
            title_str = title[0]["plain_text"] if title else "(제목 없음)"
            cast_field = page["properties"].get("출연진", {}).get("rich_text", [])
            cast_text = cast_field[0]["plain_text"] if cast_field else ""

            if not cast_text.strip():
                print(f"⚠️ 출연진 없음: {title_str}")
                continue
            names = set(
                self._extract_names_from_cast(cast_text) +
                self._extract_names_from_cast(title_str)
            )
            matched_actor_ids = [
                {"id": self.actor_name_map[name]}
                for name in names
                if name in self.actor_name_map
            ]

            if not matched_actor_ids:
                print(f"⚠️ 매칭 배우 없음: {title_str}")
                continue

            try:
                self.client.pages.update(
                    page_id=page_id,
                    properties={
                        "출연 배우": {
                            "relation": matched_actor_ids
                        }
                    }
                )
                print(f"✅ 갱신 완료: {title_str}")
            except Exception as ex:
                print(f"❌ 갱신 실패: {title_str}", ex)

    def _get_all_pages(self, database_id: str) -> list:
        results = []
        start_cursor = None

        while True:
            params = {"database_id": database_id}
            if start_cursor:
                params["start_cursor"] = start_cursor

            response = self.client.databases.query(**params)
            results.extend(response.get("results", []))

            if response.get("has_more"):
                start_cursor = response.get("next_cursor")
            else:
                break

        return results

    def _generate_ics_and_push(self, ticket: TicketInfo) -> str:
        """
        티켓 정보를 기반으로 ICS 파일을 생성하고 github page에 업로드합니다.
        """
        slug = f"{ticket.title.replace(' ', '_')}_{ticket.open_datetime.strftime('%Y%m%d%H%M')}"
        file_name = f"{slug}.ics"
        file_path = os.path.join(self.output_dir, file_name)

        cal = Calendar()
        event = Event()
        event.name = f"티켓오픈 {ticket.title}"
        event.begin = ticket.open_datetime.astimezone(settings.user_timezone)
        event.end = event.begin + timedelta(minutes=30)
        event.location = ticket.venue
        # 출연 배우 이름 추출 (중복 호출 방지)
        cast_names = self._extract_names_from_cast(ticket.cast)
        title_names = self._extract_names_from_cast(ticket.title)
        all_names = list(set(cast_names + title_names))

        event.description = ", ".join(ticket.providers + all_names)
        event.categories = {"티켓오픈"}

        # 알림 추가 방식 수정
        alarm_lines = [
            ContentLine(name="BEGIN", value="VALARM"),
            ContentLine(name="TRIGGER", value="-PT30M"),
            ContentLine(name="ACTION", value="DISPLAY"),
            ContentLine(name="DESCRIPTION", value="Reminder"),
            ContentLine(name="END", value="VALARM")
        ]

        for line in alarm_lines:
            event.extra.append(line)

        cal.events.add(event)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(cal.serialize())

        return f"{self.ical_url}{self.output_dir}/{file_name}"
