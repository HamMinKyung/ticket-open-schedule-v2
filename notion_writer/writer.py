# notion_db_writer.py
import asyncio
import logging
import time
import threading
import random
import math
from typing import Optional, List

logger = logging.getLogger(__name__)

from ics.grammar.parse import ContentLine
from notion_client import Client
from notion_client.errors import APIResponseError, HTTPResponseError, RequestTimeoutError
from utils.config import settings
from models.ticket import TicketInfo
from ics import Calendar, Event
import re
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import glob
from urllib.parse import quote


class _NotionRequestGate:
    """프로세스 내 모든 Notion 요청과 재시도의 속도를 함께 제한합니다."""

    def __init__(self, interval: float = 0.5):
        self.interval = interval
        self.next_request_at = 0.0
        self.lock = threading.Lock()

    def call(self, fn, *args, retries: int = 3, **kwargs):
        if retries < 1:
            raise ValueError("retries must be at least 1")
        # 응답과 재시도까지 직렬화하여 429 대기 중 다른 작업의 요청도 멈춥니다.
        with self.lock:
            for attempt in range(retries):
                wait = self.next_request_at - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                self.next_request_at = time.monotonic() + self.interval
                try:
                    return fn(*args, **kwargs)
                except HTTPResponseError as exc:
                    if exc.status not in (429, 529):
                        raise
                    try:
                        retry_after = float(exc.headers.get("Retry-After", "0"))
                        if not math.isfinite(retry_after) or retry_after < 0:
                            retry_after = 0.0
                    except (TypeError, ValueError):
                        retry_after = 0.0
                    wait = max(retry_after, 2 ** attempt) + random.uniform(0, 0.25)
                    self.next_request_at = max(self.next_request_at, time.monotonic() + wait)
                    if attempt == retries - 1:
                        raise
                    logger.warning("Notion API %s - 전체 요청 %.2f초 대기 후 재시도 (%s/%s)",
                                   exc.status, wait, attempt + 1, retries - 1)
                except RequestTimeoutError:
                    if attempt == retries - 1:
                        raise
                    wait = 2 ** attempt
                    self.next_request_at = max(self.next_request_at, time.monotonic() + wait)
                    logger.warning("Notion API 타임아웃 - %s초 후 재시도 (%s/%s)",
                                   wait, attempt + 1, retries - 1)


_NOTION_GATE = _NotionRequestGate()


def _notion_call(fn, *args, retries: int = 3, **kwargs):
    return _NOTION_GATE.call(fn, *args, retries=retries, **kwargs)


class NotionRepository:
    """
    Notion API를 통한 데이터베이스 CRUD를 담당합니다.
    """

    def __init__(
            self,
            client: Optional[Client] = None,
            database_id: Optional[str] = None
    ):
        # 재시도는 공통 게이트에서 수행하여 SDK의 별도 재시도와 중복되지 않게 합니다.
        self.client = client or Client(auth=settings.NOTION_TOKEN, max_retries=0)
        self.database_id = database_id or settings.NOTION_DB_ID
        self.actor_db_id = settings.NOTION_ACT_DB_ID
        self.title_db_id = settings.NOTION_TITLE_DB_ID
        self._data_source_ids: dict[str, str] = {}
        self._page_index = None
        self.actor_name_map = self._load_actor_name_map()
        self.title_name_map = self._load_title_name_map()
        self.output_dir = settings.GB_ICAL_DIR
        self.ical_url = settings.GB_ICAL_URL

        os.makedirs(self.output_dir, exist_ok=True)

    def _find_page(self, ticket: TicketInfo) -> Optional[dict]:
        """
        동일 제목 및 오픈일시의 페이지가 이미 존재하는지 조회합니다.
        """
        if self._page_index is not None:
            return self._page_index.get(self._ticket_key(ticket))
        local_dt = self._local_open_datetime(ticket)
        iso_date = local_dt.isoformat(timespec="seconds")
        response = self._query_collection(
            self.database_id,
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

    @staticmethod
    def _date_value(value, time_zone=None):
        if not value:
            return None
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(time_zone) if time_zone else settings.DEFAULT_TIMEZONE)
        return dt.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _text_value(parts):
        return "".join(part.get("text", {}).get("content", part.get("plain_text", "")) for part in parts)

    def _ticket_key(self, ticket):
        return ticket.title, self._date_value(self._local_open_datetime(ticket).isoformat(timespec="seconds"))

    def _load_ticket_index(self, tickets):
        dates = [self._local_open_datetime(ticket).replace(microsecond=0) for ticket in tickets]
        pages = self._get_all_pages(self.database_id, filter={"and": [
            {"property": "오픈 일시", "date": {"on_or_after": min(dates).isoformat()}},
            {"property": "오픈 일시", "date": {"on_or_before": max(dates).isoformat()}},
        ]})
        index = {}
        for page in pages:
            props = page.get("properties", {})
            date = props.get("오픈 일시", {}).get("date")
            if date and date.get("start"):
                key = (self._text_value(props.get("공연 제목", {}).get("title", [])),
                       self._date_value(date["start"], date.get("time_zone")))
                index.setdefault(key, page)
        logger.info("기존 티켓 일괄 조회 완료: %s건", len(pages))
        return index

    def _property_value(self, prop, kind):
        value = prop.get(kind)
        if kind in ("title", "rich_text"):
            return self._text_value(value or [])
        if kind == "multi_select":
            return sorted(item["name"] for item in (value or []))
        if kind == "relation":
            return sorted(item["id"].replace("-", "").lower() for item in (value or []))
        if kind == "select":
            return value.get("name") if value else None
        if kind == "date":
            if not value:
                return None
            return tuple(self._date_value(value.get(key), value.get("time_zone")) for key in ("start", "end"))
        return value

    def _changed_properties(self, page, desired):
        existing = page.get("properties", {})
        changes = {}
        for name, prop in desired.items():
            kind = next(iter(prop))
            current = existing.get(name, {})
            if kind == "relation" and current.get("has_more"):
                relations = []
                cursor = None
                while True:
                    params = {"page_id": page["id"], "property_id": current["id"], "page_size": 100}
                    if cursor:
                        params["start_cursor"] = cursor
                    response = _notion_call(self.client.pages.properties.retrieve, **params)
                    relations.extend(item["relation"] for item in response["results"])
                    if not response.get("has_more"):
                        break
                    cursor = self._next_cursor(response, cursor)
                current = {"relation": relations}
            if self._property_value(current, kind) != self._property_value(prop, kind):
                changes[name] = prop
        return changes

    @staticmethod
    def _block_value(block):
        kind = block["type"]
        content = block.get(kind, {})
        # API가 추가하는 ID, 작성 시각, plain_text 등은 비교에서 제외합니다.
        return (kind, NotionRepository._text_value(content.get("rich_text", [])),
                content.get("color", "default"))

    @staticmethod
    def _next_cursor(response, current):
        cursor = response.get("next_cursor")
        if not cursor or cursor == current:
            raise RuntimeError("Notion pagination did not advance")
        return cursor

    def _get_all_blocks(self, page_id):
        blocks, cursor = [], None
        while True:
            params = {"block_id": page_id, "page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            response = _notion_call(self.client.blocks.children.list, **params)
            blocks.extend(response.get("results", []))
            if not response.get("has_more"):
                return blocks
            cursor = self._next_cursor(response, cursor)

    def _append_blocks(self, page_id, blocks):
        for offset in range(0, len(blocks), 100):
            _notion_call(self.client.blocks.children.append,
                         block_id=page_id, children=blocks[offset:offset + 100])

    def _sync_blocks(self, page_id, desired):
        existing = self._get_all_blocks(page_id)
        changed = False
        shared = 0
        for old, new in zip(existing, desired):
            if old["type"] != new["type"]:
                break
            if self._block_value(old) != self._block_value(new):
                _notion_call(self.client.blocks.update, block_id=old["id"], **{new["type"]: new[new["type"]]})
                changed = True
            shared += 1
        # 타입이나 길이가 바뀐 경우에만 나머지 구간을 교체합니다.
        for old in existing[shared:]:
            _notion_call(self.client.blocks.delete, block_id=old["id"])
            changed = True
        if desired[shared:]:
            self._append_blocks(page_id, desired[shared:])
            changed = True
        return changed

    def _build_properties(self, ticket: TicketInfo) -> dict:
        """
        TicketInfo 모델을 Notion 페이지 속성(JSON)으로 변환합니다.
        """
        local_dt = self._local_open_datetime(ticket)
        iso_date = local_dt.isoformat(timespec="seconds")

        props = {
            "공연 제목": {
                "title": [{"type": "text", "text": {"content": ticket.title}}]
            },
            "구분": {
                "rich_text": [{"type": "text", "text": {"content": ticket.category[:2000]}}]
            },
            "오픈 일시": {
                "date": {"start": iso_date}
            },
            "오픈 회차": {
                "rich_text": [{"type": "text", "text": {"content": ticket.round_info[:2000]}}]
            },
            "공연 기간": {
                "rich_text": [{"type": "text", "text": {"content": ticket.performance_period[:2000]}}]
            },
            "오픈 타입": {
                "multi_select": [{"name": name} for name in sorted(ticket.open_type_all)]
            },
            "공연 장소": {
                "rich_text": [{"type": "text", "text": {"content": ticket.venue[:2000]}}]
            },
            "출연진": {
                "rich_text": [{"type": "text", "text": {"content": ticket.cast[:2000]}}]
            },
            "예매처": {
                "multi_select": [{"name": name} for name in sorted(ticket.providers)]
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
            "관련 작품": {
                "relation": [
                    {"id": self.title_name_map[name]}
                    for name in set(
                        self._extract_names_from_title(ticket.title)
                    )
                    if name in self.title_name_map
                ]
            },
            "등록 링크": {"url": ticket.ical_url},
            "지역": {
                "select": {"name": ticket.regions}
            }
        }

        urls = self._ordered_detail_urls(ticket)
        for idx, url in enumerate(urls):
            key = "상세 링크" if idx == 0 else f"상세 링크{idx + 1}"
            props[key] = {"url": url}

        return props

    def _build_contents(self, content: dict, ical_url: str) -> list[dict]:
        """
        TicketInfo.content 딕셔너리를 Notion 블록 리스트로 변환합니다.
        긴 텍스트(value)는 2000자씩 잘라 여러 paragraph 블록으로 분할 삽입합니다.
        """

        def utf16_len(s: str) -> int:
            return sum(2 if ord(c) > 0xFFFF else 1 for c in s)

        def chunk_text(text: str, limit: int = 2000) -> list[str]:
            chunks, current, current_len = [], [], 0
            for ch in text:
                ch_len = 2 if ord(ch) > 0xFFFF else 1
                if current_len + ch_len > limit:
                    chunks.append("".join(current))
                    current, current_len = [], 0
                current.append(ch)
                current_len += ch_len
            if current:
                chunks.append("".join(current))
            return chunks or [""]

        if not isinstance(content, dict):
            content = {"내용": str(content)} if content else {}

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
            for chunk in chunk_text(str(value)):
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
                changes = self._changed_properties(existing, props)
                if changes:
                    _notion_call(self.client.pages.update, page_id=page_id, properties=changes)
                    existing["properties"].update(changes)
                body_changed = self._sync_blocks(page_id, contents)
                if changes or body_changed:
                    logger.info("🔁 변경 부분만 갱신: %s (page_id=%s)", ticket.title, page_id)
                else:
                    logger.info("변경 없음, 쓰기 생략: %s (page_id=%s)", ticket.title, page_id)

            else:
                # 생성 시 children 옵션으로 한 번에 삽입
                created = _notion_call(self.client.pages.create,
                    parent=self._page_parent(self.database_id),
                    properties=props,
                    children=contents[:100]
                )
                page_id = created["id"]
                if self._page_index is not None:
                    self._page_index[self._ticket_key(ticket)] = {"id": page_id, "properties": props}
                self._append_blocks(page_id, contents[100:])
                logger.info(f"🆕 생성 및 블록 삽입 완료: {ticket.title} (page_id={page_id})")

        except Exception as ex:
            logger.error(f"❌ Notion 처리 실패: {ticket.title}", exc_info=ex)

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

    def _load_title_name_map(self) -> dict:
        results = self._get_all_pages(self.title_db_id)
        return {
            p["properties"]["공연명"]["title"][0]["plain_text"]: p["id"]
            for p in results
            if p["properties"]["공연명"]["title"]
        }

    def _extract_names_from_cast(self, cast_text: str) -> list[str]:
        matched_names = []
        for name in self.actor_name_map.keys():
            # 경계 처리: 이름 앞뒤가 (시작/끝/공백/쉼표/개행/구두점) 중 하나일 때만 매칭
            pattern = rf'(?<!\w){re.escape(name)}(?!\w)'
            if re.search(pattern, cast_text):
                matched_names.append(name)

        return matched_names

    def _extract_names_from_title(self, title_text: str) -> list[str]:
        matched_names = []
        for name in self.title_name_map.keys():
            if name in title_text:
                matched_names.append(name)

        return matched_names

    async def write_all(self, tickets: List[TicketInfo]) -> None:
        if not tickets:
            return
        # 조회 실패 시 신규 티켓으로 오인하여 중복 생성하지 않도록 쓰기 전에 완료합니다.
        self._page_index = await asyncio.to_thread(self._load_ticket_index, tickets)
        try:
            # API는 어차피 직렬화됩니다. 동일 키의 중복 입력도 생성된 페이지를 재사용합니다.
            for ticket in tickets:
                await asyncio.to_thread(self.upsert_ticket, ticket)
        finally:
            self._page_index = None

        ics_files = glob.glob(f"{self.output_dir}/*.ics")
        logger.info(f"📁 {self.output_dir} 내 .ics 파일 수: {len(ics_files)}개")

    def sync_existing_ticket_relations(self):
        pages = self._get_all_pages(self.database_id)
        logger.info(f"🔄 기존 티켓 DB에서 출연진 필드 기반으로 출연 배우 Relation 갱신 시작: {len(pages)}건")
        for page in pages:
            page_id = page["id"]
            title = page["properties"].get("공연 제목", {}).get("title", [])
            title_str = title[0]["plain_text"] if title else "(제목 없음)"
            cast_field = page["properties"].get("출연진", {}).get("rich_text", [])
            cast_text = cast_field[0]["plain_text"] if cast_field else ""

            if not cast_text.strip():
                logger.debug(f"⚠️ 출연진 없음: {title_str}")
                continue
            names = set(
                self._extract_names_from_cast(cast_text) +
                self._extract_names_from_cast(title_str)
            )
            title_names = set(self._extract_names_from_title(title_str))
            matched_actor_ids = [
                {"id": self.actor_name_map[name]}
                for name in names
                if name in self.actor_name_map
            ]
            matched_title_ids = [
                {"id": self.title_name_map[name]}
                for name in title_names
                if name in self.title_name_map
            ]

            if not matched_actor_ids and not matched_title_ids:
                logger.debug(f"⚠️ 매칭 배우 및 작품 없음: {title_str}")
                continue

            properties = {}
            if matched_actor_ids:
                properties["출연 배우"] = {"relation": matched_actor_ids}
            if matched_title_ids:
                properties["관련 작품"] = {"relation": matched_title_ids}

            try:
                properties = self._changed_properties(page, properties)
                if not properties:
                    continue
                _notion_call(self.client.pages.update,
                    page_id=page_id,
                    properties=properties
                )
                logger.info(f"✅ 갱신 완료: {title_str}")
            except Exception as ex:
                logger.error(f"❌ 갱신 실패: {title_str}", exc_info=ex)

    def _get_all_pages(self, database_id: str, **query_params) -> list:
        results = []
        start_cursor = None

        while True:
            params = {**query_params, "page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor

            response = self._query_collection(database_id, **params)
            results.extend(response.get("results", []))

            if response.get("has_more"):
                start_cursor = self._next_cursor(response, start_cursor)
            else:
                break

        return results

    def _query_collection(self, database_id: str, **kwargs) -> dict:
        if hasattr(self.client, "data_sources"):
            ds_id = self._resolve_data_source_id(database_id)
            return _notion_call(self.client.data_sources.query, data_source_id=ds_id, **kwargs)
        return _notion_call(self.client.databases.query, database_id=database_id, **kwargs)

    def _page_parent(self, database_id: str) -> dict:
        if hasattr(self.client, "data_sources"):
            return {"data_source_id": self._resolve_data_source_id(database_id)}
        return {"database_id": database_id}

    def _generate_ics_and_push(self, ticket: TicketInfo) -> str:
        """
        티켓 정보를 기반으로 ICS 파일을 생성하고 github page에 업로드합니다.
        """
        slug_title = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", ticket.title.replace(" ", "_")).strip("._ ")
        slug = f"{slug_title}_{ticket.open_datetime.strftime('%Y%m%d%H%M')}"
        file_name = f"{slug}.ics"
        file_path = os.path.join(self.output_dir, file_name)

        cal = Calendar()
        event = Event()
        event.name = f"티켓오픈 {ticket.title}"
        event.begin = self._local_open_datetime(ticket)
        event.end = event.begin + timedelta(minutes=30)
        event.location = ticket.venue
        # 출연 배우 이름 추출 (중복 호출 방지)
        cast_names = self._extract_names_from_cast(ticket.cast)
        title_names = self._extract_names_from_cast(ticket.title)
        related_work_names = self._extract_names_from_title(ticket.title)
        all_names = list(set(cast_names + title_names + related_work_names))

        event.description = ", ".join(ticket.providers) +" "+ ", ".join(all_names)
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

        base_url = self.ical_url.rstrip("/")
        output_dir = self.output_dir.replace("\\", "/").strip("/")
        return f"{base_url}/{output_dir}/{quote(file_name)}"

    @staticmethod
    def _ordered_detail_urls(ticket: TicketInfo) -> list[str]:
        urls = list(ticket.detail_url_all)
        if ticket.detail_url and ticket.detail_url not in urls:
            urls.insert(0, ticket.detail_url)
        return sorted(urls, key=lambda url: (url != ticket.detail_url, url))

    @staticmethod
    def _local_open_datetime(ticket: TicketInfo):
        dt = ticket.open_datetime
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=settings.DEFAULT_TIMEZONE)
        return dt.astimezone(settings.DEFAULT_TIMEZONE)

    def _resolve_data_source_id(self, database_or_data_source_id: str) -> str:
        """
        설정의 DB ID를 먼저 조회하고, 성공한 ID 변환은 재사용합니다.
        DB가 없을 때만 직접 지정된 data source ID인지 확인합니다.
        """
        if database_or_data_source_id in self._data_source_ids:
            return self._data_source_ids[database_or_data_source_id]

        try:
            db = _notion_call(self.client.databases.retrieve, database_id=database_or_data_source_id)
        except APIResponseError as exc:
            if exc.code != "object_not_found":
                raise
            _notion_call(self.client.data_sources.retrieve, data_source_id=database_or_data_source_id)
            data_source_id = database_or_data_source_id
        else:
            # 단일 소스 가정: 첫 번째 data_source를 사용
            data_sources = db.get("data_sources", [])
            if not data_sources:
                raise RuntimeError("Database has no data_sources; share/permissions or structure issue.")
            data_source_id = data_sources[0]["id"]

        self._data_source_ids[database_or_data_source_id] = data_source_id
        return data_source_id
