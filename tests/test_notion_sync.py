import copy
import tempfile
import unittest
from datetime import datetime
from unittest.mock import Mock, patch

from models.ticket import TicketInfo
from notion_writer.writer import NotionRepository


class NotionSyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        patcher = patch("notion_writer.writer._notion_call", side_effect=lambda fn, *a, **kw: fn(*a, **kw))
        patcher.start()
        self.addCleanup(patcher.stop)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.repo = NotionRepository.__new__(NotionRepository)
        self.repo.client = Mock()
        self.repo.database_id = "database"
        self.repo._page_index = None
        self.repo.actor_name_map = {}
        self.repo.title_name_map = {}
        self.repo.output_dir = directory.name
        self.repo._generate_ics_and_push = Mock(return_value="https://example.com/ticket.ics")
        self.repo._page_parent = Mock(return_value={"data_source_id": "source"})
        self.repo._query_collection = Mock()
        self.repo.client.blocks.children.list.return_value = {"results": [], "has_more": False}
        self.repo.client.comments.list.return_value = {"results": [], "has_more": False}
        self.ticket = TicketInfo(
            title="테스트 공연", open_datetime=datetime(2026, 9, 21, 14),
            source="멜론티켓", providers={"멜론티켓", "NOL"},
            content={"소개": "공연 안내"}, regions="서울", venue="공연장",
            ical_url="https://example.com/ticket.ics",
        )

    def page(self, ticket=None, page_id="page"):
        ticket = ticket or self.ticket
        props = copy.deepcopy(self.repo._build_properties(ticket))
        # 실제 API 응답에 포함되는 메타데이터와 UTC 표기, 목록 순서 차이를 재현합니다.
        for name, prop in props.items():
            kind = next(iter(prop))
            prop.update(id=name, type=kind)
        props["오픈 일시"]["date"] = {"start": "2026-09-21T05:00:00.000Z", "end": None, "time_zone": None}
        props["예매처"]["multi_select"].reverse()
        for item in props["예매처"]["multi_select"]:
            item.update(id="option", color="default")
        return {"id": page_id, "properties": props}

    def blocks(self, ticket=None):
        blocks = self.repo._build_contents((ticket or self.ticket).content, "")
        for number, block in enumerate(blocks):
            block.update(id=f"block-{number}", created_time="ignored", has_children=False)
            content = block[block["type"]]
            content.setdefault("color", "default")
            for part in content["rich_text"]:
                part["plain_text"] = part["text"]["content"]
                part["annotations"] = {"bold": False, "color": "default"}
        return blocks

    def set_existing(self, page=None, blocks=None):
        self.repo._query_collection.return_value = {"results": [page or self.page()], "has_more": False}
        self.repo.client.blocks.children.list.return_value = {
            "results": self.blocks() if blocks is None else blocks, "has_more": False,
        }

    def assert_no_writes(self):
        self.repo.client.pages.update.assert_not_called()
        self.repo.client.pages.create.assert_not_called()
        self.repo.client.blocks.update.assert_not_called()
        self.repo.client.blocks.delete.assert_not_called()
        self.repo.client.blocks.children.append.assert_not_called()

    async def test_unchanged_ticket_does_not_write_and_still_generates_ical(self):
        self.set_existing()
        await self.repo.write_all([self.ticket])
        self.assert_no_writes()
        self.repo._query_collection.assert_called_once()
        self.repo._generate_ics_and_push.assert_called_once_with(self.ticket)
        self.assertIsNone(self.repo._page_index)

    async def test_only_changed_property_is_updated(self):
        self.set_existing()
        self.ticket.cast = "새 출연진"
        await self.repo.write_all([self.ticket])
        self.repo.client.pages.update.assert_called_once_with(
            page_id="page", properties={"출연진": {"rich_text": [{"type": "text", "text": {"content": "새 출연진"}}]}}
        )
        self.repo.client.blocks.update.assert_not_called()
        self.repo.client.blocks.delete.assert_not_called()
        self.repo.client.blocks.children.append.assert_not_called()

    async def test_only_changed_body_block_is_updated(self):
        self.set_existing()
        self.ticket.content = {"소개": "수정된 안내"}
        await self.repo.write_all([self.ticket])
        self.repo.client.pages.update.assert_not_called()
        self.repo.client.blocks.update.assert_called_once_with(
            block_id="block-1", paragraph={"rich_text": [{"type": "text", "text": {"content": "수정된 안내"}}], "color": "default"}
        )
        self.repo.client.blocks.delete.assert_not_called()
        self.repo.client.blocks.children.append.assert_not_called()

    async def test_paginated_batch_lookup_matches_split_title_and_utc_date(self):
        other = self.ticket.model_copy(update={"title": "다른 공연"})
        first = self.page()
        first["properties"]["공연 제목"]["title"] = [
            {"text": {"content": "테스트 "}}, {"text": {"content": "공연"}},
        ]
        self.repo._query_collection.side_effect = [
            {"results": [first], "has_more": True, "next_cursor": "next"},
            {"results": [self.page(other, "other")], "has_more": False},
        ]
        self.repo.client.blocks.children.list.return_value = {"results": self.blocks(), "has_more": False}
        await self.repo.write_all([self.ticket, other])
        self.assert_no_writes()
        calls = self.repo._query_collection.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0].kwargs["page_size"], 100)
        self.assertEqual(calls[1].kwargs["start_cursor"], "next")
        self.assertEqual(calls[0].kwargs["filter"], calls[1].kwargs["filter"])
        self.assertEqual(len(calls[0].kwargs["filter"]["and"]), 2)

    async def test_lookup_failure_aborts_without_creating_duplicates(self):
        self.repo._query_collection.side_effect = RuntimeError("query failed")
        with self.assertRaisesRegex(RuntimeError, "query failed"):
            await self.repo.write_all([self.ticket])
        self.assert_no_writes()

    async def test_duplicate_new_ticket_reuses_created_page(self):
        self.repo._query_collection.return_value = {"results": [], "has_more": False}
        self.repo.client.pages.create.return_value = {"id": "new-page"}
        self.repo.client.blocks.children.list.return_value = {"results": self.blocks(), "has_more": False}
        await self.repo.write_all([self.ticket, self.ticket])
        self.repo.client.pages.create.assert_called_once()
        self.repo.client.pages.update.assert_not_called()
        self.repo.client.blocks.update.assert_not_called()
        self.repo._query_collection.assert_called_once()

    async def test_empty_batch_does_not_query(self):
        await self.repo.write_all([])
        self.repo._query_collection.assert_not_called()
        self.assert_no_writes()

    async def test_same_title_and_time_in_different_venues_create_separate_pages(self):
        other = self.ticket.model_copy(update={"venue": "다른 공연장"})
        self.set_existing()
        self.repo.client.pages.create.return_value = {"id": "other-page"}
        await self.repo.write_all([self.ticket, other])
        self.repo.client.pages.create.assert_called_once()
        self.repo.client.pages.update.assert_not_called()

    async def test_existing_pages_match_their_own_venues(self):
        other = self.ticket.model_copy(update={"venue": "다른 공연장"})
        self.repo._query_collection.return_value = {"results": [self.page(), self.page(other, 'other')], "has_more": False}
        self.repo.client.blocks.children.list.return_value = {"results": self.blocks(), "has_more": False}
        await self.repo.write_all([self.ticket, other])
        self.assert_no_writes()

    def test_direct_lookup_checks_venue(self):
        self.set_existing()
        other = self.ticket.model_copy(update={"venue": "다른 공연장"})
        self.assertIsNone(self.repo._find_page(other))
        self.assertEqual(self.repo._find_page(self.ticket)['id'], 'page')

    def test_calendar_files_are_distinct_for_different_venues(self):
        other = self.ticket.model_copy(update={"venue": "다른 공연장"})
        self.repo.ical_url = 'https://example.com'
        one = NotionRepository._generate_ics_and_push(self.repo, self.ticket)
        two = NotionRepository._generate_ics_and_push(self.repo, other)
        self.assertNotEqual(one, two)

    def overflow_ticket(self, count=5):
        self.ticket.detail_url = 'https://example.com/0'
        self.ticket.detail_url_all = {f'https://example.com/{n}' for n in range(count)}
        return self.ticket

    async def test_overflow_links_become_comments_on_new_page(self):
        self.overflow_ticket()
        self.repo._query_collection.return_value = {"results": [], "has_more": False}
        self.repo.client.pages.create.return_value = {"id": "new-page"}
        await self.repo.write_all([self.ticket])
        props = self.repo.client.pages.create.call_args.kwargs['properties']
        self.assertNotIn('상세 링크4', props)
        comment = self.repo.client.comments.create.call_args.kwargs
        self.assertEqual(comment['parent'], {'page_id': 'new-page'})
        self.assertEqual([p['text']['link']['url'] for p in comment['rich_text'] if p['text'].get('link')],
                         ['https://example.com/3', 'https://example.com/4'])

    async def test_existing_page_gets_only_new_overflow_links(self):
        self.overflow_ticket()
        self.set_existing()
        old = {'rich_text': [{'text': {'content': '추가 상세 링크 (자동 등록)\n'}},
                            {'text': {'content': 'https://example.com/3', 'link': {'url': 'https://example.com/3'}}}]}
        self.repo.client.comments.list.side_effect = [
            {'results': [], 'has_more': True, 'next_cursor': 'next'},
            {'results': [old], 'has_more': False},
        ]
        await self.repo.write_all([self.ticket])
        created = self.repo.client.comments.create.call_args.kwargs['rich_text']
        self.assertEqual([p['text']['link']['url'] for p in created if p['text'].get('link')], ['https://example.com/4'])
        self.repo.client.comments.list.side_effect = None
        self.repo.client.comments.list.return_value = {'results': [old, {'rich_text': created}], 'has_more': False}
        self.repo.client.comments.create.reset_mock()
        await self.repo.write_all([self.ticket])
        self.repo.client.comments.create.assert_not_called()

    def test_three_links_never_access_comments_and_unused_properties_clear(self):
        self.overflow_ticket(3)
        self.assertFalse(self.repo._append_overflow_comments('page', self.ticket))
        self.repo.client.comments.list.assert_not_called()
        self.overflow_ticket(1)
        props = self.repo._build_properties(self.ticket)
        self.assertEqual(props['상세 링크2'], {'url': None})
        self.assertEqual(props['상세 링크3'], {'url': None})

    def test_incomplete_comment_pagination_does_not_post(self):
        self.overflow_ticket()
        self.repo.client.comments.list.return_value = {'results': [], 'has_more': True, 'next_cursor': None}
        with self.assertRaisesRegex(RuntimeError, 'pagination'):
            self.repo._append_overflow_comments('page', self.ticket)
        self.repo.client.comments.create.assert_not_called()

    async def test_subsecond_ticket_time_matches_saved_second_precision(self):
        self.set_existing()
        self.ticket.open_datetime = self.ticket.open_datetime.replace(microsecond=123456)
        await self.repo.write_all([self.ticket])
        self.assert_no_writes()

    async def test_incomplete_pagination_aborts_before_writes(self):
        self.repo._query_collection.return_value = {"results": [], "has_more": True, "next_cursor": None}
        with self.assertRaisesRegex(RuntimeError, "pagination"):
            await self.repo.write_all([self.ticket])
        self.assert_no_writes()

    def test_all_block_pages_are_read_before_deleting_removed_tail(self):
        blocks = self.blocks()
        self.repo.client.blocks.children.list.side_effect = [
            {"results": blocks[:1], "has_more": True, "next_cursor": "next"},
            {"results": blocks[1:], "has_more": False},
        ]
        self.assertTrue(self.repo._sync_blocks("page", blocks[:1]))
        self.repo.client.blocks.delete.assert_called_once_with(block_id="block-1")
        self.repo.client.blocks.update.assert_not_called()
        self.repo.client.blocks.children.append.assert_not_called()

    def test_type_change_preserves_matching_prefix(self):
        blocks = self.blocks()
        desired = copy.deepcopy(blocks)
        desired[1] = {"type": "heading_2", "heading_2": {"rich_text": [{"text": {"content": "새 제목"}}]}}
        self.repo.client.blocks.children.list.return_value = {"results": blocks, "has_more": False}
        self.assertTrue(self.repo._sync_blocks("page", desired))
        self.repo.client.blocks.delete.assert_called_once_with(block_id="block-1")
        self.repo.client.blocks.children.append.assert_called_once_with(block_id="page", children=desired[1:])

    def test_appends_are_chunked_to_notion_limit(self):
        blocks = self.blocks() * 101
        self.repo._append_blocks("page", blocks)
        sizes = [len(call.kwargs["children"]) for call in self.repo.client.blocks.children.append.call_args_list]
        self.assertEqual(sizes, [100, 100, 2])

    def test_truncated_relation_is_fully_read_before_comparison(self):
        page = {"id": "page", "properties": {"출연 배우": {"id": "relation-property", "has_more": True, "relation": []}}}
        self.repo.client.pages.properties.retrieve.side_effect = [
            {"results": [{"relation": {"id": "actor-1"}}], "has_more": True, "next_cursor": "next"},
            {"results": [{"relation": {"id": "actor-2"}}], "has_more": False},
        ]
        self.assertEqual(self.repo._changed_properties(page, {"출연 배우": {"relation": [{"id": "actor-2"}, {"id": "actor-1"}]}}), {})
        self.assertEqual(self.repo.client.pages.properties.retrieve.call_count, 2)

    def test_unchanged_follow_relations_are_not_written(self):
        self.repo.actor_name_map = {"배우": "actor"}
        page = self.page()
        page["properties"]["공연 제목"]["title"] = [{"plain_text": "테스트 공연"}]
        page["properties"]["출연진"]["rich_text"] = [{"plain_text": "배우"}]
        page["properties"]["출연 배우"] = {"relation": [{"id": "actor"}]}
        self.repo._query_collection.return_value = {"results": [page], "has_more": False}
        self.repo.sync_existing_ticket_relations()
        self.assert_no_writes()


if __name__ == "__main__":
    unittest.main()
