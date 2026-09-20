import unittest
import tempfile
import json
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import httpx
from notion_client import Client
from notion_client.errors import APIResponseError

from notion_writer.writer import NotionRepository, _NotionRequestGate, _notion_call


def api_error(code, status, headers=None):
    return APIResponseError(
        status=status, message=code, code=code,
        headers=httpx.Headers(headers), raw_body_text="",
    )


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class ClientInitializationTests(unittest.TestCase):
    def test_sdk_queries_and_creates_pages_using_resolved_data_source(self):
        requests = []

        def respond(request):
            requests.append(request)
            self.assertEqual(request.headers["Notion-Version"], "2025-09-03")
            if request.url.path == "/v1/databases/database":
                return httpx.Response(200, json={"id": "database", "data_sources": [{"id": "source"}]})
            if request.url.path == "/v1/data_sources/source/query":
                return httpx.Response(200, json={"results": [], "has_more": False})
            if request.url.path == "/v1/pages":
                return httpx.Response(200, json={"id": "new-page"})
            self.fail(f"Unexpected request: {request.method} {request.url.path}")

        with closing(Client(auth="test", retry=False, notion_version="2025-09-03", client=httpx.Client(
            transport=httpx.MockTransport(respond)
        ))) as client:
            repo = NotionRepository.__new__(NotionRepository)
            repo.client = client
            repo._data_source_ids = {}
            with patch("notion_writer.writer._NOTION_GATE", _NotionRequestGate(interval=0)):
                repo._query_collection("database", page_size=100)
                _notion_call(client.pages.create, parent=repo._page_parent("database"), properties={})
        self.assertEqual(len(requests), 3)
        self.assertEqual(json.loads(requests[1].content), {"page_size": 100})
        self.assertEqual(json.loads(requests[2].content)["parent"], {"data_source_id": "source"})

    def test_real_client_initializes_and_disables_sdk_retries(self):
        with tempfile.TemporaryDirectory() as directory, patch(
            "notion_writer.writer.settings.GB_ICAL_DIR", directory
        ), patch.object(NotionRepository, "_load_actor_name_map", return_value={}), patch.object(
            NotionRepository, "_load_title_name_map", return_value={}
        ):
            repo = NotionRepository()
            self.addCleanup(repo.client.close)
            self.assertIs(repo.client.options.retry, False)
            self.assertEqual(repo.client.options.notion_version, "2025-09-03")

            # SDK 전체 경로에서 429가 공통 게이트까지 전달되는지 검증합니다.
            clock = FakeClock()
            starts = []

            def respond(request):
                starts.append(clock.now)
                if len(starts) == 1:
                    return httpx.Response(429, headers={"Retry-After": "7"}, json={
                        "object": "error", "status": 429, "code": "rate_limited", "message": "test limit",
                    })
                return httpx.Response(200, json={"object": "database", "id": "test-database"})

            repo.client.client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch("notion_writer.writer._NOTION_GATE", _NotionRequestGate()), patch(
                "notion_writer.writer.time.monotonic", clock.monotonic
            ), patch("notion_writer.writer.time.sleep", clock.sleep), patch(
                "notion_writer.writer.random.uniform", return_value=0
            ):
                result = _notion_call(repo.client.databases.retrieve, database_id="test-database")
            self.assertEqual(result["id"], "test-database")
            self.assertEqual(starts, [100, 107])


class NotionGateTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.gate = _NotionRequestGate()
        for target, value in [
            ("time.monotonic", self.clock.monotonic),
            ("time.sleep", self.clock.sleep),
            ("random.uniform", lambda *args: 0),
        ]:
            patcher = patch("notion_writer.writer." + target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_concurrent_requests_share_one_rate_limit(self):
        starts = []
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda _: self.gate.call(lambda: starts.append(self.clock.now)), range(6)))
        self.assertEqual(starts, [100, 100.5, 101, 101.5, 102, 102.5])

    def test_retry_after_is_respected_then_other_requests_remain_paced(self):
        fn = Mock(side_effect=[api_error("rate_limited", 429, {"Retry-After": "7"}), "ok"])
        self.assertEqual(self.gate.call(fn), "ok")
        self.assertEqual(self.clock.now, 107)
        self.gate.call(lambda: None)
        self.assertEqual(self.clock.now, 107.5)

    def test_exhausted_retry_preserves_cooldown_for_next_worker(self):
        fn = Mock(side_effect=api_error("rate_limited", 429, {"Retry-After": "9"}))
        with self.assertRaises(APIResponseError):
            self.gate.call(fn)
        self.assertEqual(fn.call_count, 3)
        self.assertEqual(self.clock.now, 118)
        self.gate.call(lambda: None)
        self.assertEqual(self.clock.now, 127)

    def test_invalid_retry_header_uses_exponential_backoff(self):
        fn = Mock(side_effect=[api_error("rate_limited", 429, {"Retry-After": "invalid"}),
                               api_error("rate_limited", 429), "ok"])
        self.assertEqual(self.gate.call(fn), "ok")
        self.assertEqual(self.clock.now, 103)

    def test_overload_uses_server_delay(self):
        fn = Mock(side_effect=[api_error("service_overload", 529, {"Retry-After": "65"}), "ok"])
        self.assertEqual(self.gate.call(fn), "ok")
        self.assertEqual(self.clock.now, 165)

    def test_permission_error_is_not_retried(self):
        fn = Mock(side_effect=api_error("restricted_resource", 403))
        with self.assertRaises(APIResponseError):
            self.gate.call(fn)
        fn.assert_called_once()


class DataSourceResolutionTests(unittest.TestCase):
    def setUp(self):
        clock = FakeClock()
        for target, value in [
            ("_NOTION_GATE", _NotionRequestGate()),
            ("time.monotonic", clock.monotonic),
            ("time.sleep", clock.sleep),
        ]:
            patcher = patch("notion_writer.writer." + target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.repo = NotionRepository.__new__(NotionRepository)
        self.repo.client = Mock()
        self.repo._data_source_ids = {}

    def test_database_resolution_is_shared_by_queries_and_page_creation(self):
        self.repo.client.databases.retrieve.return_value = {"data_sources": [{"id": "source"}]}
        self.repo._query_collection("database", start_cursor="next")
        self.assertEqual(self.repo._page_parent("database"), {"data_source_id": "source"})
        self.repo.client.databases.retrieve.assert_called_once_with(database_id="database")
        self.repo.client.data_sources.retrieve.assert_not_called()
        self.repo.client.data_sources.query.assert_called_once_with(
            data_source_id="source", start_cursor="next"
        )

    def test_direct_source_id_is_supported_and_cached(self):
        self.repo.client.databases.retrieve.side_effect = api_error("object_not_found", 404)
        self.assertEqual(self.repo._resolve_data_source_id("source"), "source")
        self.assertEqual(self.repo._resolve_data_source_id("source"), "source")
        self.repo.client.data_sources.retrieve.assert_called_once_with(data_source_id="source")

    def test_other_errors_are_not_treated_as_id_mismatches(self):
        for code, status in [("unauthorized", 401), ("restricted_resource", 403), ("rate_limited", 429)]:
            with self.subTest(code=code):
                self.repo.client.databases.retrieve.side_effect = api_error(code, status)
                with self.assertRaises(APIResponseError):
                    self.repo._resolve_data_source_id("database")
                self.repo.client.data_sources.retrieve.assert_not_called()
                self.assertEqual(self.repo._data_source_ids, {})

    def test_inaccessible_id_is_not_cached(self):
        self.repo.client.databases.retrieve.side_effect = api_error("object_not_found", 404)
        self.repo.client.data_sources.retrieve.side_effect = api_error("object_not_found", 404)
        with self.assertRaises(APIResponseError):
            self.repo._resolve_data_source_id("missing")
        self.assertEqual(self.repo._data_source_ids, {})

    def test_empty_database_is_not_cached(self):
        self.repo.client.databases.retrieve.return_value = {"data_sources": []}
        with self.assertRaises(RuntimeError):
            self.repo._resolve_data_source_id("database")
        self.repo.client.data_sources.retrieve.assert_not_called()
        self.assertEqual(self.repo._data_source_ids, {})


if __name__ == "__main__":
    unittest.main()
