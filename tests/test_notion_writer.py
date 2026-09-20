import unittest
from unittest.mock import Mock

import httpx
from notion_client.errors import APIResponseError

from notion_writer.writer import NotionRepository


def api_error(code, status):
    return APIResponseError(
        response=httpx.Response(status, request=httpx.Request("GET", "https://api.notion.com")),
        message=code,
        code=code,
    )


class DataSourceResolutionTests(unittest.TestCase):
    def setUp(self):
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
