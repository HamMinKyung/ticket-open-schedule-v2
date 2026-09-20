import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from crawler.melon import MelonCrawler


class Response:
    def __init__(self, status, html=""):
        self.status = status
        self.html = html

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def text(self):
        return self.html


class Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, **kwargs):
        self.calls.append(kwargs["data"])
        return next(self.responses)


class MelonLockTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.crawler = MelonCrawler((datetime(2026, 9, 21), datetime(2026, 9, 28)))
        self.sleep = patch("crawler.melon.asyncio.sleep", new_callable=AsyncMock)
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    async def test_first_page_lock_stops_all_remaining_pages_and_genres(self):
        session = Session([Response(423)])
        self.assertEqual(await self.crawler._fetch_list(session), [])
        self.assertTrue(self.crawler.locked)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(session.calls[0]["pageIndex"], "1")

    async def test_later_page_lock_also_stops_collection(self):
        session = Session([Response(200), Response(423)])
        self.assertEqual(await self.crawler._fetch_list(session), [])
        self.assertTrue(self.crawler.locked)
        self.assertEqual(len(session.calls), 2)

    async def test_crawl_preserves_lock_signal_and_skips_details(self):
        session = Session([Response(423)])
        with patch("crawler.base.aiohttp.ClientSession", return_value=session), patch.object(
            self.crawler, "_fetch_detail", new_callable=AsyncMock
        ) as detail:
            self.assertEqual(await self.crawler.crawl(), [])
        self.assertTrue(self.crawler.locked)
        detail.assert_not_awaited()

    async def test_success_still_fetches_all_configured_pages(self):
        count = len(self.crawler.cfg["pages"]) * len(self.crawler.cfg["genre_map"])
        session = Session([Response(200) for _ in range(count)])
        self.crawler.locked = True
        self.assertEqual(await self.crawler._fetch_list(session), [])
        self.assertFalse(self.crawler.locked)
        self.assertEqual(len(session.calls), count)


if __name__ == "__main__":
    unittest.main()
