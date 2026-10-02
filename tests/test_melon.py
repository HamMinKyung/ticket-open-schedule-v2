import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from crawler.melon import MelonCrawler
from bs4 import BeautifulSoup


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
    def __init__(self, responses, get_responses=None):
        self.responses = iter(responses)
        self.calls = []
        self.get_calls = []
        self.get_responses = iter(get_responses or [Response(200)])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return next(self.get_responses)

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

    async def test_empty_page_stops_each_genre(self):
        count = len(self.crawler.cfg["genre_map"])
        session = Session([Response(200) for _ in range(count)])
        self.crawler.locked = True
        self.assertEqual(await self.crawler._fetch_list(session), [])
        self.assertFalse(self.crawler.locked)
        self.assertEqual(len(session.calls), count)

    async def test_entry_lock_skips_ajax_requests(self):
        session = Session([], [Response(423)])
        self.assertEqual(await self.crawler._fetch_list(session), [])
        self.assertTrue(self.crawler.locked)
        self.assertEqual(session.calls, [])

    async def test_detail_lock_stops_queued_details(self):
        session = Session([], [Response(423)])
        item = {'title_tag': BeautifulSoup('<a href="./detail.htm?csoonId=1">test</a>', 'html.parser').a}
        self.assertEqual(await self.crawler._fetch_detail(session, item), [])
        self.assertTrue(self.crawler.locked)
        self.assertEqual(await self.crawler._fetch_detail(session, item), [])
        self.assertEqual(len(session.get_calls), 1)

    async def test_session_headers_and_ajax_header(self):
        from unittest.mock import MagicMock
        session = MagicMock()
        session.get.return_value = Response(200)
        session.post.return_value = Response(423)
        await self.crawler._fetch_list(session)
        initial = session.get.call_args.kwargs['headers']
        ajax = session.post.call_args.kwargs['headers']
        self.assertEqual(initial['User-Agent'], ajax['User-Agent'])
        self.assertEqual(ajax['X-Requested-With'], 'XMLHttpRequest')
        self.assertNotIn('X-Requested-With', self.crawler._get_headers())


if __name__ == "__main__":
    unittest.main()
