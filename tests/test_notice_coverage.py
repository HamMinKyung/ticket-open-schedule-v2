import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from bs4 import BeautifulSoup
from crawler.melon import MelonCrawler
from crawler.yes24 import Yes24Crawler
from crawler.sejongpac import SejongPac
from crawler.charlotte import CharlotteCrawler
from crawler.ticketlink import TicketLinkCrawler
from crawler.sac import SacCrawler
from crawler.caci import CaciCrawler
from utils.notice import extract_venue, extract_labeled_openings


def response(html='', data=None):
    r = MagicMock()
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    r.status = 200
    r.text = AsyncMock(return_value=html)
    r.json = AsyncMock(return_value=data)
    return r


class CoverageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dr = (datetime(2026, 10, 1), datetime(2026, 10, 11, 23, 59, 59))
        self.sleep = patch('asyncio.sleep', new_callable=AsyncMock)
        self.sleep.start()
        self.addCleanup(self.sleep.stop)

    async def test_woodz_real_notice_has_both_dates_and_seoul_venue(self):
        c = MelonCrawler(self.dr)
        session = MagicMock()
        html = Path(__file__).with_name('fixtures').joinpath('melon_woodz.html').read_text(encoding='utf-8')
        session.get.return_value = response(html)
        item = {'title_tag': BeautifulSoup('<a href="./detail.htm?csoonId=12896">WOODZ</a>', 'html.parser').a,
                'pass_date_check': True, 'open_date': None, 'genre': '콘서트'}
        tickets = await c._fetch_detail(session, item)
        self.assertEqual([(t.open_type, t.open_datetime) for t in tickets],
                         [('선예매', datetime(2026, 10, 6, 20)), ('티켓오픈', datetime(2026, 10, 7, 20))])
        self.assertTrue(all(t.venue == 'KSPO DOME' and t.regions == '서울' for t in tickets))
        c.end = datetime(2026, 10, 6, 23, 59)
        self.assertEqual(len(await c._fetch_detail(session, item)), 1)

    async def test_melon_reaches_fifth_page_after_expired_notices_and_deduplicates_genres(self):
        c = MelonCrawler(self.dr)
        def row(n, date):
            return f'<ul class="list_ticket_cont"><li><a class="tit" href="./detail.htm?csoonId={n}">공연</a><span class="date">{date}</span></li></ul>'
        session = MagicMock()
        session.get.return_value = response('')
        session.post.side_effect = [response(row(n, '2026.09.01 20:00')) for n in range(1, 5)] + [
            response(row(5, '오픈일정 보기')), response(''), response(row(5, '오픈일정 보기')), response(''), response('')]
        items = await c._fetch_list(session)
        self.assertEqual(len(items), 1)
        self.assertIn('csoonId=5', items[0]['title_tag']['href'])
        self.assertEqual(session.post.call_args_list[4].kwargs['data']['pageIndex'], '41')

    async def test_melon_repeated_page_stops(self):
        c = MelonCrawler(self.dr)
        html = '<ul class="list_ticket_cont"><li><a class="tit" href="./detail.htm?csoonId=1">공연</a><span class="date">오픈일정 보기</span></li></ul>'
        session = MagicMock()
        session.get.return_value = response('')
        session.post.return_value = response(html)
        self.assertEqual(len(await c._fetch_list(session)), 1)
        self.assertEqual(session.post.call_count, 6)

    def test_missing_date_does_not_borrow_next_schedule(self):
        soup = BeautifulSoup('<dl class="register_info"><dd class="txt_date">2026.09.17</dd></dl>'
            '<dl class="schedule_info"><dt class="tit_type">선예매</dt><dt class="tit_type">티켓오픈</dt>'
            '<dd class="txt_date">2026년 10월 7일 (수) 20:00</dd></dl>', 'html.parser')
        self.assertEqual(MelonCrawler(self.dr)._parse_open_dates(soup), [('티켓오픈', datetime(2026, 10, 7, 20))])

    async def test_yes24_single_row_sixth_page_is_collected(self):
        c = Yes24Crawler(self.dr)
        def row(n):
            return f'<div class="noti-tbl"><table><tbody><tr><td>티켓오픈</td><td><a href="?id={n}">콘서트</a></td><td>2026.10.06 20:00</td></tr></tbody></table></div>'
        s = MagicMock()
        s.post.side_effect = [response(row(n)) for n in range(1, 7)] + [response('')]
        self.assertEqual(len(await c._fetch_list(s)), 6)
        self.assertEqual(s.post.call_count, 7)

    async def test_sejong_third_page_and_future_general_sale_reach_detail(self):
        c = SejongPac(self.dr)
        def row(n):
            return f'<div class="tbl_list"><table><tbody><tr><td>{n}</td><td><a href="/notice/{n}">공연</a></td><td></td><td>2026-10-12 14:00</td><td></td><td></td></tr></tbody></table></div>'
        s = MagicMock()
        s.get.side_effect = [response(row(n)) for n in range(1, 4)] + [response('')]
        self.assertEqual(len(await c._fetch_list(s)), 3)

    async def test_charlotte_follows_second_page(self):
        c = CharlotteCrawler(self.dr)
        def row(n):
            return f'<table><tbody><tr><td class="left"><a href="view.asp?seq={n}">공연 티켓오픈</a></td><td>2026-09-29</td></tr></tbody></table>'
        s = MagicMock()
        s.get.side_effect = [response(row(1) + '<a href="list.asp?page=2">2</a>'), response(row(2))]
        self.assertEqual(len(await c._fetch_list(s)), 2)
        self.assertEqual(s.get.call_args.kwargs['params']['page'], 2)

    async def test_caci_follows_category_pagination(self):
        c = CaciCrawler(self.dr)
        def data(n, has_next):
            return {'Filter': {'CategoryID': 17, 'PageIndex': n},
                    'Pager': {'HasNextPage': has_next, 'NextPageIndex': n + 1},
                    'ArticleTitles': [{'ArticleID': n, 'CategoryID': 17, 'Title': '공연 티켓오픈', 'DetailsUrl': f'/notice/{n}'}]}
        s = MagicMock()
        s.get.side_effect = [response('data: ' + json.dumps(data(1, True))), response('data: ' + json.dumps(data(2, False)))]
        s.post.return_value = response(data={'Code': 0, 'Tag': 'token'})
        self.assertEqual(len(await c._fetch_list(s)), 2)
        self.assertEqual(json.loads(s.post.call_args.kwargs['data']['value'])['CategoryID'], 17)

    def test_caci_inline_markup_and_separate_date_line(self):
        from crawler.lgart import LGArtCrawler
        c = CaciCrawler(self.dr)
        text = LGArtCrawler._content_text('<p>선예매 : <b>2026년 10월 6일</b> 20:00</p><p>일반예매 :</p><p>2026년 10월 7일 20:00</p>')
        self.assertEqual(c._extract_open_datetimes(text), [('선예매', datetime(2026, 10, 6, 20)), ('일반예매', datetime(2026, 10, 7, 20))])

    async def test_page_limit_warns_instead_of_silent_truncation(self):
        c = Yes24Crawler(self.dr)
        s = MagicMock()
        s.post.return_value = response('<div class="noti-tbl"><table><tbody><tr><td>티켓오픈</td><td><a href="?id=1">공연</a></td><td>2026.10.06 20:00</td></tr></tbody></table></div>')
        with patch('crawler.yes24.MAX_PAGES', 1), self.assertLogs('utils.notice', level='WARNING') as logs:
            self.assertEqual(len(await c._fetch_list(s)), 1)
        self.assertIn('누락', logs.output[0])

    async def test_ticketlink_body_venue_and_presale_inside_range(self):
        c = TicketLinkCrawler(self.dr)
        s = MagicMock()
        s.get.return_value = response(data={'notice': {'title': 'WOODZ 콘서트', 'placeName': '',
            'ticketOpenDatetime': '2026-10-12T20:00:00',
            'content': '<p>공연장 : KSPO DOME</p><p>선예매 : 2026년 10월 6일 (화) 20:00</p><p>일반예매 : 2026년 10월 12일 (월) 20:00</p>'}})
        tickets = await c._fetch_detail(s, {'noticeId': 1})
        self.assertEqual([(t.open_type, t.open_datetime, t.venue) for t in tickets],
                         [('선예매', datetime(2026, 10, 6, 20), 'KSPO DOME')])

    async def test_sac_general_outside_end_still_inspects_presale(self):
        c = SacCrawler(self.dr)
        s = MagicMock()
        s.get.return_value = response(data={'result': 'success', 'paging': {'totalPage': 1, 'result': [
            {'SN': 1, 'TICKET_OPEN_DATE': '2026-10-12T14:00:00'}]}})
        self.assertEqual(len(await c._fetch_list(s)), 1)

    def test_venue_aliases_and_reject_narrative(self):
        for label in ('공연장', '공연 장소', '공 연 장 소', '장소', 'Venue'):
            self.assertEqual(extract_venue(f'- {label} : KSPO DOME'), 'KSPO DOME')
        self.assertIsNone(extract_venue('공연장 문의 : 1234'))
        self.assertIsNone(extract_venue('이 공연은 공연장 : KSPO DOME에서 열립니다'))

    def test_opening_parser_does_not_take_performance_or_closing_dates(self):
        text = '공연일시 : 2026년 10월 6일 20:00\n선예매 마감 : 2026년 10월 7일 20:00\n선예매 : 2026년 10월 6일 오후 8시'
        self.assertEqual(extract_labeled_openings(text), [('선예매', datetime(2026, 10, 6, 20))])


class WindowTests(unittest.TestCase):
    def test_publication_five_calendar_days(self):
        from utils.notice import PublicationWindow
        window = PublicationWindow(datetime(2026, 10, 2))
        self.assertTrue(window.includes('2026.09.28'))
        self.assertTrue(window.includes('2026-10-02'))
        self.assertFalse(window.includes('2026-09-27'))
        self.assertFalse(window.includes('2026-10-03'))
        self.assertTrue(window.includes(None))
        self.assertTrue(window.expired_page([('2026-09-27', False)]))
        self.assertFalse(window.expired_page([('2026-09-27', True)]))
        self.assertFalse(window.expired_page([('2026-09-27', False), (None, False)]))
        self.assertFalse(window.expired_page([('2026-09-27', True), ('2026-09-28', False)]))

    def test_opening_boundaries_and_disordered_page(self):
        from utils.notice import OpeningWindow
        start, end = datetime(2026, 10, 2), datetime(2026, 10, 12)
        window = OpeningWindow(start, end)
        self.assertFalse(window.beyond([start, end]))
        self.assertTrue(window.beyond([datetime(2026, 10, 13)]))
        window = OpeningWindow(start, end, descending=True)
        self.assertFalse(window.beyond([end, start]))
        self.assertTrue(window.beyond([datetime(2026, 10, 1)]))
        window = OpeningWindow(start, end)
        self.assertFalse(window.beyond([None]))
        self.assertFalse(window.beyond([datetime(2026, 10, 14), datetime(2026, 10, 13)]))
