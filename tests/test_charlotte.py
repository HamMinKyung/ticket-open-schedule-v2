import json
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from crawler.charlotte import CharlotteCrawler
from merge.merge import merge_ticket_sources


def response(html):
    result = MagicMock()
    result.__aenter__ = AsyncMock(return_value=result)
    result.text = AsyncMock(return_value=html)
    return result


class CharlotteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.crawler = CharlotteCrawler((datetime(2026, 1, 1), datetime(2026, 12, 31, 23, 59)))
        self.fixtures = Path(__file__).parent / 'fixtures'

    async def test_live_list_fixture_only_first_search_page(self):
        session = MagicMock()
        session.get.return_value = response((self.fixtures / 'charlotte_list.html').read_text(encoding='utf-8'))
        items = await self.crawler._fetch_list(session)
        self.assertEqual(len(items), 10)
        session.get.assert_called_once()
        self.assertEqual(session.get.call_args.kwargs['params'],
                         {'page': 1, 'schType': 'TITLE', 'schWord': '티켓오픈'})
        self.assertEqual(items[0]['published'], datetime(2026, 8, 7))
        self.assertIn('seq=4201', items[0]['detail_url'])

    async def test_live_detail_presale_and_general_without_rental(self):
        session = MagicMock()
        session.get.return_value = response((self.fixtures / 'charlotte_detail.html').read_text(encoding='utf-8'))
        item = {'title': '뮤지컬 <겨울왕국> 한국 초연 - 3차 티켓오픈 안내',
                'published': datetime(2026, 8, 7), 'detail_url': 'https://www.charlottetheater.co.kr/customer/notice/view.asp?seq=4201'}
        tickets = await self.crawler._fetch_detail(session, item)
        self.assertEqual([(t.open_type, t.open_datetime) for t in tickets], [
            ('선예매', datetime(2026, 8, 18, 11)), ('티켓오픈', datetime(2026, 8, 19, 14))])
        self.assertEqual(tickets[0].performance_period, '2026년 8월 13일~2027년 3월 1일')
        self.assertEqual(tickets[0].round_info, '10월 13일(화)~11월 8일(일)')
        self.assertIn('정선아', tickets[0].cast)
        self.assertNotIn('3차', tickets[0].title)

    def test_dates_year_rollover_invalid_and_duplicates(self):
        crawler = CharlotteCrawler((datetime(2026, 12, 31), datetime(2027, 1, 5)))
        text = '티켓오픈: 1월 2일(토) 오후 12시 30분\n티켓오픈: 1월 2일(토) 오후 12시 30분\n티켓오픈: 2월 30일 오전 11시\n공연일정: 1월 3일 오후 2시\n티켓오픈: 2026년 1월 2일 오후 1시'
        self.assertEqual(crawler._extract_open_datetimes(text, datetime(2026, 12, 20)),
                         [('티켓오픈', datetime(2027, 1, 2, 12, 30))])

    async def test_non_ticket_notice_and_duplicate_excluded(self):
        session = MagicMock()
        row = '<tr><td class="left"><a href="view.asp?seq=1">공연 티켓오픈</a></td><td>2026-09-01</td></tr>'
        session.get.return_value = response('<table><tbody>' + row * 2 + row.replace('seq=1', 'seq=2').replace('공연 티켓오픈', '점검 안내') + '</tbody></table>')
        self.assertEqual(len(await self.crawler._fetch_list(session)), 1)

    def test_all_ten_real_notices_have_only_expected_open_dates(self):
        crawler = CharlotteCrawler((datetime.min, datetime.max))
        notices = json.loads((self.fixtures / 'charlotte_notices.json').read_text(encoding='utf-8'))
        expected = {
            '4201': [('선예매', '2026-08-18 11:00'), ('티켓오픈', '2026-08-19 14:00')],
            '4198': [('선예매', '2026-07-01 11:00'), ('티켓오픈', '2026-07-02 14:00')],
            '4192': [('선예매', '2026-05-28 11:00'), ('티켓오픈', '2026-05-29 14:00')],
            '4174': [('선예매', '2026-02-05 14:00')],
            '4169': [('티켓오픈', '2026-01-15 11:00')],
            '4166': [('선예매', '2026-01-08 14:00')],
            '4162': [('선예매', '2025-12-18 14:00')],
            '4157': [('선예매', '2025-11-13 14:00')],
            '4148': [('선예매', '2025-10-16 14:00')],
            '4132': [('선예매', '2025-07-10 14:00'), ('티켓오픈', '2025-07-10 14:00')],
        }
        self.assertEqual(set(notices), set(expected))
        all_tickets = []
        for seq, notice in notices.items():
            with self.subTest(seq=seq):
                item = {**notice, 'published': datetime.fromisoformat(notice['published'])}
                tickets = crawler._parse_notice(notice['text'], item)
                self.assertEqual([(t.open_type, t.open_datetime.strftime('%Y-%m-%d %H:%M')) for t in tickets], expected[seq])
                self.assertNotEqual(tickets[0].round_info, '-')
                self.assertNotEqual(tickets[0].cast, '-')
                self.assertNotIn('기획사', tickets[0].cast)
                if '킹키부츠' in tickets[0].title:
                    self.assertEqual(tickets[0].performance_period, '2025년 12월 17일(수) ~ 2026년 3월 29일(일)')
                    self.assertIn('김호영', tickets[0].cast)
                if seq == '4132':
                    self.assertEqual(tickets[0].title, '뮤지컬 〈브로드웨이 42번가〉')
                    self.assertIn('박칼린', tickets[0].cast)
                all_tickets.extend(tickets)
        merged = merge_ticket_sources(all_tickets)
        self.assertEqual(len(merged), 13)
        self.assertEqual(merged[-1].open_type_all, {'선예매', '티켓오픈'})

    def test_unrelated_dates_do_not_inherit_neighbor_open_labels(self):
        text = '''티켓오픈: 2026년 7월 1일 오후 2시
- 오페라글라스 예매 :
2026년 7월 2일 오후 4시
- 추가오픈공연 :
2026년 7월 3일 오후 7시 공연
※ 선예매 오픈 시스템 점검: 7월 1일 오후 1시 30분 ~ 1시 59분
- 마티네 할인 : 7/10 오후 2시 공연 예매 시 적용
● 할인정보
7월 11일 오후 2시
● 티켓오픈 정보'''
        self.assertEqual(self.crawler._extract_open_datetimes(text, datetime(2026, 6, 1)),
                         [('티켓오픈', datetime(2026, 7, 1, 14))])

    def test_recovered_date_still_obeys_requested_range(self):
        crawler = CharlotteCrawler((datetime(2026, 7, 2), datetime(2026, 7, 2, 23, 59)))
        notices = json.loads((self.fixtures / 'charlotte_notices.json').read_text(encoding='utf-8'))
        self.assertEqual(crawler._extract_open_datetimes(notices['4198']['text'], datetime(2026, 6, 22)),
                         [('티켓오픈', datetime(2026, 7, 2, 14))])
