import json
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from crawler.lgart import LGArtCrawler


def response(data):
    result = MagicMock()
    result.__aenter__ = AsyncMock(return_value=result)
    result.text = AsyncMock(return_value='data: ' + json.dumps(data))
    result.json = AsyncMock(return_value=data)
    return result


class LGArtFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_ticket_button_filter_only_collects_first_page(self):
        crawler = LGArtCrawler((datetime(2026, 1, 1), datetime(2027, 1, 1)))
        session = MagicMock()
        session.get.side_effect = [response({
            'Categories': [{'Name': '티켓', 'Value': 42}],
            'Filter': {'CategoryID': None, 'PageIndex': 1, 'PageSize': 10},
            'ArticleTitles': [],
        }), response({
            'Filter': {'CategoryID': 42, 'PageIndex': 1},
            'Pager': {'HasNextPage': True, 'NextPageIndex': 2},
            'ArticleTitles': [{
                'CategoryID': 42, 'ArticleID': 123, 'Title': '[티켓오픈] 공연',
                'DetailsUrl': '/community/ko/notice/123',
            }],
        })]
        session.post.return_value = response({'Code': 0, 'Tag': 'fresh-token%3d'})
        items = await crawler._fetch_list(session)
        self.assertEqual(len(items), 1)
        self.assertEqual(session.get.call_count, 2)
        session.post.assert_called_once()
        sent = json.loads(session.post.call_args.kwargs['data']['value'])
        self.assertEqual(sent['CategoryID'], 42)
        self.assertEqual(sent['PageIndex'], 1)
        self.assertEqual(session.get.call_args.kwargs['params'], {'q': 'fresh-token='})
        self.assertNotIn('?', crawler.list_url)

    async def test_failed_token_does_not_collect_unfiltered_notices(self):
        crawler = LGArtCrawler((datetime(2026, 1, 1), datetime(2027, 1, 1)))
        session = MagicMock()
        session.get.return_value = response({
            'Categories': [{'Name': '티켓', 'Value': 17}],
            'Filter': {}, 'ArticleTitles': [],
        })
        session.post.return_value = response({'Code': 1})
        with self.assertRaisesRegex(ValueError, '주소 생성 실패'):
            await crawler._fetch_list(session)
        self.assertEqual(session.get.call_count, 1)


class LGArtDateTests(unittest.TestCase):
    def setUp(self):
        self.crawler = LGArtCrawler((datetime(2026, 1, 1), datetime(2027, 12, 31)))

    def test_opening_formats_from_first_page(self):
        cases = [
            ('※ 1차 티켓오픈: 9/30(수) 오후 2시', datetime(2026, 9, 30, 14)),
            ('※ 티켓오픈 일정: 9월 11일(금) 오후 4시', datetime(2026, 9, 11, 16)),
            ('- 티켓 오픈 일시: 8월 25일(화) 오전 11시', datetime(2026, 8, 25, 11)),
            ('티켓오픈 일시: 2026년 8월 19일(수) 낮 12시', datetime(2026, 8, 19, 12)),
            ('※ 티켓오픈 일정: 8월 14일(금) 오전 11시', datetime(2026, 8, 14, 11)),
            ('티켓오픈 일시: 2026년 8월 21일(금) 오후 6시', datetime(2026, 8, 21, 18)),
            ('- 일반예매 : 08월 13일(목) 오후 3시', datetime(2026, 8, 13, 15)),
            ('[티켓 오픈 일정]\n• 일시: 2026.8.6(목) 2pm', datetime(2026, 8, 6, 14)),
            ('- 티켓 오픈 일시: 7월 23일(목) 오후 2시', datetime(2026, 7, 23, 14)),
        ]
        for text, expected in cases:
            with self.subTest(text=text):
                result = self.crawler._extract_open_datetimes(text, datetime(2026, 7, 1))
                self.assertEqual([dt for _, dt in result], [expected])
                self.assertEqual(result[0][0], '일반예매')

    def test_presale_and_general_sale_exclude_closing_and_suspension(self):
        text = ('[ 티켓 오픈 ]\n'
                '- 패키지 구매자 선오픈: 2026.9.8(화) 2pm ~ 9.10(목) 1pm\n'
                '- 일반 회원 오픈: 2026.9.10(목) 2pm\n'
                '※ 일반 회원 티켓 오픈 준비를 위해 9.10(목) 1pm~2pm에는 예매가 중단됩니다.\n'
                '[ 공연정보 ]\n- 기간: 2026.11.20(금)-29(일)')
        self.assertEqual(self.crawler._extract_open_datetimes(text, datetime(2026, 9, 2)), [
            ('선예매', datetime(2026, 9, 8, 14)), ('일반예매', datetime(2026, 9, 10, 14))])
        self.crawler.start = datetime(2026, 9, 9)
        self.assertEqual(self.crawler._extract_open_datetimes(text, datetime(2026, 9, 2)), [
            ('일반예매', datetime(2026, 9, 10, 14))])

    def test_performance_dates_cannot_become_opening(self):
        text = ('[티켓 오픈 일정]\n추후 안내\n[공연 정보]\n'
                '- 공연일정: 2026년 12월 5일(토) ~ 2027년 3월 14일(일)\n'
                '- 공연시간: 19:30\n※ 티켓오픈 공연 기간: 2026.12.5(토) 14:00')
        self.assertEqual(self.crawler._extract_open_datetimes(text, datetime(2026, 9, 1)), [])
        self.assertEqual(self.crawler._performance_period(text), '2026년 12월 5일(토) ~ 2027년 3월 14일(일)')

    def test_invalid_dates_missing_time_and_year_rollover(self):
        parse = self.crawler._parse_korean_datetime
        self.assertIsNone(parse('2026.2.30(월) 오후 2시'))
        self.assertIsNone(parse('2026.9.8 ~ 9.10 오후 2시'))
        self.assertEqual(parse('1/2(토) 12am', datetime(2026, 12, 20)), datetime(2027, 1, 2))
        self.assertEqual(parse('2026.9.8(화) 2:30pm'), datetime(2026, 9, 8, 14, 30))

    def test_inline_markup_and_performance_section(self):
        text = self.crawler._content_text('<p>티켓오픈: <b>2026.9.8(화)</b> <span>2pm</span></p><p>• 공연일시: 2026.10.23 ~ 10.25</p>')
        self.assertEqual(self.crawler._extract_open_datetimes(text, datetime(2026, 9, 1)), [('일반예매', datetime(2026, 9, 8, 14))])
        self.assertEqual(self.crawler._performance_period(text), '2026.10.23 ~ 10.25')
        self.assertEqual(self.crawler._performance_period('[티켓 오픈 일정]\n• 일시: 2026.8.6(목) 2pm'), '-')
        self.assertEqual(self.crawler._extract_cast('※ 캐스팅 스케줄은 별도 공지됩니다.\n[공연소개]\n소개글\n[캐스팅]\n배우 A\n[기획사정보]\n회사'), '배우 A')

    def test_notice_suffixes_are_removed_from_titles(self):
        cases = {
            '[티켓오픈] 뮤지컬 <시카고> 1차 티켓오픈 안내': '뮤지컬 <시카고>',
            '시카고 1차': '시카고',
            '눈, 눈, 눈 — 3층 추가 좌석': '눈, 눈, 눈',
            "[티켓오픈] 이자람 판소리 '눈, 눈, 눈' 3층 좌석 추가 오픈 안내": "이자람 판소리 '눈, 눈, 눈'",
            "[티켓오픈] '피아노 피아노' 발코니석(우측) 추가 오픈 안내": "'피아노 피아노'",
            '뮤지컬 <드라큘라> 마지막 티켓오픈 안내': '뮤지컬 <드라큘라>',
            '뮤지컬 <3층의 사람들>': '뮤지컬 <3층의 사람들>',
        }
        for original, expected in cases.items():
            with self.subTest(title=original):
                self.assertEqual(self.crawler._strip_notice_title(original), expected)
