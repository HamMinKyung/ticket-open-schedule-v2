import unittest
from datetime import datetime
from bs4 import BeautifulSoup

from crawler.sejongpac import SejongPac
from crawler.yes24 import Yes24Crawler


class Response:
    def __init__(self, html):
        self.html = html

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    async def text(self):
        return self.html


class Session:
    def __init__(self, html):
        self.html = html

    def get(self, url):
        return Response(self.html)


class PreviewCorrectionTests(unittest.IsolatedAsyncioTestCase):
    def test_yes24_detail_button_product_url(self):
        for href in ('https://ticket.yes24.com/Perf/60292',
                     'javascript:jsf_base_GoToUrl(jsf_base_GetSiteDetailURL(60292));'):
            soup = BeautifulSoup(f'<div class="noti-vt-btns"><a href="{href}">상세보기</a></div>', 'html.parser')
            self.assertEqual(Yes24Crawler._extract_product_url(soup), 'https://ticket.yes24.com/Perf/60292')
        self.assertIsNone(Yes24Crawler._extract_product_url(BeautifulSoup(
            '<div class="noti-vt-btns"><a href="javascript:notify(18527)">알림</a></div>', 'html.parser')))

    def test_yes24_overview_and_narrative_cast(self):
        crawler = Yes24Crawler((datetime(2026, 9, 21), datetime(2026, 9, 28)))
        self.assertEqual(crawler._build_performance_period('일  시 : 2026. 10. 17. (토) 15:00, 19:00'),
                         '2026. 10. 17. (토) 15:00, 19:00')
        self.assertEqual(crawler._extract_cast({'공연 소개': '무대에는 라흐마니노프 역의 원태민과 니콜라이 달 역의 정동화가 출연한다.'}),
                         '원태민, 정동화')
        self.assertIsNone(crawler._extract_cast({'공연 소개': '출연 정보는 추후 공지합니다.'}))

    async def test_sejong_time_is_not_venue(self):
        crawler = SejongPac((datetime(2026, 9, 21), datetime(2026, 9, 28)))
        for place, expected in [('월, 수, 목 7시 30분 / 토, 일 3시', '세종문화회관'),
                                ('세종문화회관 M씨어터', '세종문화회관 M씨어터')]:
            html = ('<table><tr><th>티켓오픈일</th><td><p>2026년 09월 22일 10:00 일반예매</p></td></tr>'
                    f'<tr><th>공연정보</th><td><p>공연장소 : {place}</p></td></tr></table>')
            tickets = await crawler._fetch_detail(Session(html), {'link': 'https://example.com', 'title': '에너미'})
            self.assertEqual(len(tickets), 1)
            self.assertEqual(tickets[0].venue, expected)
            if expected != place:
                self.assertEqual(tickets[0].content['공연시간'], place)
