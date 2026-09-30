import unittest

from bs4 import BeautifulSoup
from crawler.ticketlink import TicketLinkCrawler
from utils.utils import extract_cast_from_lines


class TicketLinkCastTests(unittest.TestCase):
    def test_promotion_is_not_a_cast_header(self):
        lines = ['주역배우 캐스팅 수순의 맨 윗선에 우뚝 서있는 배우다.',
                 '배우 김수하는 차세대 뮤지컬 배우 중 독보적인 스타다.',
                 '배우 에녹도 처음으로 시카고에 참여한다.']
        self.assertEqual(extract_cast_from_lines(lines), '-')
        soup = BeautifulSoup('<div>' + ''.join('<p>' + line + '</p>' for line in lines) + '</div>', 'html.parser')
        self.assertEqual(TicketLinkCrawler.extract_cast_from_body(soup), '-')

    def test_real_cast_after_promotion_and_nested_markup(self):
        soup = BeautifulSoup('''<div><p>새로운 캐스팅을 소개합니다.</p>
            <p>배우 김수하가 합류한다.</p><p><strong>[캐스팅]</strong></p>
            <p>벨마 켈리 役 - 최정원, 윤공주, 린아<br>록시 하트 役 - 아이비, 김수하</p>
            <p>[공연정보]</p><p>홍보 문구</p></div>''', 'html.parser')
        cast = TicketLinkCrawler.extract_cast_from_body(soup)
        self.assertIn('최정원', cast)
        self.assertIn('김수하', cast)
        self.assertNotIn('합류', cast)
        self.assertNotIn('홍보', cast)

    def test_inline_cast(self):
        soup = BeautifulSoup('<p>출연진: 최정원, 윤공주, 린아</p>', 'html.parser')
        self.assertEqual(TicketLinkCrawler.extract_cast_from_body(soup), '최정원, 윤공주, 린아')
