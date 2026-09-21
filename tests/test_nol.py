import unittest
from datetime import datetime

from crawler.nol import NolCrawler


def notice(notice_id=1, **changes):
    return {
        "id": notice_id,
        "title": "테스트 공연",
        "goods_code": "26000001",
        "venue_name": "예술의전당",
        "goods_region_name": "서울",
        "goods_genre_name": "뮤지컬",
        "goods_start_date": "2026-10-01",
        "goods_end_date": "2026-10-31",
        "goods_info": "공연 안내<br>상세 내용",
        "casting_info": "배우 A<br>배우 B",
        "goods_seat_type": 1,
        "ticket_dates": [
            {"ticket_open_date": "2026-09-15T14:00:00", "ticket_open_type_name": "일반 예매"},
        ],
        **changes,
    }


class Response:
    def __init__(self, data):
        self.data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        pass

    async def json(self):
        return self.data


class Session:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.payloads = []

    def post(self, url, json):
        self.payloads.append(json)
        return Response(next(self.pages))


class NolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.crawler = NolCrawler((datetime(2026, 9, 15), datetime(2026, 9, 22, 23, 59)))

    async def test_maps_api_notice_without_requesting_detail(self):
        tickets = await self.crawler._fetch_detail(None, notice())
        self.assertEqual(len(tickets), 1)
        ticket = tickets[0]
        self.assertEqual(ticket.detail_url, "https://nol.yanolja.com/ticket/products/26000001")
        self.assertEqual(ticket.performance_period, "2026-10-01 ~ 2026-10-31")
        self.assertEqual(ticket.content["공연정보"], "공연 안내\n상세 내용")
        self.assertEqual(ticket.providers, {"놀티켓"})
        self.assertTrue(ticket.solo_sale)

    async def test_body_presale_is_separate_from_general_sale(self):
        item = notice(ticket_dates=[
            {"ticket_open_date": "2026-09-22T12:00:00", "ticket_open_type_name": "일반 예매"},
        ], goods_info=("아티스트 선예매: 2026년 9월 21일 (월) 오후 12시 ~ 오후 3시<br>"
                       "일반 예매: 2026년 9월 22일 (화) 오후 12시 ~"))
        tickets = await self.crawler._fetch_detail(None, item)
        self.assertEqual(len(tickets), 2)
        general, presale = tickets
        self.assertEqual(general.round_info, "-")
        self.assertEqual(presale.open_type, "아티스트 선예매")
        self.assertEqual(presale.open_datetime, datetime(2026, 9, 21, 12))
        self.assertEqual(presale.round_info, "선예매")
        self.crawler.start = datetime(2026, 9, 22)
        self.assertEqual(len(self.crawler._schedules(item)), 1)

    async def test_body_only_presale_keeps_notice_in_list(self):
        item = notice(ticket_dates=[], goods_info="아티스트 선예매: 2026년 9월 21일 (월) 오전 12시 ~ 오후 3시")
        items = await self.crawler._fetch_list(Session([{'notices': [item], 'summary': {}}]))
        self.assertEqual(len(items), 1)
        self.assertEqual(self.crawler._schedules(item)[0][1], datetime(2026, 9, 21))

    async def test_multiple_dates_filter_invalid_and_duplicate_entries(self):
        dates = [
            {"ticket_open_date": "2026-09-15T05:00:00Z", "ticket_other_open_name": "팬클럽 선예매"},
            {"ticket_open_date": "2026-09-15T14:00:00", "ticket_other_open_name": "팬클럽 선예매"},
            {"ticket_open_date": "2026-09-17T20:00:00", "ticket_open_type_name": "일반 예매"},
            {"ticket_open_date": "2026-09-14T20:00:00"},
            {"ticket_open_date": "2026-09-23T20:00:00"},
            {"ticket_open_date": "추후공지"},
            {"ticket_open_date": None},
            None,
        ]
        tickets = await self.crawler._fetch_detail(None, notice(ticket_dates=dates))
        self.assertEqual([t.open_type for t in tickets], ["팬클럽 선예매", "일반 예매"])
        self.assertEqual(tickets[0].open_datetime, datetime(2026, 9, 15, 14))

    async def test_missing_dates_or_product_is_skipped(self):
        for changes in ({"ticket_dates": None}, {"goods_code": ""}):
            self.assertEqual(await self.crawler._fetch_detail(None, notice(**changes)), [])

    async def test_unsupported_region_is_skipped(self):
        self.assertEqual(await self.crawler._fetch_detail(None, notice(
            venue_name="대구 공연장", goods_region_name="대구",
        )), [])

    async def test_provider_region_does_not_override_actual_location(self):
        self.assertEqual(await self.crawler._fetch_detail(None, notice(
            venue_name="인스파이어 아레나", goods_region_name="서울",
        )), [])

    async def test_address_disambiguates_gwangju(self):
        tickets = await self.crawler._fetch_detail(None, notice(
            venue_name="광주 공연장", venue_address="경기도 광주시", goods_region_name="서울",
        ))
        self.assertEqual(tickets[0].regions, "경기")

    async def test_pagination_and_notice_deduplication(self):
        session = Session([
            {"notices": [notice()], "summary": {"next_cursor": "next"}},
            {"notices": [notice(), notice(2)], "summary": {"next_cursor": None}},
        ])
        items = await self.crawler._fetch_list(session)
        self.assertEqual([i["id"] for i in items], [1, 2])
        self.assertEqual(session.payloads, [{"sort": "open"}, {"sort": "open", "cursor": "next"}])

    async def test_repeated_cursor_is_reported(self):
        page = {"notices": [notice()], "summary": {"next_cursor": "same"}}
        with self.assertRaises(ValueError):
            await self.crawler._fetch_list(Session([page, page]))

    async def test_changed_response_schema_is_reported(self):
        with self.assertRaises(ValueError):
            await self.crawler._fetch_list(Session([{"error": "invalid"}]))


if __name__ == "__main__":
    unittest.main()
