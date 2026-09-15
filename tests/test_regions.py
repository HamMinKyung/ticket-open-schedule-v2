import unittest

from utils.utils import resolve_region


class RegionTests(unittest.TestCase):
    def test_location_takes_priority_over_title(self):
        self.assertEqual(resolve_region("세종문화회관", "뮤지컬 광주"), "서울")
        self.assertEqual(resolve_region("부산 공연장", "서울의 별"), "부산")
        self.assertIsNone(resolve_region("대전예술의전당", "서울 공연"))

    def test_address_takes_priority_over_venue_and_provider(self):
        self.assertIsNone(resolve_region("서울 공연장", "", "인천광역시 중구", default_region="서울"))
        self.assertEqual(resolve_region("광주 공연장", "", "경기도 광주시", default_region="서울"), "경기")
        self.assertIsNone(resolve_region("광주 공연장", "", "광주광역시", default_region="경기"))

    def test_gyeonggi_cities_and_stadium_word(self):
        for venue in ("의정부예술의전당", "남양주 공연장", "서울랜드"):
            with self.subTest(venue=venue):
                self.assertEqual(resolve_region(venue), "경기")
        self.assertEqual(resolve_region("서울 월드컵경기장"), "서울")
        self.assertIsNone(resolve_region("종합경기장"))

    def test_inspire_is_excluded_even_if_provider_says_seoul(self):
        self.assertIsNone(resolve_region("인스파이어 아레나", default_region="서울"))

    def test_explicit_title_location(self):
        self.assertIsNone(resolve_region("", "공연 - 대전"))
        self.assertEqual(resolve_region("", "[부산] 공연"), "부산")
        self.assertEqual(resolve_region("", "LIVE IN SEOUL"), "서울")
        self.assertIsNone(resolve_region("", "서울의 별"))

    def test_unknown_location_is_not_assumed_to_be_seoul(self):
        self.assertIsNone(resolve_region("미정", "테스트 공연"))
        self.assertEqual(resolve_region("미정", default_region="부산"), "부산")


if __name__ == "__main__":
    unittest.main()
