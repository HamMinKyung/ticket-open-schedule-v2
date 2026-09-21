import unittest
from datetime import datetime
from models.ticket import TicketInfo
from merge.merge import merge_ticket_sources


def ticket(venue, region='경기', **changes):
    return TicketInfo(title='가족뮤지컬 〈산타와 루돌프〉', open_datetime=datetime(2026, 9, 22, 14),
                      venue=venue, regions=region, source='티켓링크', **changes)


class MergeLocationTests(unittest.TestCase):
    def test_same_province_different_venues_stay_separate(self):
        merged = merge_ticket_sources([ticket('안산문화예술의전당', cast='안산 배우', detail_url='https://example.com/ansan'),
                                       ticket('수원SK아트리움', detail_url='https://example.com/suwon')])
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[1].cast, '-')
        self.assertEqual(merged[0].detail_url_all, {'https://example.com/ansan'})

    def test_different_regions_stay_separate_without_venue(self):
        self.assertEqual(len(merge_ticket_sources([ticket('-', '서울'), ticket('-', '부산')])), 2)

    def test_same_venue_with_spacing_merges_providers(self):
        first, second = ticket('수원 SK 아트리움', detail_url='https://example.com/a'), ticket('수원SK아트리움', detail_url='https://example.com/b')
        second.source = 'YES24'
        merged = merge_ticket_sources([first, second])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].providers, {'티켓링크', 'YES24'})

    def test_cast_only_propagates_within_same_location(self):
        one, two, three = ticket('안산', cast='배우 A'), ticket('안산'), ticket('수원')
        two.open_datetime = datetime(2026, 9, 23, 14)
        merged = merge_ticket_sources([one, two, three])
        self.assertEqual([t.cast for t in merged], ['배우 A', '배우 A', '-'])
