"""Conservative performance location identity shared by merge and Notion sync."""
import re
import unicodedata


def location_key(region: str, venue: str) -> tuple[str, str]:
    def normalize(value):
        value = unicodedata.normalize('NFKC', value or '').casefold()
        return re.sub(r'[\s,]+', '', value).strip('-')
    return normalize(region), normalize(venue)


def venue_family(venue: str) -> tuple[str, bool]:
    """Recognize only known venue roots; never discard an explicit hall name."""
    for root in ('충무아트센터', '샤롯데씨어터', 'lg아트센터서울', 'lg아트센터'):
        if venue.startswith(root):
            family = 'lg아트센터' if root.startswith('lg아트센터') else root
            return family, venue == root
    return venue, False


def resolve_location(key, candidates):
    """Resolve an unspecified hall only within one title/date/region group."""
    region, venue = key[2]
    family, generic = venue_family(venue)
    if not generic:
        return key[2]
    details = {candidate[2] for candidate in candidates
               if candidate[:2] == key[:2] and candidate[2][0] == region
               and venue_family(candidate[2][1]) == (family, False)}
    return next(iter(details)) if len(details) == 1 else key[2]
