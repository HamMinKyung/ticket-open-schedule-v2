"""Conservative performance location identity shared by merge and Notion sync."""
import re
import unicodedata


def location_key(region: str, venue: str) -> tuple[str, str]:
    def normalize(value):
        value = unicodedata.normalize('NFKC', value or '').casefold()
        return re.sub(r'\s+', '', value).strip('-')
    return normalize(region), normalize(venue)
