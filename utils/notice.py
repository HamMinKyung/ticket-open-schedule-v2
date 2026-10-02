"""Shared notice field extraction and bounded pagination."""
import logging
import re

logger = logging.getLogger(__name__)
MAX_PAGES = 100


class PageGuard:
    def __init__(self, source):
        self.source = source
        self.seen = set()

    def accept(self, keys):
        keys = {key for key in keys if key is not None and key != ""}
        if not keys:
            return False
        if keys <= self.seen:
            logger.warning('[%s] 반복 목록 감지: 페이지 탐색 중단', self.source)
            return False
        self.seen.update(keys)
        return True

    def limit(self):
        logger.warning('[%s] 목록 %s페이지 제한 도달: 일부 공지가 누락될 수 있습니다', self.source, MAX_PAGES)


def extract_venue(text):
    # Match field labels, never a venue name mentioned in narrative text.
    match = re.search(r'(?im)^[ \t]*[-•·※]?[ \t]*(?:공\s*연\s*장(?:\s*소)?|장\s*소|venue)[ \t]*[:：][ \t]*([^\n\r]+)', text or '')
    return match.group(1).strip() if match else None


def extract_labeled_openings(text):
    """Only explicit, year-qualified booking fields; never performance dates."""
    from datetime import datetime
    result = []
    pattern = re.compile(
        r'(?im)^[ \t]*[-•·※]?[ \t]*(?P<label>[^\n:：]*?(?:선\s*예매|일반\s*예매|티켓\s*오픈))'
        r'(?:[ \t]*(?:일시|일정|기간))?[ \t]*[:：][ \t]*(?:\n[ \t]*)?'
        r'(?P<y>\d{4})[ \t]*[년./-][ \t]*(?P<m>\d{1,2})[ \t]*[월./-][ \t]*(?P<d>\d{1,2})[일.]?'
        r'[ \t]*(?:\([^)]*\))?[ \t]*(?P<ampm>오전|오후)?[ \t]*(?P<h>\d{1,2})'
        r'(?:[:：](?P<minute>\d{2})|시(?:[ \t]*(?P<kor_minute>\d{1,2})분)?)'
    )
    for match in pattern.finditer(text or ''):
        label = match['label'].strip()
        if any(word in label for word in ('마감', '종료', '인증', '취소')):
            continue
        hour = int(match['h'])
        if match['ampm']:
            if not 1 <= hour <= 12:
                continue
            hour = hour % 12 + (12 if match['ampm'] == '오후' else 0)
        try:
            dt = datetime(int(match['y']), int(match['m']), int(match['d']), hour,
                          int(match['minute'] or match['kor_minute'] or 0))
        except ValueError:
            continue
        entry = (label, dt)
        if entry not in result:
            result.append(entry)
    return result


class PublicationWindow:
    """Five calendar days including the collection start date; pins do not end paging."""
    def __init__(self, start, days=5):
        from datetime import timedelta
        self.cutoff = start.date() - timedelta(days=days - 1)
        self.end = start.date()

    @staticmethod
    def date(value):
        from datetime import datetime
        match = re.search(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', str(value or ''))
        if not match:
            return None
        try:
            return datetime(*map(int, match.groups())).date()
        except ValueError:
            return None

    def includes(self, value):
        date = self.date(value)
        # Unknown publication dates must not silently discard a notice.
        return date is None or self.cutoff <= date <= self.end

    def expired_page(self, entries):
        dates = [self.date(value) for value, pinned in entries if not pinned]
        return bool(dates) and all(date is not None and date < self.cutoff for date in dates)


class OpeningWindow:
    """Stop only on a fully dated page beyond the requested sorted window."""
    def __init__(self, start, end, descending=False):
        self.start, self.end = start, end
        self.descending = descending
        self.previous = None
        self.ordered = True

    def beyond(self, dates):
        if not dates or any(date is None for date in dates):
            return False
        values = ([self.previous] if self.previous is not None else []) + dates
        for left, right in zip(values, values[1:]):
            if (left < right if self.descending else left > right):
                self.ordered = False
        self.previous = dates[-1]
        if not self.ordered:
            return False
        if self.descending:
            return all(date < self.start for date in dates)
        return all(date > self.end for date in dates)
