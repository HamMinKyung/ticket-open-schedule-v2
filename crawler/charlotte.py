import re
from datetime import datetime
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.base import AsyncCrawlerBase
from models.ticket import TicketInfo
from utils.config import settings
from utils.utils import extract_cast_from_lines, extract_open_round, extract_open_round_period, normalize_title


class CharlotteCrawler(AsyncCrawlerBase):
    """샤롯데씨어터 제목 '티켓오픈' 검색 결과의 첫 페이지만 수집한다."""

    def __init__(self, date_range):
        super().__init__(date_range)
        self.cfg = settings.CRAWLERS['charlotte']
        self.list_url = self.cfg['base_url'] + self.cfg['list_endpoint']
        self.headers = dict(self.cfg['headers'])

    async def _fetch_list(self, session):
        async with session.get(self.list_url, params=self.cfg['params'], headers=self.headers) as resp:
            resp.raise_for_status()
            soup = BeautifulSoup(await resp.text(), 'html.parser')
        items, seen = [], set()
        for link in soup.select('tbody td.left a[href]'):
            title = link.get_text(' ', strip=True)
            url = urljoin(self.list_url, link['href'])
            parsed = urlparse(url)
            seq = parse_qs(parsed.query).get('seq', [''])[0]
            if (parsed.netloc != urlparse(self.list_url).netloc
                    or parsed.path != '/customer/notice/view.asp'
                    or not seq.isdigit() or seq in seen or '티켓오픈' not in title):
                continue
            row = link.find_parent('tr').get_text(' ', strip=True)
            published = re.search(r'\d{4}-\d{2}-\d{2}', row)
            if not published:
                continue
            seen.add(seq)
            items.append({'title': title, 'detail_url': url,
                          'published': datetime.strptime(published.group(), '%Y-%m-%d')})
        return items

    async def _fetch_detail(self, session, item):
        async with session.get(item['detail_url'], headers=self.headers) as resp:
            resp.raise_for_status()
            soup = BeautifulSoup(await resp.text(), 'html.parser')
        body = soup.select_one('.list_view .view_contents')
        if body is None:
            raise ValueError('샤롯데씨어터 공지 본문을 찾을 수 없습니다')
        # 인라인 태그로 나뉜 날짜는 연결하고 문단/줄바꿈만 보존한다.
        for tag in body.select('script, style'):
            tag.decompose()
        for tag in body.find_all('br'):
            tag.replace_with('\n')
        for tag in body.find_all(['p', 'div', 'li', 'ul']):
            tag.insert_before('\n')
            tag.insert_after('\n')
        text = '\n'.join(' '.join(line.split()) for line in body.get_text().splitlines() if line.strip())
        return self._parse_notice(text, item)

    def _parse_notice(self, text, item):
        raw_title = item['title']
        title = re.sub(r'\s*(?:-\s*)?(?:(?:\d+\s*차|첫|LAST|추가|마지막)\s*)*티켓\s*오픈.*$', '', raw_title, flags=re.I)
        performance, round_period = self._extract_periods(text)
        cast_section = re.search(r'^[●■\s]*(?:출연진|캐스팅|캐스트|CAST)\s*[:：]?\s*\n([\s\S]*?)(?=^[●■]|\Z)', text, re.M | re.I)
        cast = extract_cast_from_lines(['출연진'] + cast_section.group(1).splitlines()) if cast_section else '-'
        return [TicketInfo(
            title=normalize_title(title), open_datetime=dt, open_type=label,
            round_info=round_period or extract_open_round(raw_title) or '-',
            performance_period=performance, cast=cast,
            detail_url=item['detail_url'], category='뮤지컬' if '뮤지컬' in title else '공연',
            venue='샤롯데씨어터', providers={'샤롯데씨어터'}, source='샤롯데씨어터',
            regions='서울', content={'공지': text},
        ) for label, dt in self._extract_open_datetimes(text, item['published'])]

    @staticmethod
    def _extract_periods(text):
        performance, round_period = '-', extract_open_round_period(text)
        section = ''
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if re.match(r'^[●■]', line) or re.match(r'^※\s*.*티켓\s*오픈\s*$', line):
                section = 'open' if re.search(r'티켓\s*오픈', line) else 'performance' if '공연정보' in line else ''
            match = re.match(r'^[-•]?\s*(공연\s*(?:일정|기간|일시)|추가\s*오픈\s*공연)\s*[:：]\s*(.*)$', line)
            if not match:
                continue
            value = match.group(2).strip()
            if not value and i + 1 < len(lines):
                value = lines[i + 1].strip()
            if not re.search(r'\d', value):
                continue
            if section == 'open' or re.search(r'추가\s*오픈', match.group(1)):
                round_period = round_period or value
            elif section == 'performance' and performance == '-':
                performance = value
        return performance, round_period

    def _extract_open_datetimes(self, text, published):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        result, seen = [], set()
        date_pattern = re.compile(
            r'(?:(\d{4})\s*(?:년|[.\-/])\s*)?(\d{1,2})\s*(?:월|[.\-/])\s*'
            r'(\d{1,2})\s*일?\s*(?:\([^)]*\))?\s*'
            r'(오전|오후|낮|밤)?\s*(\d{1,2})(?:\s*시(?:\s*(\d{1,2})\s*분)?|:(\d{2}))')
        for i, line in enumerate(lines):
            match = date_pattern.search(line)
            if not match:
                continue
            excluded = r'오페라|글라스|자막|안경|기간|회차|공연\s*(?:일정|시간)|추가\s*오픈\s*공연|시스템|점검|마감|불가능|할인|공연\s*$'
            label_pattern = r'티켓\s*오픈|선예매|일반\s*예매'
            if re.search(excluded, line):
                continue
            context = line if re.search(label_pattern, line) else ''
            # 별도 날짜 줄에만 라벨을 연결한다. 다른 항목의 날짜를 빌리지 않는다.
            if not context and not line[:match.start()].strip(' -※•') and not line[match.end():].strip(' !.'):
                if i and re.search(label_pattern, lines[i - 1]) and not date_pattern.search(lines[i - 1]):
                    context = lines[i - 1]
                if not context:
                    # 홍보 문구 한 줄을 사이에 둔 '날짜 → 티켓오픈!' 표기.
                    for candidate in lines[i + 1:i + 3]:
                        if re.search(excluded, candidate) or date_pattern.search(candidate) or re.match(r'^[●■※*-]', candidate):
                            break
                        if re.search(label_pattern, candidate):
                            context = candidate
                            break
            if not context or re.search(excluded, context):
                continue
            year, month, day, ampm, hour, minute, colon_minute = match.groups()
            hour = int(hour)
            if ampm and not 1 <= hour <= 12:
                continue
            if ampm in ('오후', '낮', '밤') and hour < 12:
                hour += 12
            elif ampm == '오전' and hour == 12:
                hour = 0
            candidates = []
            for candidate_year in ([int(year)] if year else range(published.year - 1, published.year + 2)):
                try:
                    candidates.append(datetime(candidate_year, int(month), int(day), hour, int(minute or colon_minute or 0)))
                except ValueError:
                    continue
            if not candidates:
                continue
            dt = min(candidates, key=lambda value: abs(value - published))
            label = '선예매' if '선예매' in context else '일반예매' if '일반예매' in context else '티켓오픈'
            key = (label, dt)
            if self.start <= dt <= self.end and key not in seen:
                seen.add(key)
                result.append(key)
        return result
