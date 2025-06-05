from pydantic_settings import BaseSettings, SettingsConfigDict
from zoneinfo import ZoneInfo
from datetime import timezone, timedelta, datetime
from typing import Dict, Any


class Settings(BaseSettings):
    NOTION_TOKEN: str
    NOTION_DB_ID: str
    NOTION_ACT_DB_ID: str
    NOTION_PAGE_ID: str
    USER_AGENT: str = "Mozilla/5.0"
    HTTP_TIMEOUT: int = 10
    ICAL_USERNAME: str
    ICAL_TOKEN: str
    ICAL_REPO: str
    GITHUB_ICAL_DIR: str = "ical_exports"
    GITHUB_ICAL_URL: str = "https://hamminkyung.github.io/notion-calendar-ics/"
    GITHUB_BRANCH: str = "main"


    # .env에 값이 없으면 기본 9시간으로 설정
    TIMEZONE_OFFSET_HOURS: int = 9
    DEFAULT_TIMEZONE: ZoneInfo = ZoneInfo("Asia/Seoul")

    # 크롤러별 설정 일원 관리
    CRAWLERS: Dict[str, Any] = {
        'inter_park': {
            'base_url': 'https://tickets.interpark.com',
            'regions': ['SEOUL', 'GYEONGGI'],
            'list_endpoint': '/contents/api/open-notice/notice-list',
            'params': {
                'goodsGenre': 'ALL',
                'offset': 0,
                'pageSize': 500,
                'sorting': 'OPEN_ASC'
            },
            'detail_endpoint': '/contents/notice/detail/',
            'selectors': {
                'info_title': '.DetailInfo_infoWrap__1BtFi h2',
                'schedule_box': '.DetailBooking_bookingBox__wcWDI',
                'schedule_title': '.DetailBooking_scheduleTitle__REaUd',
                'schedule_date': '.DetailBooking_scheduleDate__4WvwQ'
            },
            'sibling': {
                'name': 'div',
                'class': 'DetailInfo_contents__grsx5',
            },
            'contents': {
                'venue': "공연장소",
                'period': "공연 일시",
                'cast': "캐스팅",
                'performance_info': "공연정보",
                'open_period': "오픈 공연 기간",
                'open_period2': "오픈 회차",
                'datetime': "일 시",
                'period2': "공연일시",
            }
        },
        'melon': {
            'base_url': 'https://ticket.melon.com',
            'list_endpoint': 'https://ticket.melon.com/csoon/ajax/listTicketOpen.htm',
            'Referer': 'https://ticket.melon.com/csoon/index.htm',
            'user_agents': [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/135.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/91.0.4472.114 Safari/537.36',
                # 필요에 따라 추가…
            ],
            'sale_patterns': [
                ('선예매', r'선예매[:]?\s*(\d+년.+?\d{2}:\d{2})'),
                ('티켓오픈', r'티켓오픈[:]?\s*(\d+년.+?\d{2}:\d{2})')
            ],
            'genre_map': {
                'GENRE_CON_ALL': '콘서트',
                'GENRE_ART_ALL': '뮤지컬/연극',
                'GENRE_CLA_ALL': '클래식'
            },
            'pages': [1, 2, 3],
            'detail_selectors': {
                'title': 'p.tit_consert',
                'base_box': 'div.box_concert_time',
                'cast_box': 'div.box_concert_info',
                'schedule_dt': 'dt.tit_type',
                'schedule_dd': 'dd.txt_date',
                'content_wrap': 'wrap_detailview_cont',
                'solo_icon': 'span.ico_list1'
            }
        },
        'sejong_pac': {
            'base_url': "https://www.sejongpac.or.kr",
            'list_endpoint': "https://www.sejongpac.or.kr/portal/bbs/B0000049/list.do",
            "params": {
                "menuNo": "200440",
                "pageIndex": "1",
            },
            "pages": [1, 2],

        },
        'sac': {
            'base_url': "https://www.sac.or.kr",
            'list_endpoint': "/site/main/show/dataTicketList",
            "params" : {
                "pageSize": 10,
                "ticketOpenFlag": "Y",
                "sortOrder": "B.TICKET_OPEN_DATE",
                "sortDirection": "DESC",
            },
            "detail_endpoint" :"/site/main/show/show_view?SN="
        }
    }

    class Config:
        env_file = ".env"

    @property
    def user_timezone(self) -> timezone:
        return timezone(timedelta(hours=self.TIMEZONE_OFFSET_HOURS))

    @property
    def current_year(self) -> int:
        return datetime.now(self.user_timezone).year


settings = Settings()
