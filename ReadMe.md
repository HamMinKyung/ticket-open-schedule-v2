# 🎫 Ticket Open Schedule Bot

매주 일요일/수요일, NOL 티켓 / 멜론티켓 등에서 다음 주 예매 오픈 예정 공연을 수집해 Notion으로 전송합니다.

NOL 티켓은 `crawler/nol.py`의 `NolCrawler`를 사용합니다. 오픈 예정 목록 API의 모든 페이지를 조회하고, 공지에 포함된 선예매·일반예매 일정을 각각 기간 필터링합니다. 공연정보와 출연진도 같은 API 응답에서 추출하며, 상세 링크는 `https://nol.yanolja.com/ticket/products/{goods_code}`입니다. 기존 `crawler/interpark.py`와 설정은 보존되어 있습니다.

## ✅ 기능
- 매주 일요일/수요일 08:00 자동 실행 (GitHub Actions)
- 놀티켓, 멜론티켓, 세종문화회관 티켓, 예술의 전당 티켓 예매 오픈 예정 공연 수집
- 공연명, 예매시작일시, 공연기간, 예매처, 출연진 포함
- Notion 페이지에 자동 전송

## 🔐 환경변수
- `NOTION_TOKEN` – Notion Integration 토큰
- `NOTION_PAGE_ID` – Notion Page ID
- `NOTION_DB_ID` - Notion database ID
- `NOTION_ACT_DB_ID` -  Notion follow 배우 database ID
- `NOTION_TITLE_DB_ID` - Notion follow 작품 database ID
- `GB_ICAL_DIR` - 공연 일정 iCal 파일 저장 디렉토리
- `GB_ICAL_URL` - 공연 일정 iCal 파일 URL
- `GB_BRANCH` - GitHub Branch 이름
