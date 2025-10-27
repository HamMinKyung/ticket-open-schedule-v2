# 🎫 Ticket Open Schedule Bot

매주 일요일/수요일, 인터파크 / 멜론티켓 에서 다음 주 예매 오픈 예정 공연을 수집해 Notion으로 전송합니다.

## ✅ 기능
- 매주 일요일 08:00 자동 실행 (GitHub Actions)
- 놀티켓, 멜론티켓, 세종문화회관 티켓, 예술의 전당 티켓 예매 오픈 예정 공연 수집
- 공연명, 예매시작일시, 공연기간, 예매처, 출연진 포함
- Notion 페이지에 자동 전송

## 🔐 환경변수
- `NOTION_TOKEN` – Notion Integration 토큰
- `NOTION_PAGE_ID` – Notion Page ID
- `NOTION_DB_ID` - Notion database ID
- `NOTION_ACT_DB_ID` -  Notion follow 배우 database ID
- `GB_ICAL_DIR` - 공연 일정 iCal 파일 저장 디렉토리
- `GB_ICAL_URL` - 공연 일정 iCal 파일 URL
- `GB_BRANCH` - GitHub Branch 이름
