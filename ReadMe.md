# 🎫 Ticket Open Schedule Bot

매주 일요일, 인터파크 / 멜론티켓 에서 다음 주 예매 오픈 예정 공연을 수집해 Notion으로 전송합니다.

## ✅ 기능
- 매주 일요일 08:00 자동 실행 (GitHub Actions)
- 공연명, 예매시작일시, 공연기간, 예매처, 출연진 포함
- Notion 페이지에 자동 전송

## 🔐 환경변수
- `NOTION_TOKEN` – Notion Integration 토큰
- `NOTION_PAGE_ID` – Notion Page ID
- `NOTION_DB_ID` - Notion database ID
