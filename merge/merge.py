from collections import defaultdict
from datetime import datetime
from typing import List, OrderedDict, Tuple

from models.ticket import TicketInfo
from utils.utils import extract_open_round, normalize_title, normalize_title_for_merge


def _title_score(title: str) -> tuple[int, int, int]:
    category_words = ("뮤지컬", "연극", "콘서트", "클래식", "오페라", "전시", "공연")
    has_category = int(any(word in title for word in category_words))
    has_bracketed_work = int("〈" in title and "〉" in title)
    return has_category, has_bracketed_work, len(title)


def _round_score(round_info: str) -> tuple[int, int]:
    has_open_round = int(bool(extract_open_round(round_info)))
    return has_open_round, len(round_info or "")


def merge_ticket_sources(tickets: List[TicketInfo]) -> List[TicketInfo]:
    merged: "OrderedDict[Tuple[str, datetime], TicketInfo]" = OrderedDict()
    for tk in tickets:
        # 1) source를 providers에 포함
        tk.providers.add(tk.source)
        tk.detail_url_all.add(tk.detail_url)
        tk.open_type_all.add(tk.open_type)

        # 2) title 정규화 및 키 생성
        normalized_title = normalize_title(tk.title)
        merge_title = normalize_title_for_merge(normalized_title)
        tk.title = normalized_title
        key = (merge_title, tk.open_datetime.strftime("%Y-%m-%d %H:%M"))

        if key in merged:
            # 이미 있으면 providers만 합친다
            if _title_score(normalized_title) > _title_score(merged[key].title):
                merged[key].title = normalized_title
            merged[key].providers |= tk.providers
            merged[key].detail_url_all |= tk.detail_url_all
            merged[key].open_type_all |= tk.open_type_all
            if tk.round_info != "-" and _round_score(tk.round_info) > _round_score(merged[key].round_info):
                merged[key].round_info = tk.round_info
            if merged[key].venue == "-" and tk.venue != "-":
                merged[key].venue = tk.venue
            if merged[key].cast == "-" and tk.cast != "-":
                merged[key].cast = tk.cast
            if not merged[key].content and tk.content:
                merged[key].content = tk.content
        else:
            # 처음 보는 조합이면 복제하지 않고 그대로 저장
            merged[key] = tk

    return list(merged.values())
