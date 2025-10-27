from collections import defaultdict
from datetime import datetime
from typing import List, OrderedDict, Tuple

from models.ticket import TicketInfo
from utils.utils import normalize_title


def merge_ticket_sources(tickets: List[TicketInfo]) -> List[TicketInfo]:
    merged: "OrderedDict[Tuple[str, datetime], TicketInfo]" = OrderedDict()
    for tk in tickets:
        # 1) source를 providers에 포함
        tk.providers.add(tk.source)
        tk.detail_url_all.add(tk.detail_url)
        tk.open_type_all.add(tk.open_type)

        # 2) title 정규화 및 키 생성
        normalized_title = normalize_title(tk.title)
        tk.title = normalized_title
        key = (normalized_title, tk.open_datetime.strftime("%Y-%m-%d %H:%M"))

        if key in merged:
            # 이미 있으면 providers만 합친다
            merged[key].providers |= tk.providers
            merged[key].detail_url_all |= tk.detail_url_all
            merged[key].open_type_all |= tk.open_type_all
        else:
            # 처음 보는 조합이면 복제하지 않고 그대로 저장
            merged[key] = tk

    return list(merged.values())
