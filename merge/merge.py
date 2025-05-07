from collections import defaultdict
from datetime import datetime
from typing import List, OrderedDict, Tuple

from models.ticket import TicketInfo


def merge_ticket_sources(tickets: List[TicketInfo]) -> List[TicketInfo]:
    merged: "OrderedDict[Tuple[str, datetime], TicketInfo]" = OrderedDict()
    for tk in tickets:
        # 1) source를 providers에 포함
        tk.providers.add(tk.source)

        # 2) title/open_datetime 기준 키 생성
        key = (tk.title.strip(""), tk.open_datetime.strftime("%Y-%m-%d %H:%M"))

        if key in merged:
            # 이미 있으면 providers만 합친다
            merged[key].providers |= tk.providers
        else:
            # 처음 보는 조합이면 복제하지 않고 그대로 저장
            merged[key] = tk

    return list(merged.values())
