from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Set

class TicketInfo(BaseModel):
    # alias가 붙은 필드를 field name으로도 허용
    model_config = ConfigDict(populate_by_name=True)

    title: str           = Field(..., alias="공연 제목")
    open_datetime: datetime = Field(..., alias="오픈 일시")
    round_info: str      = Field("-", alias="오픈 회차")
    cast: str            = Field("-", alias="출연진")
    detail_url: str      = Field("-", alias="상세 링크(원본)")
    category: str        = Field("-", alias="구분")
    open_type: str       = Field("-", alias="오픈 타입(원본)")
    open_type_all: Set[str]       = Field(default_factory=set, alias="오픈 타입")
    venue: str           = Field("-", alias="공연 장소")
    providers: Set[str]  = Field(default_factory=set, alias="예매처")
    solo_sale: bool      = Field(False, alias="단독 판매")
    content: dict         = Field("", alias="내용")
    source: str          = Field(..., alias="예매처 구분(원본)")
    detail_url_all: Set[str] = Field(default_factory=set, alias="상세 링크")
    ical_url: str = Field("", alias="등록 링크")
