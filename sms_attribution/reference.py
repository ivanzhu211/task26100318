"""独立的全量重算参考实现：对当前已收到事件集合从头计算，用于测试核对。

刻意采用最朴素的写法（内存保存去重后的事件、每次查询全量重算），
与被测的增量实现保持逻辑独立。
"""
from __future__ import annotations

from typing import Optional

from .attribution import WINDOW_SECONDS, Event, CampaignStats, Attribution


class ReferenceModel:
    def __init__(self) -> None:
        self._outbound: dict[str, tuple[str, str]] = {}
        self._delivered: dict[str, int] = {}
        self._orders: dict[str, tuple[str, int, int]] = {}

    def ingest(self, event: Event) -> None:
        kind = event["type"]
        if kind == "outbound":
            self._outbound.setdefault(
                event["message_id"], (event["contact_id"], event["campaign_id"])
            )
        elif kind == "delivered":
            self._delivered.setdefault(event["message_id"], event["at"])
        elif kind == "purchase":
            self._orders.setdefault(
                event["order_id"],
                (event["contact_id"], event["at"], event["amount"]),
            )
        else:
            raise ValueError(f"unknown event type: {kind!r}")

    def _attributions(self) -> dict[str, Optional[tuple[str, str]]]:
        eligible = [
            (mid, contact, camp, self._delivered[mid])
            for mid, (contact, camp) in self._outbound.items()
            if mid in self._delivered
        ]
        result: dict[str, Optional[tuple[str, str]]] = {}
        for oid, (contact, at, _amount) in self._orders.items():
            candidates = [
                (delivered_at, mid, camp)
                for mid, c, camp, delivered_at in eligible
                if c == contact and at - WINDOW_SECONDS <= delivered_at <= at
            ]
            if candidates:
                _d, mid, camp = max(candidates, key=lambda t: (t[0], t[1]))
                result[oid] = (mid, camp)
            else:
                result[oid] = None
        return result

    def campaign_stats(self, campaign_id: str) -> CampaignStats:
        attrs = self._attributions()
        revenue = 0
        contacts: set[str] = set()
        for oid, attr in attrs.items():
            if attr is not None and attr[1] == campaign_id:
                revenue += self._orders[oid][2]
                contacts.add(self._orders[oid][0])
        return {"revenue": revenue, "conversions": len(contacts)}

    def explain(self, order_id: str) -> Optional[Attribution]:
        if order_id not in self._orders:
            return None
        attr = self._attributions()[order_id]
        if attr is None:
            return None
        return {"message_id": attr[0], "campaign_id": attr[1]}
