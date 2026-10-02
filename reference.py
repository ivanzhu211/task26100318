"""Brute-force reference implementation used only by tests.

Recomputes attribution from the full event set on every query, with no
indexes and no incremental state beyond the raw event log.
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

WINDOW_SECONDS = 604800


class ReferenceAttribution:
    def __init__(self) -> None:
        self._outbound: Dict[str, Tuple[str, str]] = {}
        self._delivered: Dict[str, int] = {}
        self._purchases: Dict[str, Tuple[str, int, int]] = {}

    def ingest(self, event: dict) -> None:
        kind = event["type"]
        if kind == "outbound":
            self._outbound.setdefault(
                event["message_id"], (event["contact_id"], event["campaign_id"])
            )
        elif kind == "delivered":
            self._delivered.setdefault(event["message_id"], event["at"])
        elif kind == "purchase":
            self._purchases.setdefault(
                event["order_id"],
                (event["contact_id"], event["at"], event["amount"]),
            )
        else:
            raise ValueError(f"unknown event type: {kind!r}")

    def _qualified(self) -> Dict[str, Tuple[str, str, int]]:
        # message_id -> (contact_id, campaign_id, delivered_at)
        return {
            mid: (self._outbound[mid][0], self._outbound[mid][1], at)
            for mid, at in self._delivered.items()
            if mid in self._outbound
        }

    def _attribute_all(self) -> Dict[str, Tuple[str, str]]:
        messages = self._qualified()
        result: Dict[str, Tuple[str, str]] = {}
        for order_id, (contact_id, at, _amount) in self._purchases.items():
            best_mid: Optional[str] = None
            best_key: Optional[Tuple[int, str]] = None
            for mid, (cid, _camp, delivered_at) in messages.items():
                if cid != contact_id:
                    continue
                if not (at - WINDOW_SECONDS <= delivered_at <= at):
                    continue
                key = (delivered_at, mid)
                if best_key is None or key > best_key:
                    best_key = key
                    best_mid = mid
            if best_mid is not None:
                result[order_id] = (best_mid, messages[best_mid][1])
        return result

    def campaign_stats(self, campaign_id: str) -> Tuple[int, int]:
        attributed = self._attribute_all()
        revenue = 0
        contacts = set()
        for order_id, (_mid, camp) in attributed.items():
            if camp != campaign_id:
                continue
            contact_id, _at, amount = self._purchases[order_id]
            revenue += amount
            contacts.add(contact_id)
        return (revenue, len(contacts))

    def explain(self, order_id: str) -> Optional[Tuple[str, str]]:
        return self._attribute_all().get(order_id)
