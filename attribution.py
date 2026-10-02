"""Last-touch attribution store backed by SQLite.

Events arrive out of order; every ingest() call leaves the database in a
state equivalent to recomputing all attribution from scratch.
"""

from __future__ import annotations

import sqlite3
from typing import Optional, Tuple, TypedDict, Union

WINDOW_SECONDS = 604800  # 7 days


class OutboundEvent(TypedDict):
    type: str  # "outbound"
    message_id: str
    contact_id: str
    campaign_id: str


class DeliveredEvent(TypedDict):
    type: str  # "delivered"
    message_id: str
    at: int


class PurchaseEvent(TypedDict):
    type: str  # "purchase"
    order_id: str
    contact_id: str
    at: int
    amount: int  # USD cents


Event = Union[OutboundEvent, DeliveredEvent, PurchaseEvent]


class AttributionStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE outbound (
                message_id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL
            );
            CREATE TABLE delivered (
                message_id TEXT PRIMARY KEY,
                at INTEGER NOT NULL
            );
            -- Messages that have both outbound and delivered events and
            -- therefore participate in attribution.
            CREATE TABLE message (
                message_id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                delivered_at INTEGER NOT NULL
            );
            CREATE INDEX idx_message_contact_time
                ON message(contact_id, delivered_at, message_id);
            CREATE TABLE purchase (
                order_id TEXT PRIMARY KEY,
                contact_id TEXT NOT NULL,
                at INTEGER NOT NULL,
                amount INTEGER NOT NULL
            );
            CREATE INDEX idx_purchase_contact_at ON purchase(contact_id, at);
            -- Current attribution decision per order.
            CREATE TABLE attribution (
                order_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                contact_id TEXT NOT NULL,
                amount INTEGER NOT NULL
            );
            -- Number of attributed orders per (campaign, contact); a row
            -- counts as one converted contact while n_orders > 0.
            CREATE TABLE campaign_contact (
                campaign_id TEXT NOT NULL,
                contact_id TEXT NOT NULL,
                n_orders INTEGER NOT NULL,
                PRIMARY KEY (campaign_id, contact_id)
            );
            CREATE TABLE campaign_stat (
                campaign_id TEXT PRIMARY KEY,
                revenue INTEGER NOT NULL,
                conversions INTEGER NOT NULL
            );
            """
        )

    def ingest(self, event: Event) -> None:
        kind = event["type"]
        with self._db:
            if kind == "outbound":
                self._ingest_outbound(event)
            elif kind == "delivered":
                self._ingest_delivered(event)
            elif kind == "purchase":
                self._ingest_purchase(event)
            else:
                raise ValueError(f"unknown event type: {kind!r}")

    # -- ingest helpers --------------------------------------------------

    def _ingest_outbound(self, event: OutboundEvent) -> None:
        cur = self._db.execute(
            "INSERT OR IGNORE INTO outbound(message_id, contact_id, campaign_id)"
            " VALUES (?, ?, ?)",
            (event["message_id"], event["contact_id"], event["campaign_id"]),
        )
        if cur.rowcount == 0:
            return  # duplicate delivery, content identical
        row = self._db.execute(
            "SELECT at FROM delivered WHERE message_id = ?",
            (event["message_id"],),
        ).fetchone()
        if row is not None:
            self._promote_message(event["message_id"], row[0])

    def _ingest_delivered(self, event: DeliveredEvent) -> None:
        cur = self._db.execute(
            "INSERT OR IGNORE INTO delivered(message_id, at) VALUES (?, ?)",
            (event["message_id"], event["at"]),
        )
        if cur.rowcount == 0:
            return  # duplicate delivery, content identical
        row = self._db.execute(
            "SELECT 1 FROM outbound WHERE message_id = ?",
            (event["message_id"],),
        ).fetchone()
        if row is not None:
            self._promote_message(event["message_id"], event["at"])

    def _promote_message(self, message_id: str, delivered_at: int) -> None:
        row = self._db.execute(
            "SELECT contact_id, campaign_id FROM outbound WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        contact_id, campaign_id = row
        self._db.execute(
            "INSERT INTO message(message_id, contact_id, campaign_id,"
            " delivered_at) VALUES (?, ?, ?, ?)",
            (message_id, contact_id, campaign_id, delivered_at),
        )
        # Only purchases in [delivered_at, delivered_at + WINDOW] can be
        # affected by this newly qualified message.
        affected = self._db.execute(
            "SELECT order_id, at, amount FROM purchase"
            " WHERE contact_id = ? AND at BETWEEN ? AND ?",
            (contact_id, delivered_at, delivered_at + WINDOW_SECONDS),
        ).fetchall()
        for order_id, at, amount in affected:
            self._reattribute(order_id, contact_id, at, amount)

    def _ingest_purchase(self, event: PurchaseEvent) -> None:
        cur = self._db.execute(
            "INSERT OR IGNORE INTO purchase(order_id, contact_id, at, amount)"
            " VALUES (?, ?, ?, ?)",
            (event["order_id"], event["contact_id"], event["at"], event["amount"]),
        )
        if cur.rowcount == 0:
            return  # duplicate delivery, content identical
        self._reattribute(
            event["order_id"], event["contact_id"], event["at"], event["amount"]
        )

    # -- attribution core -------------------------------------------------

    def _best_candidate(
        self, contact_id: str, at: int
    ) -> Optional[Tuple[str, str]]:
        return self._db.execute(
            "SELECT message_id, campaign_id FROM message"
            " WHERE contact_id = ? AND delivered_at BETWEEN ? AND ?"
            " ORDER BY delivered_at DESC, message_id DESC LIMIT 1",
            (contact_id, at - WINDOW_SECONDS, at),
        ).fetchone()

    def _reattribute(
        self, order_id: str, contact_id: str, at: int, amount: int
    ) -> None:
        best = self._best_candidate(contact_id, at)
        old = self._db.execute(
            "SELECT message_id, campaign_id FROM attribution WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        old_message = old[0] if old else None
        new_message = best[0] if best else None
        if old_message == new_message:
            return
        if old is not None:
            self._adjust(old[1], contact_id, amount, -1)
            self._db.execute(
                "DELETE FROM attribution WHERE order_id = ?", (order_id,)
            )
        if best is not None:
            self._db.execute(
                "INSERT INTO attribution(order_id, message_id, campaign_id,"
                " contact_id, amount) VALUES (?, ?, ?, ?, ?)",
                (order_id, best[0], best[1], contact_id, amount),
            )
            self._adjust(best[1], contact_id, amount, +1)

    def _adjust(
        self, campaign_id: str, contact_id: str, amount: int, delta: int
    ) -> None:
        self._db.execute(
            "INSERT INTO campaign_stat(campaign_id, revenue, conversions)"
            " VALUES (?, 0, 0) ON CONFLICT(campaign_id) DO NOTHING",
            (campaign_id,),
        )
        self._db.execute(
            "UPDATE campaign_stat SET revenue = revenue + ?"
            " WHERE campaign_id = ?",
            (delta * amount, campaign_id),
        )
        if delta > 0:
            row = self._db.execute(
                "INSERT INTO campaign_contact(campaign_id, contact_id, n_orders)"
                " VALUES (?, ?, 1)"
                " ON CONFLICT(campaign_id, contact_id)"
                " DO UPDATE SET n_orders = n_orders + 1"
                " RETURNING n_orders",
                (campaign_id, contact_id),
            ).fetchone()
            if row[0] == 1:
                self._db.execute(
                    "UPDATE campaign_stat SET conversions = conversions + 1"
                    " WHERE campaign_id = ?",
                    (campaign_id,),
                )
        else:
            row = self._db.execute(
                "UPDATE campaign_contact SET n_orders = n_orders - 1"
                " WHERE campaign_id = ? AND contact_id = ?"
                " RETURNING n_orders",
                (campaign_id, contact_id),
            ).fetchone()
            if row[0] == 0:
                self._db.execute(
                    "DELETE FROM campaign_contact"
                    " WHERE campaign_id = ? AND contact_id = ?",
                    (campaign_id, contact_id),
                )
                self._db.execute(
                    "UPDATE campaign_stat SET conversions = conversions - 1"
                    " WHERE campaign_id = ?",
                    (campaign_id,),
                )

    # -- queries ----------------------------------------------------------

    def campaign_stats(self, campaign_id: str) -> Tuple[int, int]:
        """Return (attributed revenue in cents, distinct converted contacts)."""
        row = self._db.execute(
            "SELECT revenue, conversions FROM campaign_stat WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return (0, 0)
        return (row[0], row[1])

    def explain(self, order_id: str) -> Optional[Tuple[str, str]]:
        """Return (message_id, campaign_id) for the order, or None."""
        return self._db.execute(
            "SELECT message_id, campaign_id FROM attribution WHERE order_id = ?",
            (order_id,),
        ).fetchone()

    def close(self) -> None:
        self._db.close()
