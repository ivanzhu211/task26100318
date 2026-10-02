"""短信营销末次触达归因库（SQLite 持久化，增量维护归因与活动汇总）。"""
from __future__ import annotations

import sqlite3
from typing import Optional, TypedDict

WINDOW_SECONDS = 604800  # 7 天归因窗口

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages(
  message_id TEXT PRIMARY KEY,
  contact_id TEXT,
  campaign_id TEXT,
  delivered_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_contact_delivered
  ON messages(contact_id, delivered_at, message_id);
CREATE TABLE IF NOT EXISTS orders(
  order_id TEXT PRIMARY KEY,
  contact_id TEXT NOT NULL,
  at INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  message_id TEXT,
  campaign_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_orders_contact_at ON orders(contact_id, at);
CREATE TABLE IF NOT EXISTS campaign_contact(
  campaign_id TEXT NOT NULL,
  contact_id TEXT NOT NULL,
  order_count INTEGER NOT NULL,
  PRIMARY KEY(campaign_id, contact_id)
);
CREATE TABLE IF NOT EXISTS campaign_stats(
  campaign_id TEXT PRIMARY KEY,
  revenue INTEGER NOT NULL,
  conversions INTEGER NOT NULL
);
"""


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
    amount: int  # USD 整数分


Event = OutboundEvent | DeliveredEvent | PurchaseEvent


class CampaignStats(TypedDict):
    revenue: int
    conversions: int


class Attribution(TypedDict):
    message_id: str
    campaign_id: str


class AttributionDB:
    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)
        self._conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self._conn.close()

    # ---- 写入路径 ----

    def ingest(self, event: Event) -> None:
        kind = event["type"]
        with self._conn:
            if kind == "outbound":
                self._ingest_outbound(event)  # type: ignore[arg-type]
            elif kind == "delivered":
                self._ingest_delivered(event)  # type: ignore[arg-type]
            elif kind == "purchase":
                self._ingest_purchase(event)  # type: ignore[arg-type]
            else:
                raise ValueError(f"unknown event type: {kind!r}")

    def _ingest_outbound(self, event: OutboundEvent) -> None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO messages(message_id, contact_id, campaign_id)"
            " VALUES (?, ?, ?)",
            (event["message_id"], event["contact_id"], event["campaign_id"]),
        )
        if cur.rowcount == 0:
            row = self._conn.execute(
                "SELECT contact_id FROM messages WHERE message_id = ?",
                (event["message_id"],),
            ).fetchone()
            if row[0] is not None:
                return  # 重复投递，内容相同，直接忽略
            self._conn.execute(
                "UPDATE messages SET contact_id = ?, campaign_id = ?"
                " WHERE message_id = ?",
                (event["contact_id"], event["campaign_id"], event["message_id"]),
            )
        self._maybe_activate(event["message_id"])

    def _ingest_delivered(self, event: DeliveredEvent) -> None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO messages(message_id, delivered_at) VALUES (?, ?)",
            (event["message_id"], event["at"]),
        )
        if cur.rowcount == 0:
            row = self._conn.execute(
                "SELECT delivered_at FROM messages WHERE message_id = ?",
                (event["message_id"],),
            ).fetchone()
            if row[0] is not None:
                return  # 重复回执，忽略
            self._conn.execute(
                "UPDATE messages SET delivered_at = ? WHERE message_id = ?",
                (event["at"], event["message_id"]),
            )
        self._maybe_activate(event["message_id"])

    def _maybe_activate(self, message_id: str) -> None:
        row = self._conn.execute(
            "SELECT contact_id, delivered_at FROM messages WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        if row[0] is None or row[1] is None:
            return  # outbound/delivered 尚未凑齐，暂不参与归因
        self._on_message_activated(message_id, row[0], row[1])

    def _on_message_activated(
        self, message_id: str, contact_id: str, delivered_at: int
    ) -> None:
        # 新可归因消息只可能影响其 7 天窗口内的订单；按 (contact_id, at) 索引范围扫描，
        # 不触碰该联系人窗口之外的历史。
        rows = self._conn.execute(
            "SELECT order_id, at, amount, message_id, campaign_id FROM orders"
            " WHERE contact_id = ? AND at >= ? AND at <= ?",
            (contact_id, delivered_at, delivered_at + WINDOW_SECONDS),
        ).fetchall()
        for order_id, at, amount, old_msg, old_camp in rows:
            best = self._best_message_for(contact_id, at)
            new_msg, new_camp = best if best else (None, None)
            if new_msg == old_msg:
                continue
            self._apply_attribution(
                order_id, contact_id, amount, old_camp, new_msg, new_camp
            )

    def _ingest_purchase(self, event: PurchaseEvent) -> None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO orders(order_id, contact_id, at, amount)"
            " VALUES (?, ?, ?, ?)",
            (event["order_id"], event["contact_id"], event["at"], event["amount"]),
        )
        if cur.rowcount == 0:
            return  # 重复投递，内容相同，直接忽略
        best = self._best_message_for(event["contact_id"], event["at"])
        new_msg, new_camp = best if best else (None, None)
        self._apply_attribution(
            event["order_id"], event["contact_id"], event["amount"],
            None, new_msg, new_camp,
        )

    def _apply_attribution(
        self,
        order_id: str,
        contact_id: str,
        amount: int,
        old_camp: Optional[str],
        new_msg: Optional[str],
        new_camp: Optional[str],
    ) -> None:
        self._conn.execute(
            "UPDATE orders SET message_id = ?, campaign_id = ? WHERE order_id = ?",
            (new_msg, new_camp, order_id),
        )
        if old_camp is not None:
            self._remove_from_campaign(old_camp, contact_id, amount)
        if new_camp is not None:
            self._add_to_campaign(new_camp, contact_id, amount)

    def _add_to_campaign(self, campaign_id: str, contact_id: str, amount: int) -> None:
        cur = self._conn.execute(
            "INSERT OR IGNORE INTO campaign_contact(campaign_id, contact_id, order_count)"
            " VALUES (?, ?, 0)",
            (campaign_id, contact_id),
        )
        is_new_contact = cur.rowcount == 1
        self._conn.execute(
            "UPDATE campaign_contact SET order_count = order_count + 1"
            " WHERE campaign_id = ? AND contact_id = ?",
            (campaign_id, contact_id),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO campaign_stats(campaign_id, revenue, conversions)"
            " VALUES (?, 0, 0)",
            (campaign_id,),
        )
        self._conn.execute(
            "UPDATE campaign_stats SET revenue = revenue + ?,"
            " conversions = conversions + ? WHERE campaign_id = ?",
            (amount, 1 if is_new_contact else 0, campaign_id),
        )

    def _remove_from_campaign(
        self, campaign_id: str, contact_id: str, amount: int
    ) -> None:
        self._conn.execute(
            "UPDATE campaign_contact SET order_count = order_count - 1"
            " WHERE campaign_id = ? AND contact_id = ?",
            (campaign_id, contact_id),
        )
        cur = self._conn.execute(
            "DELETE FROM campaign_contact"
            " WHERE campaign_id = ? AND contact_id = ? AND order_count = 0",
            (campaign_id, contact_id),
        )
        contact_left = cur.rowcount == 1
        self._conn.execute(
            "UPDATE campaign_stats SET revenue = revenue - ?,"
            " conversions = conversions - ? WHERE campaign_id = ?",
            (amount, 1 if contact_left else 0, campaign_id),
        )

    # ---- 归因候选查询 ----

    def _best_message_for(self, contact_id: str, at: int) -> Optional[tuple[str, str]]:
        """返回 (message_id, campaign_id)：送达最晚、并列取 message_id 字典序最大。"""
        row = self._conn.execute(
            "SELECT message_id, campaign_id FROM messages"
            " WHERE contact_id = ? AND delivered_at IS NOT NULL"
            " AND delivered_at <= ? AND delivered_at >= ?"
            " ORDER BY delivered_at DESC, message_id DESC LIMIT 1",
            (contact_id, at, at - WINDOW_SECONDS),
        ).fetchone()
        return (row[0], row[1]) if row else None

    # ---- 查询路径（后续步骤实现） ----

    def campaign_stats(self, campaign_id: str) -> CampaignStats:
        row = self._conn.execute(
            "SELECT revenue, conversions FROM campaign_stats WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return {"revenue": 0, "conversions": 0}
        return {"revenue": row[0], "conversions": row[1]}

    def explain(self, order_id: str) -> Optional[Attribution]:
        row = self._conn.execute(
            "SELECT message_id, campaign_id FROM orders WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return {"message_id": row[0], "campaign_id": row[1]}
