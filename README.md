# sms-attribution

短信营销末次触达归因库（Python + SQLite）。消息、送达回执、购买确认可以任意
乱序/重复到达，每次 `ingest` 返回后所有查询都等价于对当前事件集合从头计算。

## 接口

```python
from sms_attribution import AttributionDB

db = AttributionDB("attr.db")          # 或 ":memory:"
db.ingest({"type": "outbound",  "message_id": "m1", "contact_id": "c1", "campaign_id": "A"})
db.ingest({"type": "delivered", "message_id": "m1", "at": 1_700_000_000})
db.ingest({"type": "purchase",  "order_id": "o1", "contact_id": "c1",
           "at": 1_700_000_100, "amount": 1299})   # amount 为 USD 整数分
db.campaign_stats("A")                 # {"revenue": 1299, "conversions": 1}
db.explain("o1")                       # {"message_id": "m1", "campaign_id": "A"}
```

## 归因规则

- 消息需同时具备 outbound 与 delivered 才参与归因；回执可先于 outbound 到达。
- 候选：同 contact、`order_at - 604800 <= delivered_at <= order_at`（边界包含）。
- 取送达时间最晚者；并列取 `message_id` ASCII 字典序最大者。
- 活动收入 = 归属订单金额之和；转化人数 = 归属订单的去重 contact 数。

## 实现要点

- `messages(contact_id, delivered_at, message_id)` 索引：单笔订单归因与候选
  查询均为索引范围扫描。
- `orders(contact_id, at)` 索引：迟到消息/回执激活时，只扫描其 7 天窗口内的
  受影响订单，不触碰联系人完整历史。
- `campaign_contact` + `campaign_stats`：收入与去重人数增量维护，
  `campaign_stats` 查询为 O(1)，不扫描订单明细。
- 同类事件按 ID 去重（`INSERT OR IGNORE`），重投为无操作。

## 测试

```bash
python3 -m pytest tests/ -q
```

- 随机乱序 + 重复投递（含回执先到、同秒送达、7 天边界），每次写入后与独立
  全量重算参考实现 `sms_attribution/reference.py` 核对。
- 两笔订单分步从活动 A 迁到 B，验证 A 的转化人数只在最后一笔迁走时减少。
- 热点联系人历史 2k/20k/100k 下追加 500 个不改变归因的迟到事件，用 SQLite
  progress handler 统计 VM 步数，验证处理量不随历史长度增长。
