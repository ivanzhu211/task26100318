import random

from sms_attribution import AttributionDB
from sms_attribution.attribution import WINDOW_SECONDS
from sms_attribution.reference import ReferenceModel


def assert_consistent(db, ref, campaign_ids, order_ids):
    attrs = ref._attributions()
    for oid in order_ids:
        expected = attrs.get(oid)
        expected = None if expected is None else {
            "message_id": expected[0], "campaign_id": expected[1],
        }
        assert db.explain(oid) == expected, f"explain({oid})"
    for cid in campaign_ids:
        assert db.campaign_stats(cid) == ref.campaign_stats(cid), f"stats({cid})"


def test_randomized_shuffled_duplicated_events_match_reference():
    rng = random.Random(20261002)
    campaigns = ["A", "B", "C"]
    contacts = [f"c{i}" for i in range(6)]
    events = []
    # 跨活动触达 + 同秒送达（时间量化到 10 秒制造并列）+ 7 天边界
    for i in range(60):
        mid = f"m{i}"
        contact = rng.choice(contacts)
        camp = rng.choice(campaigns)
        delivered_at = rng.randrange(0, 200_000) // 10 * 10
        events.append({"type": "outbound", "message_id": mid,
                       "contact_id": contact, "campaign_id": camp})
        events.append({"type": "delivered", "message_id": mid, "at": delivered_at})
    for i in range(40):
        contact = rng.choice(contacts)
        base = rng.randrange(0, 200_000)
        # 一部分订单精确落在窗口边界上
        offset = rng.choice([0, 1, WINDOW_SECONDS - 1, WINDOW_SECONDS,
                             WINDOW_SECONDS + 1, rng.randrange(0, 50_000)])
        events.append({"type": "purchase", "order_id": f"o{i}", "contact_id": contact,
                       "at": base + offset, "amount": rng.randrange(1, 10_000)})
    # 重复投递（内容相同）
    duplicated = []
    for ev in events:
        duplicated.append(ev)
        if rng.random() < 0.3:
            duplicated.append(dict(ev))
    rng.shuffle(duplicated)  # 乱序：回执可能先于 outbound 到达

    db = AttributionDB()
    ref = ReferenceModel()
    order_ids = [f"o{i}" for i in range(40)]
    for ev in duplicated:
        db.ingest(ev)
        ref.ingest(ev)
        assert_consistent(db, ref, campaigns, order_ids)  # 每次写入后核对


def test_two_orders_migrate_one_by_one_and_conversions_drop_once():
    db = AttributionDB()
    c = "c1"
    db.ingest({"type": "outbound", "message_id": "a1", "contact_id": c, "campaign_id": "A"})
    db.ingest({"type": "delivered", "message_id": "a1", "at": 9_000})
    db.ingest({"type": "outbound", "message_id": "a2", "contact_id": c, "campaign_id": "A"})
    db.ingest({"type": "delivered", "message_id": "a2", "at": 19_000})
    db.ingest({"type": "purchase", "order_id": "o1", "contact_id": c, "at": 10_000, "amount": 100})
    db.ingest({"type": "purchase", "order_id": "o2", "contact_id": c, "at": 20_000, "amount": 200})
    assert db.explain("o1") == {"message_id": "a1", "campaign_id": "A"}
    assert db.explain("o2") == {"message_id": "a2", "campaign_id": "A"}
    assert db.campaign_stats("A") == {"revenue": 300, "conversions": 1}

    # 迟到触达先把 o1 改归 B
    db.ingest({"type": "delivered", "message_id": "b1", "at": 9_500})
    db.ingest({"type": "outbound", "message_id": "b1", "contact_id": c, "campaign_id": "B"})
    assert db.explain("o1") == {"message_id": "b1", "campaign_id": "B"}
    assert db.explain("o2") == {"message_id": "a2", "campaign_id": "A"}
    assert db.campaign_stats("A") == {"revenue": 200, "conversions": 1}  # 人数不降
    assert db.campaign_stats("B") == {"revenue": 100, "conversions": 1}

    # 再把 o2 改归 B，A 的转化人数此时才减少
    db.ingest({"type": "delivered", "message_id": "b2", "at": 19_500})
    db.ingest({"type": "outbound", "message_id": "b2", "contact_id": c, "campaign_id": "B"})
    assert db.explain("o2") == {"message_id": "b2", "campaign_id": "B"}
    assert db.campaign_stats("A") == {"revenue": 0, "conversions": 0}
    assert db.campaign_stats("B") == {"revenue": 300, "conversions": 1}  # 同一联系人不去重


def build_history(db, contact, n_orders):
    """每个订单各有一条恰好先于它的已送达消息，全部归活动 A。"""
    for i in range(n_orders):
        t = i * 1_000
        mid = f"h{i}"
        db.ingest({"type": "outbound", "message_id": mid,
                   "contact_id": contact, "campaign_id": "A"})
        db.ingest({"type": "delivered", "message_id": mid, "at": t})
        db.ingest({"type": "purchase", "order_id": f"{contact}-o{i}",
                   "contact_id": contact, "at": t + 10, "amount": 1})


def measure_steps(db, fn):
    steps = []
    def handler():
        steps.append(0)
        return 0
    db._conn.set_progress_handler(handler, 100)
    try:
        fn()
    finally:
        db._conn.set_progress_handler(None, 0)
    return len(steps) * 100  # SQLite VM 步数（近似，粒度 100）


def append_non_changing_late_events(db, contact, n_events, last_delivery_at):
    """迟到消息送达时间早于窗口内各订单当前归属消息的送达时间，不改变归属。"""
    for k in range(n_events):
        mid = f"late-{contact}-{k}"
        at = last_delivery_at - 500 + k  # 窗口覆盖末尾约 605 笔订单，但送达均更早
        db.ingest({"type": "delivered", "message_id": mid, "at": at})
        db.ingest({"type": "outbound", "message_id": mid,
                   "contact_id": contact, "campaign_id": "B"})


def test_late_events_do_not_scale_with_history_size():
    sizes = [2_000, 20_000]  # 热点联系人历史差一个数量级
    step_counts = []
    for n in sizes:
        db = AttributionDB()
        build_history(db, "hot", n)
        last_delivery_at = (n - 1) * 1_000
        steps = measure_steps(
            db, lambda: append_non_changing_late_events(db, "hot", 500, last_delivery_at)
        )
        step_counts.append(steps)
        # 确认这些迟到事件确实没有改变归因
        assert db.campaign_stats("A") == {"revenue": n, "conversions": 1}
        assert db.campaign_stats("B") == {"revenue": 0, "conversions": 0}
        db.close()
    small, large = step_counts
    assert small > 0
    # 历史放大 10 倍，处理步数不得随之线性增长（容忍索引深度等对数级差异）
    assert large < small * 3, f"steps: {step_counts}"
