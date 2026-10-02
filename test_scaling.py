"""Scaling tests.

Appending 1000 late events that cannot change any attribution must cost a
number of SQLite VM steps that does not grow linearly with the hotspot
contact's history length. Steps are counted with the connection's progress
handler (an exact opcode count), not wall-clock time.
"""

import pytest

from attribution import AttributionStore

DAY = 86400


class StepCounter:
    def __init__(self, conn):
        self.steps = 0
        conn.set_progress_handler(self._tick, 1)

    def _tick(self):
        self.steps += 1
        return 0

    def detach(self, conn):
        conn.set_progress_handler(None, 0)


def _build_history(store, n_messages):
    """One hotspot contact: a delivered message per day, a purchase every
    10 days, spread across three campaigns."""
    for i in range(n_messages):
        store.ingest({"type": "outbound", "message_id": f"m{i}",
                      "contact_id": "hot", "campaign_id": f"camp{i % 3}"})
        store.ingest({"type": "delivered", "message_id": f"m{i}",
                      "at": i * DAY})
        if i % 10 == 9:
            j = i // 10
            store.ingest({"type": "purchase", "order_id": f"o{j}",
                          "contact_id": "hot", "at": i * DAY + 100,
                          "amount": 100 + j})


def _late_events(n_messages):
    """1000 late events that provably cannot change any attribution."""
    events = []
    # 400 orphan delivery receipts: outbound never arrives.
    for k in range(400):
        events.append({"type": "delivered", "message_id": f"orphan{k}",
                       "at": k * 1000})
    # 300 duplicate re-deliveries of existing outbound events.
    for k in range(300):
        i = (k * 97) % n_messages
        events.append({"type": "outbound", "message_id": f"m{i}",
                       "contact_id": "hot", "campaign_id": f"camp{i % 3}"})
    # 150 newly qualified messages (300 events) delivered one day after a
    # purchase: their 7-day forward window ends before the next purchase,
    # so they can never become any order's candidate.
    for k in range(150):
        j = (k * 31 + 5) % (n_messages // 10)
        at = (10 * j + 1) * DAY
        events.append({"type": "outbound", "message_id": f"late{k}",
                       "contact_id": "hot", "campaign_id": "campX"})
        events.append({"type": "delivered", "message_id": f"late{k}",
                       "at": at})
    assert len(events) == 1000
    return events


def _snapshot(store, n_messages):
    snap = {c: store.campaign_stats(c) for c in ("camp0", "camp1", "camp2")}
    for j in range(0, n_messages // 10, 997):
        snap[f"o{j}"] = store.explain(f"o{j}")
    return snap


def _run(n_messages):
    store = AttributionStore()
    _build_history(store, n_messages)
    before = _snapshot(store, n_messages)
    counter = StepCounter(store._db)
    for ev in _late_events(n_messages):
        store.ingest(ev)
    counter.detach(store._db)
    after = _snapshot(store, n_messages)
    store.close()
    assert before == after, "late events must not change attribution"
    return counter.steps


def test_late_events_do_not_scan_full_history():
    steps_small = _run(10_000)
    steps_large = _run(100_000)
    print(f"\nsteps @10k history: {steps_small}")
    print(f"steps @100k history: {steps_large}")
    # 10x more history must not mean 10x more work; index seeks grow at
    # most logarithmically, so allow a generous 1.5x margin.
    assert steps_large < steps_small * 1.5
