"""Correctness tests.

Every ingest is followed by a full comparison against a brute-force
reference implementation, not just a final-state check.
"""

import random

import pytest

from attribution import WINDOW_SECONDS, AttributionStore
from reference import ReferenceAttribution

CAMPAIGNS = ["campA", "campB", "campC"]


def _designed_events():
    """Hand-crafted events: cross-campaign touches, same-second deliveries,
    7-day boundary (both edges), receipt-before-outbound."""
    events = []

    # Same-second deliveries for c1: tie broken by ASCII-largest message_id.
    # "m_tie_z" wins over "m_tie_a" even though it belongs to campB.
    events += [
        {"type": "outbound", "message_id": "m_tie_z", "contact_id": "c1",
         "campaign_id": "campB"},
        {"type": "outbound", "message_id": "m_tie_a", "contact_id": "c1",
         "campaign_id": "campA"},
        {"type": "delivered", "message_id": "m_tie_z", "at": 5000},
        {"type": "delivered", "message_id": "m_tie_a", "at": 5000},
        {"type": "purchase", "order_id": "o_tie", "contact_id": "c1",
         "at": 6000, "amount": 100},
    ]

    # 7-day window boundaries for c2, purchase at T.
    T = 700000
    events += [
        # delivered exactly T - WINDOW: inside (inclusive lower bound)
        {"type": "outbound", "message_id": "m_edge_in", "contact_id": "c2",
         "campaign_id": "campA"},
        {"type": "delivered", "message_id": "m_edge_in",
         "at": T - WINDOW_SECONDS},
        # delivered one second earlier: outside
        {"type": "outbound", "message_id": "m_edge_out", "contact_id": "c2",
         "campaign_id": "campB"},
        {"type": "delivered", "message_id": "m_edge_out",
         "at": T - WINDOW_SECONDS - 1},
        # delivered exactly at purchase time: inside (inclusive upper bound)
        {"type": "outbound", "message_id": "m_edge_now", "contact_id": "c2",
         "campaign_id": "campC"},
        {"type": "delivered", "message_id": "m_edge_now", "at": T},
        {"type": "purchase", "order_id": "o_edge", "contact_id": "c2",
         "at": T, "amount": 200},
    ]

    # Receipt strictly before outbound; a later cross-campaign touch wins.
    events += [
        {"type": "delivered", "message_id": "m_late", "at": 100},
        {"type": "purchase", "order_id": "o_late", "contact_id": "c3",
         "at": 1000, "amount": 300},
        {"type": "outbound", "message_id": "m_late", "contact_id": "c3",
         "campaign_id": "campA"},
        {"type": "outbound", "message_id": "m_late_b", "contact_id": "c3",
         "campaign_id": "campB"},
        {"type": "delivered", "message_id": "m_late_b", "at": 500},
    ]
    return events


def _random_events(rng):
    events = []
    for i in range(60):
        contact = f"c{rng.randint(1, 5)}"
        campaign = CAMPAIGNS[rng.randrange(len(CAMPAIGNS))]
        mid = f"m_rand_{i}"
        at = rng.randint(0, 20000)
        pair = [
            {"type": "outbound", "message_id": mid, "contact_id": contact,
             "campaign_id": campaign},
            {"type": "delivered", "message_id": mid, "at": at},
        ]
        rng.shuffle(pair)  # receipt may precede outbound
        events += pair
    for i in range(25):
        events.append({
            "type": "purchase",
            "order_id": f"o_rand_{i}",
            "contact_id": f"c{rng.randint(1, 5)}",
            "at": rng.randint(0, 25000),
            "amount": rng.randint(1, 10000),
        })
    return events


def _check_all(store, ref, order_ids):
    for camp in CAMPAIGNS:
        assert store.campaign_stats(camp) == ref.campaign_stats(camp), camp
    for oid in order_ids:
        assert store.explain(oid) == ref.explain(oid), oid


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_shuffled_and_duplicated_events_match_reference(seed):
    rng = random.Random(seed)
    events = _designed_events() + _random_events(rng)
    rng.shuffle(events)
    # Re-deliver ~25% of events at random positions (duplicates are
    # content-identical and must be no-ops).
    with_dups = []
    for ev in events:
        with_dups.append(ev)
        if rng.random() < 0.25:
            with_dups.append(dict(ev))
    rng.shuffle(with_dups)

    store = AttributionStore()
    ref = ReferenceAttribution()
    order_ids = [e["order_id"] for e in events if e["type"] == "purchase"]
    for ev in with_dups:
        store.ingest(ev)
        ref.ingest(ev)
        _check_all(store, ref, order_ids)
    store.close()


def test_same_second_tie_and_boundaries_final_state():
    store = AttributionStore()
    for ev in _designed_events():
        store.ingest(ev)
    # ASCII tie-break: m_tie_z > m_tie_a
    assert store.explain("o_tie") == ("m_tie_z", "campB")
    # Latest delivery within window wins; both bounds inclusive.
    assert store.explain("o_edge") == ("m_edge_now", "campC")
    # Receipt before outbound still attributes once both exist; the later
    # campB touch then steals it.
    assert store.explain("o_late") == ("m_late_b", "campB")
    store.close()


def test_late_touches_migrate_orders_one_by_one():
    store = AttributionStore()

    def ingest(ev):
        store.ingest(ev)

    # Two orders of the same contact, both initially attributed to campA.
    ingest({"type": "outbound", "message_id": "mA1", "contact_id": "c",
            "campaign_id": "campA"})
    ingest({"type": "delivered", "message_id": "mA1", "at": 1000})
    ingest({"type": "outbound", "message_id": "mA2", "contact_id": "c",
            "campaign_id": "campA"})
    ingest({"type": "delivered", "message_id": "mA2", "at": 100000})
    ingest({"type": "purchase", "order_id": "o1", "contact_id": "c",
            "at": 2000, "amount": 100})
    ingest({"type": "purchase", "order_id": "o2", "contact_id": "c",
            "at": 700000, "amount": 200})

    assert store.explain("o1") == ("mA1", "campA")
    assert store.explain("o2") == ("mA2", "campA")
    assert store.campaign_stats("campA") == (300, 1)
    assert store.campaign_stats("campB") == (0, 0)

    # Late touch delivered at 1500: only o1 (at 2000) is inside its window.
    ingest({"type": "outbound", "message_id": "mB1", "contact_id": "c",
            "campaign_id": "campB"})
    ingest({"type": "delivered", "message_id": "mB1", "at": 1500})

    assert store.explain("o1") == ("mB1", "campB")
    assert store.explain("o2") == ("mA2", "campA")
    assert store.campaign_stats("campA") == (200, 1)  # o2 keeps contact count
    assert store.campaign_stats("campB") == (100, 1)

    # Late touch delivered at 200000: steals o2 as well.
    ingest({"type": "outbound", "message_id": "mB2", "contact_id": "c",
            "campaign_id": "campB"})
    ingest({"type": "delivered", "message_id": "mB2", "at": 200000})

    assert store.explain("o1") == ("mB1", "campB")
    assert store.explain("o2") == ("mB2", "campB")
    # Only now does campA lose its converted contact.
    assert store.campaign_stats("campA") == (0, 0)
    assert store.campaign_stats("campB") == (300, 1)
    store.close()
