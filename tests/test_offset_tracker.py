import pytest
from src.streaming.offset_tracker import OffsetTracker


def test_tracker_commits_contiguous_completed_offsets():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.register("market.trades", 0, 42)

    tracker.mark_complete("market.trades", 0, 42)

    # Offset 41 is still pending.
    assert tracker.committable_offset("market.trades", 0) is None

    tracker.mark_complete("market.trades", 0, 41)

    # Both records are complete, so the next offset is 43.
    assert tracker.committable_offset("market.trades", 0) == 43


def test_tracker_does_not_advance_until_commit_confirmed():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.mark_complete("market.trades", 0, 41)

    # Calculating the safe offset must not consume it.
    assert tracker.committable_offset("market.trades", 0) == 42
    assert tracker.committable_offset("market.trades", 0) == 42

    # Kafka has now confirmed the commit.
    tracker.confirm_commit("market.trades", 0, 42)

    # No new records are ready to commit.
    assert tracker.committable_offset("market.trades", 0) is None


def test_tracker_handles_gaps_between_registered_offsets():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.register("market.trades", 0, 43)

    tracker.mark_complete("market.trades", 0, 41)
    tracker.mark_complete("market.trades", 0, 43)

    # Offset 42 was never delivered to this consumer.
    assert tracker.committable_offset("market.trades", 0) == 44


def test_tracker_manages_partitions_independently():
    tracker = OffsetTracker()

    # Partition 0 has an unfinished record.
    tracker.register("market.trades", 0, 10)

    # Partition 1 has a completed record.
    tracker.register("market.trades", 1, 25)
    tracker.mark_complete("market.trades", 1, 25)

    assert tracker.committable_offset("market.trades", 0) is None
    assert tracker.committable_offset("market.trades", 1) == 26

    tracker.confirm_commit("market.trades", 1, 26)

    assert tracker.committable_offset("market.trades", 1) is None

    # Partition 0 remains pending.
    assert tracker.committable_offset("market.trades", 0) is None


def test_tracker_rejects_commit_beyond_completed_offsets():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.mark_complete("market.trades", 0, 41)

    assert tracker.committable_offset("market.trades", 0) == 42

    with pytest.raises(
        ValueError,
        match="Cannot commit past incomplete records",
    ):
        tracker.confirm_commit("market.trades", 0, 45)

    # A rejected confirmation must not change tracker state.
    assert tracker.committable_offset("market.trades", 0) == 42


def test_tracker_rejects_out_of_order_registration():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 43)

    # Kafka offsets within a partition should be registered
    # in increasing order.
    with pytest.raises(
        ValueError,
        match="Offsets must be registered in increasing order",
    ):
        tracker.register("market.trades", 0, 41)


def test_tracker_rejects_confirmation_inside_offset_gap():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.register("market.trades", 0, 43)

    tracker.mark_complete("market.trades", 0, 41)
    tracker.mark_complete("market.trades", 0, 43)

    assert tracker.committable_offset("market.trades", 0) == 44

    with pytest.raises(
        ValueError,
        match="Commit offset must follow a completed record",
    ):
        tracker.confirm_commit("market.trades", 0, 43)

    # Invalid confirmation must not alter state.
    assert tracker.committable_offset("market.trades", 0) == 44


def test_tracker_exposes_tracked_partitions():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)
    tracker.register("market.trades", 1, 100)
    tracker.register("market.quotes", 0, 25)

    assert set(tracker.tracked_partitions()) == {
        ("market.trades", 0),
        ("market.trades", 1),
        ("market.quotes", 0),
    }


def test_tracker_associates_record_with_candle():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)

    candle_key = ("AAPL", "2026-10-22T14:30:00+00:00")

    tracker.associate_candle(
        "market.trades",
        0,
        41,
        candle_key,
    )

    assert tracker.offsets_for_candle(candle_key) == [
        ("market.trades", 0, 41)
    ]


def test_tracker_does_not_duplicate_candle_associations():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)

    candle_key = ("AAPL", "2026-10-22T14:30:00+00:00")

    tracker.associate_candle(
        "market.trades", 0, 41, candle_key
    )
    tracker.associate_candle(
        "market.trades", 0, 41, candle_key
    )

    assert tracker.offsets_for_candle(candle_key) == [
        ("market.trades", 0, 41)
    ]


def test_tracker_rejects_unregistered_candle_association():
    tracker = OffsetTracker()

    candle_key = ("AAPL", "2026-10-22T14:30:00+00:00")

    with pytest.raises(
        ValueError,
        match="Record must be registered before candle association",
    ):
        tracker.associate_candle(
            "market.trades",
            0,
            41,
            candle_key,
        )

    assert tracker.offsets_for_candle(candle_key) == []


def test_tracker_rejects_unknown_offset_in_registered_partition():
    tracker = OffsetTracker()

    tracker.register("market.trades", 0, 41)

    candle_key = ("AAPL", "2026-10-22T14:30:00+00:00")

    with pytest.raises(
        ValueError,
        match="Record must be registered before candle association",
    ):
        tracker.associate_candle(
            "market.trades",
            0,
            42,
            candle_key,
        )

    assert tracker.offsets_for_candle(candle_key) == []


def test_tracker_completes_only_records_for_published_candle():
    tracker = OffsetTracker()

    aapl_candle = ("AAPL", "2026-10-22T14:30:00+00:00")
    msft_candle = ("MSFT", "2026-10-22T14:30:00+00:00")

    for offset in (41, 42, 43):
        tracker.register("market.trades", 0, offset)

    tracker.associate_candle(
        "market.trades", 0, 41, aapl_candle
    )
    tracker.associate_candle(
        "market.trades", 0, 42, aapl_candle
    )
    tracker.associate_candle(
        "market.trades", 0, 43, msft_candle
    )

    tracker.complete_candle(aapl_candle)

    # Only the AAPL records should become committable.
    assert tracker.committable_offset("market.trades", 0) == 43

    # MSFT must remain pending.
    assert tracker.offsets_for_candle(msft_candle) == [
        ("market.trades", 0, 43)
    ]

    # Completed candle metadata should be removed.
    assert tracker.offsets_for_candle(aapl_candle) == []


def test_tracker_completing_candle_twice_is_idempotent():
    tracker = OffsetTracker()

    candle_key = ("AAPL", "2026-10-22T14:30:00+00:00")

    tracker.register("market.trades", 0, 41)
    tracker.associate_candle(
        "market.trades", 0, 41, candle_key
    )

    # First completion.
    tracker.complete_candle(candle_key)

    assert tracker.committable_offset("market.trades", 0) == 42
    assert tracker.offsets_for_candle(candle_key) == []

    # Repeating the operation must not change the result.
    tracker.complete_candle(candle_key)

    assert tracker.committable_offset("market.trades", 0) == 42
    assert tracker.offsets_for_candle(candle_key) == []


def test_tracker_handles_candles_completed_out_of_order():
    tracker = OffsetTracker()

    aapl_candle = ("AAPL", "2026-10-22T14:30:00+00:00")
    msft_candle = ("MSFT", "2026-10-22T14:30:00+00:00")

    for offset in (41, 42, 43):
        tracker.register("market.trades", 0, offset)

    tracker.associate_candle(
        "market.trades", 0, 41, aapl_candle
    )
    tracker.associate_candle(
        "market.trades", 0, 42, msft_candle
    )
    tracker.associate_candle(
        "market.trades", 0, 43, aapl_candle
    )

    # MSFT publishes first, but offset 41 is still pending.
    tracker.complete_candle(msft_candle)

    assert tracker.committable_offset("market.trades", 0) is None

    # AAPL publishes second, completing all remaining records.
    tracker.complete_candle(aapl_candle)

    assert tracker.committable_offset("market.trades", 0) == 44
