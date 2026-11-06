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
