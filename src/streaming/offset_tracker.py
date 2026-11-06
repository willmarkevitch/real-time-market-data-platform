class OffsetTracker:
    """Track contiguous completed Kafka offsets per partition."""

    def __init__(self):
        self._partitions = {}

    def register(self, topic, partition, offset):
        """Register a consumed Kafka record as pending."""
        key = (topic, partition)

        if key not in self._partitions:
            self._partitions[key] = {
                "next_offset": offset,
                "records": {},
            }

        self._partitions[key]["records"][offset] = False

    def mark_complete(self, topic, partition, offset):
        """Mark a record as durably processed."""
        key = (topic, partition)
        self._partitions[key]["records"][offset] = True

    def committable_offset(self, topic, partition):
        """Find the next safe offset among observed Kafka records."""
        state = self._partitions.get((topic, partition))

        if state is None:
            return None

        next_offset = state["next_offset"]
        records = state["records"]

        if not records:
            return None

        eligible = [
            offset
            for offset in sorted(records)
            if offset >= next_offset
        ]

        if not eligible:
            return None

        highest_complete = None

        for offset in eligible:
            if not records[offset]:
                break

            highest_complete = offset

        if highest_complete is None:
            return None

        return highest_complete + 1

    def confirm_commit(self, topic, partition, offset):
        """Discard completed records after Kafka confirms a commit."""
        state = self._partitions[(topic, partition)]

        next_offset = state["next_offset"]

        if offset < next_offset:
            raise ValueError("Cannot confirm an older offset")

        if self.committable_offset(topic, partition) is None:
            raise ValueError("No completed offsets to commit")

        if offset > self.committable_offset(topic, partition):
            raise ValueError("Cannot commit past incomplete records")

        records = state["records"]

        for completed_offset in list(records):
            if completed_offset < offset:
                del records[completed_offset]

        state["next_offset"] = offset
