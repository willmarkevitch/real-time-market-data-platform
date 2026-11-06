class OffsetTracker:
    """Track contiguous completed Kafka offsets per partition."""

    def __init__(self):
        self._partitions = {}
        self._candle_offsets = {}

    def register(self, topic, partition, offset):
        """Register consumed Kafka records in increasing offset order."""
        key = (topic, partition)

        if key not in self._partitions:
            self._partitions[key] = {
                "next_offset": offset,
                "records": {},
                "last_registered_offset": None,
            }

        state = self._partitions[key]
        last_offset = state["last_registered_offset"]

        if last_offset is not None and offset <= last_offset:
            raise ValueError(
                "Offsets must be registered in increasing order"
            )

        state["records"][offset] = False
        state["last_registered_offset"] = offset

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
        records = state["records"]

        if offset < next_offset:
            raise ValueError("Cannot confirm an older offset")

        committable = self.committable_offset(topic, partition)

        if committable is None:
            raise ValueError("No completed offsets to commit")

        if offset > committable:
            raise ValueError("Cannot commit past incomplete records")

        if offset - 1 not in records or not records[offset - 1]:
            raise ValueError(
                "Commit offset must follow a completed record"
            )

        for completed_offset in list(records):
            if completed_offset < offset:
                del records[completed_offset]

        state["next_offset"] = offset

    def tracked_partitions(self):
        """Return the topic-partition pairs currently being tracked."""
        return list(self._partitions.keys())

    def associate_candle(self, topic, partition, offset, candle_key):
        """Associate a registered Kafka record with a candle."""
        state = self._partitions.get((topic, partition))

        if state is None or offset not in state["records"]:
            raise ValueError(
                "Record must be registered before candle association"
            )

        if candle_key not in self._candle_offsets:
            self._candle_offsets[candle_key] = []

        record = (topic, partition, offset)

        if record not in self._candle_offsets[candle_key]:
            self._candle_offsets[candle_key].append(record)


    def offsets_for_candle(self, candle_key):
        """Return Kafka records associated with a candle."""
        return list(self._candle_offsets.get(candle_key, []))


    def complete_candle(self, candle_key):
        """Mark all records for a published candle as complete."""
        records = self._candle_offsets.get(candle_key, [])

        for topic, partition, offset in records:
            self.mark_complete(topic, partition, offset)

        self._candle_offsets.pop(candle_key, None)
