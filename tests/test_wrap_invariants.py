"""
Regression tests for the cursor/size invariant a wrapping producer used to break
(v2.0.0, matching C++ slick-queue v2.0.0).

Both read() paths and read_last() used to load slot.size twice, unvalidated, so a
producer recycling the slot between the loads left the cursor advanced by one
record's size while another record was described.

What is guaranteed, and what is not
-----------------------------------
The queue is lossy by design: a producer writes an element's data *before* it
publishes the slot's new data_index. A consumer that has been lapped can
therefore copy bytes the producer is midway through overwriting, and no amount
of validation on data_index can detect that - the index genuinely has not
changed yet. C++ has the identical hazard and simply hands back a pointer whose
target is the producer's to overwrite.

So the concurrent tests here assert the invariant that *is* guaranteed - that
the cursor and the size describe one and the same record - and not the content
of the bytes. Publish sizes cycle 1, 1, 2 and sum to 4, which divides the
16-slot queue exactly, so a record beginning at absolute index ``i`` always has
size ``{0: 1, 1: 1, 2: 2}[i % 4]``; checking the returned size against the
record index the cursor implies is exactly the C++ invariant, expressed in index
arithmetic rather than in data.

The deterministic tests at the end drive the recycle themselves, with no
producer running during the copy, so those can and do check the data content.
"""
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue, QueueTraits


class LossDetection(QueueTraits):
    """Explicit rather than relying on the default, so these tests keep asserting a
    non-zero loss count even if the default ever changes."""

    enable_loss_detection = True
from atomic_ops import AtomicCursor

QUEUE_SIZE = 16
ELEMENT_SIZE = 8
PUBLISH_SIZES = (1, 1, 2)
READ_ATTEMPTS = 20000
PRODUCER_TIMEOUT_S = 30


def _unique(prefix):
    return f"{prefix}_{os.getpid()}"


def _tag(start, n):
    return (start << 8) | n


def _untag(value):
    return value >> 8, value & 0xFF


def _publish_record(q, n):
    """Publish one record of n slots, tagging every element with the record."""
    idx = q.reserve(n)
    tag = _tag(idx, n)
    for k in range(n):
        q[idx + k][:ELEMENT_SIZE] = tag.to_bytes(ELEMENT_SIZE, 'little')
    q.publish(idx, n)
    return idx


def _prime_overrun(q):
    """Lap the queue before the consumer starts, so the overrun path is exercised
    by construction rather than by winning a scheduling race.

    Publishes whole cycles only. One cycle advances the index by sum(PUBLISH_SIZES),
    so a whole number of them leaves the index a multiple of 4 and the producer that
    starts afterwards - restarting its own cycle counter at 0 - stays in phase with
    the index/size mapping _check_cursor_size() relies on.
    """
    cycles = 8  # 8 * 4 = 32 = two laps of a 16-slot queue
    for i in range(len(PUBLISH_SIZES) * cycles):
        _publish_record(q, PUBLISH_SIZES[i % len(PUBLISH_SIZES)])


def _start_producer(q, stop):
    """Publish records of cycling size, each element tagged with its record."""
    def run():
        i = 0
        while not stop.is_set():
            _publish_record(q, PUBLISH_SIZES[i % len(PUBLISH_SIZES)])
            i += 1

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


class _RecycleOnLoad:
    """Slot proxy that runs a callback at the exact moment a reader has loaded
    data_index and is about to load slot.size.

    CPython switches threads only every few milliseconds, so the window the
    seqlock bracket closes is far too narrow to hit by racing two threads - the
    producer would have to complete a whole lap inside it. Driving the
    interleaving through the slot the reader is about to use reproduces it
    exactly, and needs no hook in the queue itself: _atomic_slots is a plain
    list of wrappers.
    """

    def __init__(self, inner, on_first_load):
        self._inner = inner
        self._on_first_load = on_first_load
        self.loads = 0

    def load_acquire(self):
        # The reader gets the pre-recycle index; by the time it looks at
        # slot.size the producer has lapped and overwritten both fields.
        value = self._inner.load_acquire()
        self.loads += 1
        if self.loads == 1 and self._on_first_load is not None:
            callback, self._on_first_load = self._on_first_load, None
            callback()
        return value

    def __getattr__(self, name):
        return getattr(self._inner, name)


# A record beginning at absolute index i has this size, by construction of
# PUBLISH_SIZES cycling 1,1,2 over a queue whose capacity 4 divides exactly.
_SIZE_FOR_INDEX = {0: 1, 1: 1, 2: 2}


def _check_cursor_size(record_index, size):
    """The cursor and the size must describe one and the same record.

    ``record_index`` is the cursor after the call minus the size returned. If the
    two came from different generations - the bug this file guards - the size will
    not be the size the producer actually published at that index.

    This holds under a concurrent producer, unlike any check on the data content.
    """
    assert record_index % 4 in _SIZE_FOR_INDEX, (
        f"record index {record_index} is not a record boundary; the cursor and the "
        f"size returned came from different records"
    )
    expected = _SIZE_FOR_INDEX[record_index % 4]
    assert size == expected, (
        f"record {record_index} was published with size {expected}, but the cursor "
        f"advanced by {size}"
    )


def _check_block(data, size, expected_start=None):
    """Full content check: every element must belong to one record, and that
    record's size must be the size that was returned.

    Only valid when no producer is running during the copy - see the module
    docstring.
    """
    assert len(data) == size * ELEMENT_SIZE, \
        f"returned {len(data)} bytes for size {size}"

    starts = set()
    for k in range(size):
        value = int.from_bytes(data[k * ELEMENT_SIZE:(k + 1) * ELEMENT_SIZE], 'little')
        start, record_size = _untag(value)
        assert record_size == size, \
            f"element {k} belongs to a record of size {record_size}, but size {size} was returned"
        starts.add(start)

    assert len(starts) == 1, f"block spans multiple records: {sorted(starts)}"
    start = starts.pop()
    if expected_start is not None:
        assert start == expected_start, \
            f"cursor describes record {expected_start} but record {start} was returned"
    return start


def test_wrapping_producer_cursor_size_invariant():
    """The cursor after the call minus the size returned must land on a record
    boundary whose published size is the size that was returned."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE, traits=LossDetection)
    _prime_overrun(q)
    stop = threading.Event()
    producer = _start_producer(q, stop)
    try:
        cursor = 0
        reads = 0
        for _ in range(READ_ATTEMPTS):
            data, size, cursor = q.read(cursor)
            if data is None:
                continue
            assert len(data) == size * ELEMENT_SIZE, \
                f"returned {len(data)} bytes for size {size}"
            _check_cursor_size(cursor - size, size)
            reads += 1

        assert reads > 0, "consumer never read anything"
        assert q.loss_count() > 0, \
            "the consumer kept up, so the overrun path was never exercised"
    finally:
        stop.set()
        producer.join(timeout=PRODUCER_TIMEOUT_S)
        q.close()


def test_wrapping_producer_atomic_cursor_invariant():
    """Same invariant through the shared-cursor overload, where the size load
    used to straddle the claiming CAS."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE, traits=LossDetection)
    _prime_overrun(q)
    stop = threading.Event()
    producer = _start_producer(q, stop)
    try:
        cursor = AtomicCursor(bytearray(8), 0)
        reads = 0
        for _ in range(READ_ATTEMPTS):
            data, size, index = q.read(cursor)
            if data is None:
                continue
            assert len(data) == size * ELEMENT_SIZE, \
                f"returned {len(data)} bytes for size {size}"
            _check_cursor_size(index, size)
            reads += 1

        assert reads > 0, "consumer never read anything"
        assert q.loss_count() > 0, \
            "the consumer kept up, so the overrun path was never exercised"
    finally:
        stop.set()
        producer.join(timeout=PRODUCER_TIMEOUT_S)
        q.close()


def test_read_last_never_returns_a_torn_size():
    """read_last() validates the slot before and after reading its size, so the
    size it returns is always one a producer actually published - never a value
    caught midway through a slot being recycled."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE)
    stop = threading.Event()
    producer = _start_producer(q, stop)
    try:
        hits = 0
        for _ in range(READ_ATTEMPTS):
            data, size = q.read_last()
            if data is None:
                continue  # the slot was recycled mid-read; correctly refused
            assert size in PUBLISH_SIZES, f"read_last returned size {size}"
            assert len(data) == size * ELEMENT_SIZE, \
                f"returned {len(data)} bytes for size {size}"
            hits += 1

        assert hits > 0, "read_last() never returned a record"
    finally:
        stop.set()
        producer.join(timeout=PRODUCER_TIMEOUT_S)
        q.close()


def test_read_last_shared_memory_wraparound():
    """The same, on a shared-memory segment attached through the reader
    constructor so the header-derived mask is exercised."""
    name = _unique("slq_wrap_shm")
    creator = SlickQueue(name=name, size=QUEUE_SIZE, element_size=ELEMENT_SIZE)
    stop = threading.Event()
    producer = _start_producer(creator, stop)
    reader = None
    try:
        reader = SlickQueue(name=name, element_size=ELEMENT_SIZE)
        assert reader.size == QUEUE_SIZE
        assert reader.mask == QUEUE_SIZE - 1

        hits = 0
        deadline = time.monotonic() + 10
        for _ in range(READ_ATTEMPTS):
            if time.monotonic() > deadline:
                break
            data, size = reader.read_last()
            if data is None:
                continue
            assert size in PUBLISH_SIZES, f"read_last returned size {size}"
            assert len(data) == size * ELEMENT_SIZE, \
                f"returned {len(data)} bytes for size {size}"
            hits += 1

        assert hits > 0, "read_last() never returned a record"
    finally:
        stop.set()
        producer.join(timeout=PRODUCER_TIMEOUT_S)
        if reader is not None:
            reader.close()
        creator.close()
        creator.unlink()


# ------------------------------------------------- deterministic interleavings


def _lap_onto_slot_zero(q):
    """Fill slots 1..7 with size-1 records, then recycle slot 0 with a size-2
    record at index 8."""
    for _ in range(QUEUE_SIZE - 1):
        _publish_record(q, 1)
    start = _publish_record(q, 2)
    assert start == QUEUE_SIZE, start
    return start


def test_read_retries_when_the_slot_is_recycled_mid_read():
    """A producer that laps between the data_index load and the size load must
    not leave the cursor describing a different record than the one returned."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE, traits=LossDetection)
    try:
        assert _publish_record(q, 1) == 0
        q._atomic_slots[0] = _RecycleOnLoad(q._atomic_slots[0],
                                            lambda: _lap_onto_slot_zero(q))

        data, size, cursor = q.read(0)
        assert data is not None
        _check_block(data, size, expected_start=cursor - size)
        assert cursor - size == QUEUE_SIZE, \
            f"expected the recycled record at {QUEUE_SIZE}, got {cursor - size}"
    finally:
        q.close()


def test_atomic_cursor_read_retries_when_the_slot_is_recycled_mid_read():
    """Same, through the shared-cursor overload, where the size load used to
    straddle the claiming CAS."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE, traits=LossDetection)
    try:
        assert _publish_record(q, 1) == 0
        q._atomic_slots[0] = _RecycleOnLoad(q._atomic_slots[0],
                                            lambda: _lap_onto_slot_zero(q))

        cursor = AtomicCursor(bytearray(8), 0)
        data, size, index = q.read(cursor)
        assert data is not None
        start = _check_block(data, size)
        assert cursor.load() - size == start, \
            f"cursor advanced to {cursor.load()} for record {start} of size {size}"
    finally:
        q.close()


def test_read_last_refuses_a_recycled_slot():
    """read_last() validates the slot against the last published index, so a
    slot a wrapping producer has already taken over is refused rather than
    returned as if it were the record the index names."""
    q = SlickQueue(size=QUEUE_SIZE, element_size=ELEMENT_SIZE)
    try:
        assert _publish_record(q, 1) == 0
        data, size = q.read_last()
        assert data is not None
        _check_block(data, size, expected_start=0)

        # A producer has recycled slot 0 for the record at index 16 - it wrote
        # slot.size and advanced data_index, but has not yet advanced
        # last_published, which still names record 0.
        q._write_slot(0, QUEUE_SIZE, 2)

        data, size = q.read_last()
        assert data is None, \
            "read_last() returned a record from a slot that no longer holds it"
    finally:
        q.close()


def run_all_tests():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print("Wrapping-producer invariant tests")
    print("=" * 70)

    passed = 0
    failed = 0
    for name, test_func in tests:
        try:
            print(f"\n[TEST] {name}...", end=" ")
            test_func()
            print("[PASSED]")
            passed += 1
        except Exception as e:
            print("[FAILED]")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
