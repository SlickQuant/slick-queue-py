"""
Tests for SpscQueue, the port of C++ slick::spsc_queue (slick-queue v2.4.0).

The ring is lossless: try_reserve() reports a full ring instead of overwriting. read()
and read_last() copy the element out and hand its slot back at once, so unlike C++ no
slot stays held between calls.
"""
import os
import struct
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import (
    SlickQueue,
    SpscQueue,
    SpscQueueTraits,
    ELEMENT_SIZE_OFFSET,
    HEADER_MAGIC_OFFSET,
    INIT_STATE_OFFSET,
    INIT_STATE_READY,
    INIT_STATE_UNINITIALIZED,
    SIZE_OFFSET,
    SPSC_HEAD_OFFSET,
    SPSC_HEADER_SIZE,
    SPSC_TAIL_OFFSET,
    _open_shm,
)

INVALID = SpscQueue.invalid_index
SLS0 = 0x534C5330
IS_WINDOWS = sys.platform == 'win32'


def push(q, value):
    index = q.reserve()
    q[index][:8] = struct.pack('<Q', value)
    q.publish(index)


def value_of(data):
    """The uint64 a read returned, or None for an empty read."""
    return None if data is None else struct.unpack('<Q', data)[0]


def produce(q, first, count, proc=None, timeout=30.0):
    """Push first..first+count-1 in batches of 1..3, so batches wrap. Fails instead of
    spinning forever if the consumer, possibly the process proc, stops reading."""
    deadline = time.monotonic() + timeout
    i = 0
    while i < count:
        n = min(1 + i % 3, count - i, q.size)
        index = q.try_reserve(n)
        if index == INVALID:
            assert proc is None or proc.poll() is None, "consumer process exited early"
            assert time.monotonic() < deadline, f"producer stuck after {i} items"
            time.sleep(0)
            continue
        for j in range(n):
            q[index + j][:8] = struct.pack('<Q', first + i + j)
        q.publish(index, n)
        i += n


def consume(q, count, timeout=30.0):
    """Read count values; return how many were not the expected next value."""
    deadline = time.monotonic() + timeout
    mismatches = 0
    n = 0
    while n < count:
        data = q.read()
        if data is None:
            assert time.monotonic() < deadline, f"consumer stuck after {n} items"
            time.sleep(0)
            continue
        mismatches += value_of(data) != n
        n += 1
    return mismatches


def expect_element_stride(producer, consumer):
    """On a ring of 4, publish batches of 3 that wrap, with bytes that differ per element
    and per position, so a stride or offset that ignores element_size shows up."""
    es = producer.element_size

    def record(i):
        return bytes((i * 7 + k) & 0xFF for k in range(es))

    nxt = 0
    for _ in range(3):
        index = producer.reserve(3)
        for j in range(3):
            producer[index + j][:] = record(nxt + j)
        producer.publish(index, 3)
        for j in range(3):
            assert consumer.read() == record(nxt + j)
        nxt += 3
    for j in range(2):
        index = producer.reserve()
        producer[index][:] = record(nxt + j)
        producer.publish(index)
    assert consumer.read_last() == (record(nxt + 1), 1)


def expect_batch_wraps(producer, consumer):
    """On a ring of 8, fill and drain 6, then publish a batch of 5 that wraps to slots
    6, 7, 0, 1, 2. producer and consumer may be one object."""
    assert producer.size == 8
    for i in range(6):
        push(producer, i)
    for i in range(6):
        assert value_of(consumer.read()) == i
    index = producer.reserve(5)
    assert index == 6
    for j in range(5):
        producer[index + j][:8] = struct.pack('<Q', 100 + j)
    assert bytes(producer[index + 2]) == bytes(producer[0])
    producer.publish(index, 5)
    for j in range(5):
        assert value_of(consumer.read()) == 100 + j
    assert consumer.read() is None


class Segments:
    """Unique segment names for one test. Unlinks each name before first use, clearing
    what a crashed run left, and on exit closes the tracked queues and unlinks again."""

    def __init__(self):
        self.names = []
        self.queues = []

    def __enter__(self):
        return self

    def name(self, tag):
        name = f"sp_{tag}_{os.getpid()}"  # short: macOS caps names at 31 characters
        SpscQueue.remove(name)
        self.names.append(name)
        return name

    def track(self, q):
        self.queues.append(q)
        return q

    def __exit__(self, *exc):
        for q in reversed(self.queues):
            q.close()
        for name in self.names:
            SpscQueue.remove(name)
        return False


def expect_raises(exc_type, fragment, fn):
    try:
        fn()
    except exc_type as e:
        assert fragment in str(e), f"expected {fragment!r} in {str(e)!r}"
        return
    raise AssertionError(f"expected {exc_type.__name__} containing {fragment!r}")


def _raw_u32(name, offset):
    raw = _open_shm(name, create=False)
    try:
        return struct.unpack_from('<I', raw.buf, offset)[0]
    finally:
        raw.close()


def _set_raw_u32(name, offset, value):
    raw = _open_shm(name, create=False)
    try:
        struct.pack_into('<I', raw.buf, offset, value)
    finally:
        raw.close()


# -- local memory -------------------------------------------------------------------

def test_invalid_arguments_raise():
    expect_raises(ValueError, "power of two", lambda: SpscQueue(size=0, element_size=8))
    expect_raises(ValueError, "power of two", lambda: SpscQueue(size=3, element_size=8))
    expect_raises(ValueError, "element_size", lambda: SpscQueue(size=4))
    expect_raises(ValueError, "size", lambda: SpscQueue(element_size=8))


def test_empty_reads_return_none():
    q = SpscQueue(size=4, element_size=8)
    assert not q.use_shm
    assert q.get_shm_name() is None
    assert q.read() is None
    assert q.read_last() == (None, 0)
    q.close()


def test_fifo_across_many_wraps():
    q = SpscQueue(size=4, element_size=8)
    nxt = 0
    for _ in range(100):
        for i in range(3):
            push(q, nxt + i)
        for i in range(3):
            assert value_of(q.read()) == nxt + i
        nxt += 3
    assert q.read() is None
    q.close()


def test_reserve_argument_checks():
    q = SpscQueue(size=4, element_size=8)
    expect_raises(ValueError, "> 0", lambda: q.reserve(0))
    expect_raises(RuntimeError, "> queue size", lambda: q.reserve(5))
    assert q.try_reserve(0) == INVALID
    assert q.try_reserve(5) == INVALID
    assert q.try_reserve(4) == 0
    q.close()


def test_full_ring_reports_full_and_read_frees_a_slot_at_once():
    # read() copies the element out, so its slot is free before read() returns. C++
    # holds the slot until the next read.
    q = SpscQueue(size=4, element_size=8)
    for i in range(4):
        push(q, i)
    assert q.try_reserve() == INVALID
    assert value_of(q.read()) == 0
    assert q.try_reserve() == 4
    assert q.try_reserve() == INVALID
    q.close()


def test_size_one_needs_no_extra_call():
    q = SpscQueue(size=1, element_size=8)
    for i in range(5):
        push(q, i)
        assert q.try_reserve() == INVALID
        assert value_of(q.read()) == i
    q.close()


def test_publish_out_of_order_never_moves_tail_backwards():
    # Publishing a later reservation first exposes both, and the earlier publish that
    # follows must not rewind tail under the consumer.
    q = SpscQueue(size=8, element_size=8)
    first = q.reserve()
    second = q.reserve()
    q[first][:8] = struct.pack('<Q', 1)
    q[second][:8] = struct.pack('<Q', 2)
    q.publish(second)
    assert value_of(q.read()) == 1
    assert value_of(q.read()) == 2

    q.publish(first)
    assert q.read() is None
    assert q.read_last() == (None, 0)

    push(q, 3)
    assert value_of(q.read()) == 3
    assert q.read() is None
    q.close()


def test_batch_reserve_wraps_around():
    q = SpscQueue(size=8, element_size=8)
    expect_batch_wraps(q, q)
    q.close()


def test_element_sizes_other_than_eight():
    for es in (1, 24):
        q = SpscQueue(size=4, element_size=es)
        expect_element_stride(q, q)
        q.close()


def test_use_after_close_raises():
    # Both paths: the consumer's cached tail is empty, or it still holds unread items.
    for cached in (False, True):
        q = SpscQueue(size=4, element_size=8)
        push(q, 1)
        push(q, 2)
        if cached:
            assert value_of(q.read()) == 1
        q.close()
        q.close()  # a second close is harmless
        for call in (q.read, q.read_last, q.try_reserve, q.reserve, q.reset, lambda: q[0]):
            expect_raises(RuntimeError, "queue is closed", call)


def test_read_last_drops_backlog_and_reports_dropped():
    q = SpscQueue(size=8, element_size=8)
    for i in range(1, 6):
        push(q, i)
    data, dropped = q.read_last()
    assert (value_of(data), dropped) == (5, 4)  # 1..4 dropped
    assert q.read() is None
    assert q.read_last() == (None, 0)

    push(q, 6)
    data, dropped = q.read_last()
    assert (value_of(data), dropped) == (6, 0)

    # An item already returned by read() is not counted as dropped.
    for i in (7, 8, 9):
        push(q, i)
    assert value_of(q.read()) == 7
    data, dropped = q.read_last()
    assert (value_of(data), dropped) == (9, 1)  # 8
    q.close()


def test_read_last_frees_whole_ring():
    q = SpscQueue(size=4, element_size=8)
    for i in range(1, 5):
        push(q, i)
    assert q.try_reserve() == INVALID
    data, dropped = q.read_last()
    assert (value_of(data), dropped) == (4, 3)
    index = q.try_reserve(4)
    assert index == 4
    for j in range(4):
        q[index + j][:8] = struct.pack('<Q', 10 + j)
    q.publish(index, 4)
    for j in range(4):
        assert value_of(q.read()) == 10 + j
    q.close()


def test_read_continues_after_read_last():
    q = SpscQueue(size=8, element_size=8)
    push(q, 1)
    push(q, 2)
    assert value_of(q.read_last()[0]) == 2
    push(q, 3)
    push(q, 4)
    assert value_of(q.read()) == 3
    assert value_of(q.read()) == 4
    assert q.read() is None
    q.close()


def test_reset_empties_queue():
    q = SpscQueue(size=4, element_size=8)
    push(q, 1)
    push(q, 2)
    assert value_of(q.read()) == 1
    q.reset()
    assert q.read() is None
    assert q.read_last() == (None, 0)
    assert q.try_reserve(4) == 0
    q.close()


def test_traits_validation():
    class NoRelax(SpscQueueTraits):
        enable_cpu_relax = False

    class NotBool(SpscQueueTraits):
        enable_cpu_relax = 1

    class Missing:
        pass

    q = SpscQueue(size=4, element_size=8, traits=NoRelax)
    push(q, 7)
    assert value_of(q.read()) == 7
    # A snapshot, as for SlickQueue: later edits to the class don't reach the queue.
    NoRelax.enable_cpu_relax = True
    assert q.traits.enable_cpu_relax is False
    expect_raises(AttributeError, "read-only", lambda: setattr(q.traits, "enable_cpu_relax", True))
    assert not hasattr(q.traits, "enable_read_last")  # a SlickQueue-only trait
    q.close()
    expect_raises(TypeError, "must be a bool", lambda: SpscQueue(size=4, element_size=8, traits=NotBool))
    expect_raises(TypeError, "SpscQueueTraits", lambda: SpscQueue(size=4, element_size=8, traits=Missing))


def _run_threads(producer_fn, consumer_fn):
    errors = []

    def guarded(fn):
        try:
            fn()
        except BaseException as e:  # surface a thread failure in the test
            errors.append(e)

    threads = [threading.Thread(target=guarded, args=(fn,)) for fn in (producer_fn, consumer_fn)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not any(t.is_alive() for t in threads), "thread did not finish"
    if errors:
        raise errors[0]


def test_concurrent_threads_lossless_fifo():
    for size in (4, 64):
        q = SpscQueue(size=size, element_size=8)
        result = {}
        _run_threads(lambda: produce(q, 0, 20000),
                     lambda: result.update(mismatches=consume(q, 20000)))
        assert result['mismatches'] == 0
        assert q.read() is None
        q.close()


def test_concurrent_read_last_accounts_for_every_item():
    # Every value is either returned or counted by read_last's dropped, exactly once.
    count = 20000
    q = SpscQueue(size=8, element_size=8)
    result = {}

    def consumer():
        miscounted = 0
        nxt = 0
        call = 0
        deadline = time.monotonic() + 30
        while nxt < count:
            assert time.monotonic() < deadline, f"consumer stuck at {nxt}"
            if call % 3 == 0:
                data, dropped = q.read_last()
            else:
                data, dropped = q.read(), 0
            call += 1
            if data is None:
                miscounted += dropped != 0
                time.sleep(0)
                continue
            value = value_of(data)
            miscounted += value != nxt + dropped
            nxt = value + 1
        result['miscounted'] = miscounted

    _run_threads(lambda: produce(q, 0, count), consumer)
    assert result['miscounted'] == 0
    q.close()


# -- shared memory ------------------------------------------------------------------

def test_creator_and_attacher_share_data():
    with Segments() as s:
        name = s.name("share")
        producer = s.track(SpscQueue(name=name, size=8, element_size=8))
        assert producer.use_shm and producer._own
        assert _raw_u32(name, HEADER_MAGIC_OFFSET) == SLS0

        consumer = s.track(SpscQueue(name=name, element_size=8))
        assert not consumer._own
        assert consumer.size == 8

        for i in (1, 2, 3):
            push(producer, i)
        assert value_of(consumer.read()) == 1
        data, dropped = consumer.read_last()
        assert (value_of(data), dropped) == (3, 1)
        assert consumer.read() is None


def test_header_layout_matches_cpp():
    assert (SPSC_TAIL_OFFSET, SPSC_HEAD_OFFSET, SPSC_HEADER_SIZE) == (128, 256, 384)
    with Segments() as s:
        name = s.name("layout")
        q = s.track(SpscQueue(name=name, size=8, element_size=8))
        for i in range(3):
            push(q, 40 + i)
        assert value_of(q.read()) == 40

        raw = _open_shm(name, create=False)
        try:
            buf = raw.buf
            assert struct.unpack_from('<I I', buf, SIZE_OFFSET) == (8, 8)
            assert struct.unpack_from('<I', buf, ELEMENT_SIZE_OFFSET)[0] == 8
            assert struct.unpack_from('<I', buf, INIT_STATE_OFFSET)[0] == INIT_STATE_READY
            assert struct.unpack_from('<Q', buf, SPSC_TAIL_OFFSET)[0] == 3
            assert struct.unpack_from('<Q', buf, SPSC_HEAD_OFFSET)[0] == 1
            for i in range(3):
                assert struct.unpack_from('<Q', buf, SPSC_HEADER_SIZE + 8 * i)[0] == 40 + i
        finally:
            del buf
            raw.close()


def test_claiming_too_small_leftover_segment_raises_and_releases_claim():
    # Regression: a creator that claimed a segment left uninitialized (its creator died
    # between creating it and claiming it) wrote a header for a ring the mapping could
    # not hold, so reads past the end of the mapping returned b'' instead of failing.
    with Segments() as s:
        name = s.name("leftover")
        keeper = s.track(SpscQueue(name=name, size=8, element_size=8))  # alive on Windows too
        _set_raw_u32(name, INIT_STATE_OFFSET, INIT_STATE_UNINITIALIZED)
        expect_raises(RuntimeError, "bytes but",
                      lambda: SpscQueue(name=name, size=1024, element_size=8))
        # The failed claim is handed back, so a creator whose geometry fits can take it.
        assert _raw_u32(name, INIT_STATE_OFFSET) == INIT_STATE_UNINITIALIZED
        again = s.track(SpscQueue(name=name, size=8, element_size=8))
        assert again._own and not keeper.read()


def test_creator_opening_existing_segment_joins_it():
    with Segments() as s:
        name = s.name("join")
        first = s.track(SpscQueue(name=name, size=8, element_size=8))
        second = s.track(SpscQueue(name=name, size=8, element_size=8))
        assert first._own and not second._own
        push(first, 7)
        assert value_of(second.read()) == 7


def test_geometry_mismatch_raises():
    with Segments() as s:
        name = s.name("mismatch")
        s.track(SpscQueue(name=name, size=8, element_size=8))
        expect_raises(ValueError, "size mismatch",
                      lambda: SpscQueue(name=name, size=16, element_size=8))
        expect_raises(ValueError, "element size mismatch",
                      lambda: SpscQueue(name=name, size=8, element_size=16))
        expect_raises(ValueError, "element size mismatch",
                      lambda: SpscQueue(name=name, element_size=16))


def test_attaching_missing_segment_raises():
    with Segments() as s:
        name = s.name("missing")
        try:
            SpscQueue(name=name, element_size=8)
        except FileNotFoundError:
            return
        raise AssertionError("expected FileNotFoundError")


def test_queue_types_reject_each_other_by_marker():
    # Both types keep init_state and the marker at the same offsets, so a mismatch fails
    # on the marker at once instead of after the 2s readiness timeout.
    with Segments() as s:
        spsc_name = s.name("vs_q")
        queue_name = s.name("vs_s")
        s.track(SpscQueue(name=spsc_name, size=8, element_size=8))
        s.track(SlickQueue(name=queue_name, size=8, element_size=8))

        start = time.monotonic()
        expect_raises(RuntimeError, "holds a slick::spsc_queue",
                      lambda: SlickQueue(name=spsc_name, element_size=8))
        expect_raises(RuntimeError, "holds a slick::spsc_queue",
                      lambda: SlickQueue(name=spsc_name, size=8, element_size=8))
        expect_raises(RuntimeError, "holds a slick::queue",
                      lambda: SpscQueue(name=queue_name, element_size=8))
        expect_raises(RuntimeError, "holds a slick::queue",
                      lambda: SpscQueue(name=queue_name, size=8, element_size=8))
        assert time.monotonic() - start < 1.0


def test_unknown_feature_bit_rejected():
    with Segments() as s:
        name = s.name("feature")
        s.track(SpscQueue(name=name, size=8, element_size=8))
        _set_raw_u32(name, HEADER_MAGIC_OFFSET, SLS0 | 0x1)
        expect_raises(RuntimeError, "unknown spsc_queue layout features",
                      lambda: SpscQueue(name=name, element_size=8))


def test_reattached_peers_resume_from_shared_cursors():
    with Segments() as s:
        name = s.name("resume")
        # The consumer creates the segment, so it stays alive on Windows while
        # producers come and go.
        consumer = SpscQueue(name=name, size=8, element_size=8)
        for first in (1, 3):
            producer = SpscQueue(name=name, element_size=8)
            push(producer, first)
            push(producer, first + 1)
            producer.close()
        assert value_of(consumer.read()) == 1

        reattached = s.track(SpscQueue(name=name, element_size=8))
        consumer.close()
        # Nothing is delivered twice: read() handed slot 1 back before it returned.
        for i in (2, 3, 4):
            assert value_of(reattached.read()) == i
        assert reattached.read() is None


def test_batch_reserve_wraps_across_mappings():
    with Segments() as s:
        name = s.name("batch")
        producer = s.track(SpscQueue(name=name, size=8, element_size=8))
        consumer = s.track(SpscQueue(name=name, element_size=8))
        expect_batch_wraps(producer, consumer)


def test_wide_elements_across_mappings():
    with Segments() as s:
        name = s.name("wide")
        producer = s.track(SpscQueue(name=name, size=4, element_size=24))
        consumer = s.track(SpscQueue(name=name, element_size=24))
        assert _raw_u32(name, ELEMENT_SIZE_OFFSET) == 24
        expect_element_stride(producer, consumer)
        raw = _open_shm(name, create=False)
        try:
            # Element 1 lives right after element 0, at 384 + 24.
            assert bytes(raw.buf[SPSC_HEADER_SIZE + 24: SPSC_HEADER_SIZE + 48]) == bytes(producer[1])
        finally:
            raw.close()


def test_reset_on_shm_queue_needs_peer_reattach():
    with Segments() as s:
        name = s.name("reset")
        producer = s.track(SpscQueue(name=name, size=4, element_size=8))
        consumer = SpscQueue(name=name, element_size=8)
        push(producer, 1)
        push(producer, 2)
        assert value_of(consumer.read()) == 1

        # reset() rewinds the shared cursors, and a peer only picks them up by reattaching.
        producer.reset()
        consumer.close()
        consumer = s.track(SpscQueue(name=name, element_size=8))
        assert consumer.read() is None
        assert consumer.read_last() == (None, 0)
        for i in range(4):
            push(producer, 10 + i)  # the whole ring is free again
        assert producer.try_reserve() == INVALID
        for i in range(4):
            assert value_of(consumer.read()) == 10 + i


def test_concurrent_lossless_fifo_across_mappings():
    with Segments() as s:
        name = s.name("stress")
        producer = s.track(SpscQueue(name=name, size=64, element_size=8))
        consumer = s.track(SpscQueue(name=name, element_size=8))
        result = {}
        _run_threads(lambda: produce(producer, 0, 20000),
                     lambda: result.update(mismatches=consume(consumer, 20000)))
        assert result['mismatches'] == 0


def test_remove_shm_unlinks_name_but_keeps_mappings():
    local = SpscQueue(size=4, element_size=8)
    assert local.remove_shm() is False
    local.close()

    with Segments() as s:
        name = s.name("remove")
        coordinator = s.track(SpscQueue(name=name, size=4, element_size=8))
        peer = s.track(SpscQueue(name=name, element_size=8))
        assert coordinator.remove_shm() is True
        push(coordinator, 5)
        assert value_of(peer.read()) == 5
        if not IS_WINDOWS:
            # The name is gone, so the next creator starts a fresh segment. Windows has
            # no name to remove: the segment lives until its last handle closes.
            fresh = s.track(SpscQueue(name=name, size=4, element_size=8))
            assert fresh._own
            assert fresh.read() is None


def test_context_manager_closes_but_keeps_segment():
    with Segments() as s:
        name = s.name("ctx")
        keeper = s.track(SpscQueue(name=name, size=4, element_size=8))
        with SpscQueue(name=name, element_size=8) as producer:
            push(producer, 9)
        assert value_of(keeper.read()) == 9


def run_tests(title, scope):
    """Run every test_* function in scope (a module's globals()) and report."""
    tests = [(name, obj) for name, obj in sorted(scope.items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print(title)
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
    sys.exit(0 if run_tests("SpscQueue tests", globals()) else 1)
