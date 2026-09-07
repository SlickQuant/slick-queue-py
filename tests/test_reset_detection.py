"""
Tests for reset() recovery, gated on the enable_reset_check trait (v2.0.0,
matching C++ slick-queue v2.0.0).

The predicate under test is on the reader's *cursor*, not on the slot it lands
on: reset() clears the control array before rewinding the reservation counter,
so a stale reader always finds a slot holding either an invalid index or a fresh
low one - neither ahead of the counter. These tests fail against a slot-based
predicate, which leaves the reader returning None forever and then skipping the
start of the new generation.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue, QueueTraits
from atomic_ops import AtomicCursor


class ResetCheck(QueueTraits):
    enable_reset_check = True


def _unique(prefix):
    return f"{prefix}_{os.getpid()}"


def _publish(q, values):
    for v in values:
        idx = q.reserve()
        q[idx][:8] = v.to_bytes(8, 'little')
        q.publish(idx)


def _drain(q, cursor):
    """Read until empty, returning (values, cursor)."""
    values = []
    while True:
        data, size, cursor = q.read(cursor)
        if data is None:
            return values, cursor
        values.append(int.from_bytes(data[:8], 'little'))


def _drain_atomic(q, cursor):
    values = []
    while True:
        data, size, _ = q.read(cursor)
        if data is None:
            return values
        values.append(int.from_bytes(data[:8], 'little'))


# --------------------------------------------------------- single-consumer


def test_stale_cursor_rewinds_onto_new_generation():
    """A reader holding a pre-reset() cursor must rewind and see the new
    generation from its start."""
    q = SlickQueue(size=16, element_size=8, traits=ResetCheck)
    try:
        _publish(q, range(5))
        values, cursor = _drain(q, 0)
        assert values == [0, 1, 2, 3, 4]
        assert cursor == 5

        q.reset()
        _publish(q, [100, 101, 102])

        # the stale cursor is 5, past the reset counter of 3
        values, cursor = _drain(q, cursor)
        assert values == [100, 101, 102], values
        assert cursor == 3
    finally:
        q.close()


def test_stale_cursor_rewinds_when_empty_after_reset():
    """Rewinding must happen even when there is nothing to read yet, otherwise
    the reader skips the start of the new generation when it does arrive."""
    q = SlickQueue(size=16, element_size=8, traits=ResetCheck)
    try:
        _publish(q, range(5))
        values, cursor = _drain(q, 0)
        assert cursor == 5

        q.reset()

        # empty queue: no data, but the cursor must have rewound to 0
        data, size, cursor = q.read(cursor)
        assert data is None
        assert cursor == 0, f"cursor should rewind to 0, got {cursor}"

        # and the new generation is then read from its start
        _publish(q, [7, 8])
        values, cursor = _drain(q, cursor)
        assert values == [7, 8], values
    finally:
        q.close()


def test_reset_check_is_opt_in():
    """Without the trait a stale cursor is not rewound - the cost is only paid
    by callers that ask for it."""
    q = SlickQueue(size=16, element_size=8)  # default traits: check off
    try:
        _publish(q, range(5))
        values, cursor = _drain(q, 0)
        assert cursor == 5

        q.reset()
        _publish(q, [100, 101, 102])

        data, size, cursor = q.read(cursor)
        assert data is None, "default traits should not rewind a stale cursor"
        assert cursor == 5
    finally:
        q.close()


def test_stale_cursor_rewinds_shared_memory():
    name = _unique("slq_reset_shm")
    q = SlickQueue(name=name, size=16, element_size=8, traits=ResetCheck)
    try:
        _publish(q, range(5))
        values, cursor = _drain(q, 0)
        assert cursor == 5

        q.reset()
        _publish(q, [200, 201])

        values, cursor = _drain(q, cursor)
        assert values == [200, 201], values
        assert cursor == 2
    finally:
        q.close()
        q.unlink()


# ----------------------------------------------------------- shared cursor


def test_atomic_cursor_rewinds_onto_new_generation():
    q = SlickQueue(size=16, element_size=8, traits=ResetCheck)
    cursor = AtomicCursor(bytearray(8), 0)
    try:
        _publish(q, range(5))
        assert _drain_atomic(q, cursor) == [0, 1, 2, 3, 4]
        assert cursor.load() == 5

        q.reset()
        _publish(q, [100, 101, 102])

        assert _drain_atomic(q, cursor) == [100, 101, 102]
        assert cursor.load() == 3
    finally:
        q.close()


def test_atomic_cursor_rewinds_when_empty_after_reset():
    q = SlickQueue(size=16, element_size=8, traits=ResetCheck)
    cursor = AtomicCursor(bytearray(8), 0)
    try:
        _publish(q, range(5))
        _drain_atomic(q, cursor)
        assert cursor.load() == 5

        q.reset()

        data, size, _ = q.read(cursor)
        assert data is None
        assert cursor.load() == 0, f"cursor should rewind to 0, got {cursor.load()}"
    finally:
        q.close()


def test_atomic_cursor_rewind_does_not_clobber_a_peer():
    """The rewind goes through a CAS, so a peer consumer that has already moved
    the shared cursor on is not thrown back to 0."""
    q = SlickQueue(size=16, element_size=8, traits=ResetCheck)
    cursor = AtomicCursor(bytearray(8), 0)
    try:
        _publish(q, range(5))
        _drain_atomic(q, cursor)
        assert cursor.load() == 5

        q.reset()
        _publish(q, [100, 101, 102])

        # a peer consumer already claimed the first record of the new generation
        cursor.store(1)

        data, size, index = q.read(cursor)
        assert data is not None
        assert int.from_bytes(data[:8], 'little') == 101, "should not have rewound to 0"
        assert index == 1
    finally:
        q.close()


def test_atomic_cursor_rewinds_shared_memory():
    name = _unique("slq_reset_ac_shm")
    q = SlickQueue(name=name, size=16, element_size=8, traits=ResetCheck)
    cursor = AtomicCursor(bytearray(8), 0)
    try:
        _publish(q, range(5))
        _drain_atomic(q, cursor)
        assert cursor.load() == 5

        q.reset()
        _publish(q, [200, 201])

        assert _drain_atomic(q, cursor) == [200, 201]
    finally:
        q.close()
        q.unlink()


# ------------------------------------------------------------------ reset()


def test_reset_rewinds_reservation_counter_in_local_mode():
    """reset() rewinds the reservation counter in local memory mode too, not
    only in shared memory."""
    q = SlickQueue(size=16, element_size=8)
    try:
        _publish(q, range(5))
        assert q.initial_reading_index() == 5

        q.reset()
        assert q.initial_reading_index() == 0

        idx = q.reserve()
        assert idx == 0, f"first reservation after reset should be 0, got {idx}"
    finally:
        q.close()


def run_all_tests():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print("Reset detection tests")
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
