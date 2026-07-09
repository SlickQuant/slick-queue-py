"""
Tests for loss_count() and initial_reading_index() (added in v1.2.0,
matching C++ slick-queue v1.5.0 queue.h:230-244).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue
from atomic_ops import AtomicCursor


def test_initial_reading_index_new_queue():
    """A newly created queue starts writing at index 0."""
    q = SlickQueue(size=16, element_size=8)
    assert q.initial_reading_index() == 0
    q.close()


def test_initial_reading_index_after_publish():
    """initial_reading_index equals the current writing index."""
    q = SlickQueue(size=16, element_size=8)
    for i in range(5):
        idx = q.reserve()
        q[idx][:8] = i.to_bytes(8, 'little')
        q.publish(idx)
    assert q.initial_reading_index() == 5

    # a late joiner starting there sees nothing until a new publish
    cursor = q.initial_reading_index()
    data, size, cursor = q.read(cursor)
    assert data is None

    idx = q.reserve()
    q[idx][:8] = (99).to_bytes(8, 'little')
    q.publish(idx)
    data, size, cursor = q.read(cursor)
    assert data is not None
    assert int.from_bytes(data[:8], 'little') == 99
    q.close()


def test_initial_reading_index_shm_opener():
    """An opener of an existing shm queue sees the creator's write index."""
    creator = SlickQueue(name="slq_test_init_idx", size=16, element_size=8)
    try:
        for i in range(3):
            idx = creator.reserve()
            creator[idx][:8] = i.to_bytes(8, 'little')
            creator.publish(idx)

        opener = SlickQueue(name="slq_test_init_idx", element_size=8)
        try:
            assert opener.initial_reading_index() == 3
        finally:
            opener.close()
    finally:
        creator.close()
        creator.unlink()


def test_no_loss_on_normal_reads():
    """Sequential reads with no overwrite never count loss."""
    q = SlickQueue(size=16, element_size=8)
    for i in range(10):
        idx = q.reserve()
        q[idx][:8] = i.to_bytes(8, 'little')
        q.publish(idx)

    cursor = 0
    for i in range(10):
        data, size, cursor = q.read(cursor)
        assert int.from_bytes(data[:8], 'little') == i
    assert q.loss_count() == 0
    q.close()


def test_single_consumer_loss_on_lap():
    """A lapped consumer skips overwritten items and counts them as loss
    (C++ queue.h:357-361)."""
    q = SlickQueue(size=4, element_size=8)
    for i in range(8):  # laps the 4-slot queue once
        idx = q.reserve()
        q[idx][:8] = i.to_bytes(8, 'little')
        q.publish(idx)

    # slot 0 now holds item 4; a cursor at 0 must return item 4 and count 4 lost
    data, size, cursor = q.read(0)
    assert data is not None
    assert int.from_bytes(data[:8], 'little') == 4
    assert q.loss_count() == 4, f"expected loss 4, got {q.loss_count()}"
    assert cursor == 5

    for i in range(5, 8):
        data, size, cursor = q.read(cursor)
        assert int.from_bytes(data[:8], 'little') == i
    assert q.loss_count() == 4  # no further loss
    q.close()


def test_atomic_cursor_loss_on_lap():
    """Work-stealing consumers count loss when claiming a lapped slot
    (C++ queue.h:406-426)."""
    q = SlickQueue(size=4, element_size=8)
    for i in range(8):
        idx = q.reserve()
        q[idx][:8] = i.to_bytes(8, 'little')
        q.publish(idx)

    cursor_buf = bytearray(8)
    cursor = AtomicCursor(cursor_buf, 0)
    cursor.store(0)

    data, size, index = q.read(cursor)
    assert data is not None
    assert int.from_bytes(data[:8], 'little') == 4
    assert q.loss_count() == 4, f"expected loss 4, got {q.loss_count()}"

    remaining = []
    while True:
        data, size, index = q.read(cursor)
        if data is None:
            break
        remaining.append(int.from_bytes(data[:8], 'little'))
    assert remaining == [5, 6, 7]
    assert q.loss_count() == 4
    cursor.release()
    q.close()


def test_reset_clears_loss_count():
    """reset() zeroes the loss counter (C++ queue.h:474-476)."""
    q = SlickQueue(size=4, element_size=8)
    for i in range(8):
        idx = q.reserve()
        q[idx][:8] = i.to_bytes(8, 'little')
        q.publish(idx)
    q.read(0)
    assert q.loss_count() == 4

    q.reset()
    assert q.loss_count() == 0
    q.close()


def run_all_tests():
    tests = [
        ("InitialReadingIndexNewQueue", test_initial_reading_index_new_queue),
        ("InitialReadingIndexAfterPublish", test_initial_reading_index_after_publish),
        ("InitialReadingIndexShmOpener", test_initial_reading_index_shm_opener),
        ("NoLossOnNormalReads", test_no_loss_on_normal_reads),
        ("SingleConsumerLossOnLap", test_single_consumer_loss_on_lap),
        ("AtomicCursorLossOnLap", test_atomic_cursor_loss_on_lap),
        ("ResetClearsLossCount", test_reset_clears_loss_count),
    ]

    print("=" * 70)
    print("Running Loss Count / Initial Reading Index Tests")
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
