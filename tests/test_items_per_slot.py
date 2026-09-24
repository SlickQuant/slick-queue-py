"""
Tests for items_per_slot: one control slot covering several elements, so a byte buffer
no longer carries a 16-byte control slot per byte (matching C++ slick-queue).

items_per_slot is the minimum number of elements a single reserve() consumes.
"""
import os
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from multiprocessing.shared_memory import SharedMemory

from atomic_ops import AtomicCursor
from slick_queue_py import (
    SlickQueue,
    QueueTraits,
    HEADER_SIZE,
    SLOT_SIZE,
    HEADER_MAGIC_OFFSET,
    ITEMS_PER_SLOT_OFFSET,
)

SLQ1 = 0x534C5131
SLQ2 = 0x534C5132
SLQ3 = 0x534C5133


class NoReadLast(QueueTraits):
    enable_read_last = False


def _marker(q):
    return struct.unpack_from("<I", q._buf, HEADER_MAGIC_OFFSET)[0]


def _set_raw_marker(q, marker):
    raw = SharedMemory(name=q._shm.name, create=False)
    try:
        struct.pack_into("<I", raw.buf, HEADER_MAGIC_OFFSET, marker)
    finally:
        raw.close()


def _unique(prefix):
    return f"{prefix}_{os.getpid()}"


def _write(q, index, payload: bytes):
    """Write a multi-element payload into a queue of 1-byte elements."""
    for k, b in enumerate(payload):
        q[index + k][0] = b


def _publish(q, payload: bytes) -> int:
    index = q.reserve(len(payload))
    _write(q, index, payload)
    q.publish(index, len(payload))
    return index


def _expect_value_error(fn, text):
    try:
        fn()
    except ValueError as e:
        assert text in str(e), str(e)
    else:
        raise AssertionError(f"expected ValueError containing {text!r}")


# ------------------------------------------------------------------ geometry


def test_default_is_one():
    q = SlickQueue(size=1024, element_size=8)
    try:
        assert q.items_per_slot == 1
        assert q.slot_count == 1024
    finally:
        q.close()


def test_shrinks_control_array():
    """The point of the feature: the buffer holds size // items_per_slot slots."""
    q = SlickQueue(size=4096, element_size=1, items_per_slot=64)
    try:
        assert q.items_per_slot == 64
        assert q.slot_count == 64
        assert len(q._local_buf) == HEADER_SIZE + SLOT_SIZE * 64 + 4096
        assert struct.unpack_from("<I", q._buf, ITEMS_PER_SLOT_OFFSET)[0] == 64
    finally:
        q.close()


def test_rejects_bad_arguments():
    _expect_value_error(lambda: SlickQueue(size=64, element_size=1, items_per_slot=3),
                        "power of two")
    _expect_value_error(lambda: SlickQueue(size=64, element_size=1, items_per_slot=0),
                        "power of two")
    _expect_value_error(lambda: SlickQueue(size=64, element_size=1, items_per_slot=128),
                        "> queue size")
    SlickQueue(size=64, element_size=1, items_per_slot=64).close()


def test_data_array_padded_for_element_alignment():
    """Matches C++: with one 16-byte control slot the data array would start at 80,
    misaligned for a C++ alignas(32) element; it is padded to the lowest set bit of
    element_size. The default layout is never padded, so older peers still read it."""
    q = SlickQueue(size=64, element_size=32, items_per_slot=64)
    try:
        assert q.slot_count == 1
        assert q._data_offset == 96
        assert len(q._local_buf) == 96 + 32 * 64
    finally:
        q.close()

    # Byte elements need no padding at all.
    q = SlickQueue(size=64, element_size=1, items_per_slot=64)
    try:
        assert q._data_offset == HEADER_SIZE + SLOT_SIZE
    finally:
        q.close()

    # items_per_slot == 1 keeps the legacy offset even where it is not a multiple of 32.
    q = SlickQueue(size=1, element_size=32)
    try:
        assert q._data_offset == HEADER_SIZE + SLOT_SIZE
    finally:
        q.close()


def test_shm_padded_layout_round_trips():
    name = _unique("slq_ips_pad")
    creator = SlickQueue(name=name, size=64, element_size=32, items_per_slot=64)
    try:
        opener = SlickQueue(name=name, element_size=32)
        try:
            assert opener._data_offset == 96
            cursor = 0
            for i in range(3):   # one unit in the ring: read each before the next
                idx = creator.reserve()
                creator[idx][:4] = (100 + i).to_bytes(4, 'little')
                creator.publish(idx)
                data, size, cursor = opener.read(cursor)
                assert int.from_bytes(data[:4], 'little') == 100 + i
        finally:
            opener.close()
    finally:
        creator.close()
        creator.unlink()


# ------------------------------------------------------------------ reserve/read


def test_rounds_reservations():
    """Anything up to 16 takes 16, a larger reservation takes whole multiples of it,
    and every index is therefore a multiple of 16."""
    q = SlickQueue(size=64, element_size=1, items_per_slot=16)
    try:
        assert q.reserve(3) == 0
        assert q.reserve(3) == 16
        assert q.reserve(20) == 32   # two units
        assert q.reserve(1) == 64
        assert q.reserve(16) == 80
    finally:
        q.close()


def test_read_advances_by_unit():
    """read() returns the published size but moves the cursor by what reserve()
    consumed, so reader and producer stay in lockstep."""
    q = SlickQueue(size=64, element_size=1, items_per_slot=16)
    try:
        messages = [b"abc", b"de", b"fghij"]
        for m in messages:
            _publish(q, m)

        cursor = 0
        for m in messages:
            before = cursor
            data, size, cursor = q.read(cursor)
            assert data == m
            assert size == len(m)
            assert cursor == before + 16
        data, size, cursor = q.read(cursor)
        assert data is None
        assert cursor == 48
    finally:
        q.close()


def test_multi_unit_message():
    """A message larger than the minimum spans several units under one control slot;
    the units it covers are skipped as a whole."""
    q = SlickQueue(size=128, element_size=1, items_per_slot=16)
    try:
        big = bytes((ord('a') + i % 26) for i in range(40))
        assert _publish(q, big) == 0
        assert _publish(q, b"ok") == 48

        data, size, cursor = q.read(0)
        assert data == big and size == 40 and cursor == 48
        data, size, cursor = q.read(cursor)
        assert data == b"ok" and cursor == 64
    finally:
        q.close()


def test_buffer_wrap():
    """A reservation that cannot fit in the tail leaves its wrap marker in the tail's
    control slot, and the reader follows it to the start of the ring."""
    q = SlickQueue(size=64, element_size=1, items_per_slot=16)
    try:
        cursor = 0
        for i in range(3):
            _publish(q, bytes([ord('0') + i]) * 16)
            data, size, cursor = q.read(cursor)
            assert data is not None
        assert cursor == 48

        wrapped = q.reserve(20)   # needs 32, only 16 left in the tail
        assert wrapped == 64
        _write(q, wrapped, b"wrapped-message-0123")

        data, size, cursor = q.read(cursor)
        assert data is None
        assert cursor == 64, "reader should follow the wrap marker"

        q.publish(wrapped, 20)
        data, size, cursor = q.read(cursor)
        assert data == b"wrapped-message-0123"
        assert cursor == 96
    finally:
        q.close()


def test_atomic_cursor_advances_by_unit():
    q = SlickQueue(size=64, element_size=1, items_per_slot=8)
    try:
        cursor = AtomicCursor(bytearray(8), 0)
        for n in (3, 9, 1):
            _publish(q, b"a" * n)

        expected = [(3, 8), (9, 24), (1, 32)]
        for size_expected, cursor_expected in expected:
            data, size, _ = q.read(cursor)
            assert size == size_expected
            assert cursor.load() == cursor_expected
        data, size, _ = q.read(cursor)
        assert data is None
    finally:
        q.close()


def test_read_last_after_wrap():
    q = SlickQueue(size=16, element_size=4, items_per_slot=4)
    try:
        for i in range(10):
            index = q.reserve(3)
            for k in range(3):
                q[index + k][:4] = (i * 10 + k).to_bytes(4, 'little')
            q.publish(index, 3)

        data, size = q.read_last()
        assert size == 3
        assert int.from_bytes(data[0:4], 'little') == 90
        assert int.from_bytes(data[8:12], 'little') == 92
    finally:
        q.close()


def test_reset_keeps_unit():
    q = SlickQueue(size=64, element_size=1, items_per_slot=16)
    try:
        for _ in range(6):
            _publish(q, b"12345")
        q.reset()
        assert q.items_per_slot == 16
        assert q.reserve(5) == 0
        assert q.reserve(5) == 16
    finally:
        q.close()


def test_loss_count_with_units():
    """A lapped reader still counts skipped elements, not units."""
    q = SlickQueue(size=64, element_size=1, items_per_slot=16)
    try:
        for i in range(8):   # two laps of four units
            _publish(q, bytes([i]))
        data, size, cursor = q.read(0)
        assert data == bytes([4])
        assert q.loss_count() == 64
    finally:
        q.close()


# ---------------------------------------------------------------- shared memory


def test_shm_attacher_adopts_items_per_slot():
    name = _unique("slq_ips_adopt")
    creator = SlickQueue(name=name, size=256, element_size=1, items_per_slot=16)
    try:
        opener = SlickQueue(name=name, element_size=1)
        try:
            assert opener.size == 256
            assert opener.items_per_slot == 16
            assert opener.slot_count == 16

            messages = [b"hello", b"a message longer than sixteen bytes", b"x"]
            for m in messages:
                _publish(creator, m)

            cursor = 0
            for m in messages:
                data, size, cursor = opener.read(cursor)
                assert data == m
            assert cursor == 16 + 48 + 16
            assert opener.read_last() == (b"x", 1)
        finally:
            opener.close()
    finally:
        creator.close()
        creator.unlink()


def test_shm_attacher_with_explicit_matching_value():
    name = _unique("slq_ips_explicit")
    creator = SlickQueue(name=name, size=64, element_size=1, items_per_slot=8)
    try:
        SlickQueue(name=name, element_size=1, items_per_slot=8).close()
        _expect_value_error(lambda: SlickQueue(name=name, element_size=1, items_per_slot=4),
                            "items_per_slot mismatch")
    finally:
        creator.close()
        creator.unlink()


def test_shm_create_or_open_mismatch_raises():
    """Same size, so this lands in the create-or-open branch and loses the init_state
    CAS - a different attach path from the opener above."""
    name = _unique("slq_ips_mismatch")
    creator = SlickQueue(name=name, size=256, element_size=1, items_per_slot=16)
    try:
        _expect_value_error(
            lambda: SlickQueue(name=name, size=256, element_size=1, items_per_slot=8),
            "items_per_slot mismatch")
        # the default is a mismatch too, not a silent reinterpretation of the layout
        _expect_value_error(
            lambda: SlickQueue(name=name, size=256, element_size=1),
            "items_per_slot mismatch")
    finally:
        creator.close()
        creator.unlink()


def test_zero_in_header_reads_as_one():
    """Segments created before the field existed left offset 28 zeroed. Build one by
    hand and confirm both attach paths treat it as items_per_slot=1."""
    name = _unique("slq_ips_legacy")
    creator = SlickQueue(name=name, size=8, element_size=8)
    try:
        raw = SharedMemory(name=creator._shm.name, create=False)
        try:
            struct.pack_into("<I", raw.buf, ITEMS_PER_SLOT_OFFSET, 0)
        finally:
            raw.close()

        opener = SlickQueue(name=name, element_size=8)
        try:
            assert opener.items_per_slot == 1
            SlickQueue(name=name, size=8, element_size=8).close()

            idx = creator.reserve()
            creator[idx][:8] = (11).to_bytes(8, 'little')
            creator.publish(idx)
            data, size, cursor = opener.read(0)
            assert int.from_bytes(data[:8], 'little') == 11
        finally:
            opener.close()
    finally:
        creator.close()
        creator.unlink()


# ---------------------------------------------------------------- layout marker


def test_marker_bit_set_only_when_items_per_slot_is_not_one():
    """Bit 1 is what turns away a peer built before items_per_slot existed: such a
    peer rejects any feature bit it does not know, so the bit must be set exactly when
    the layout differs from the one it assumes - never on a default queue."""
    queues = [
        (SlickQueue(size=64, element_size=1, items_per_slot=16), SLQ3),
        (SlickQueue(size=64, element_size=1), SLQ1),
        (SlickQueue(size=64, element_size=1, items_per_slot=16, traits=NoReadLast), SLQ2),
    ]
    try:
        for q, expected in queues:
            assert _marker(q) == expected, f"got 0x{_marker(q):08X}"
    finally:
        for q, _ in queues:
            q.close()


def test_shm_marker_bit_is_accepted_by_this_build():
    name = _unique("slq_ips_marker")
    creator = SlickQueue(name=name, size=64, element_size=1, items_per_slot=16)
    try:
        assert _marker(creator) == SLQ3
        opener = SlickQueue(name=name, element_size=1)
        opener.close()
        SlickQueue(name=name, size=64, element_size=1, items_per_slot=16).close()
    finally:
        creator.close()
        creator.unlink()


def _expect_corrupt(name):
    try:
        SlickQueue(name=name, element_size=1)
    except RuntimeError as e:
        assert "disagrees with its items_per_slot field" in str(e), str(e)
    else:
        raise AssertionError("expected RuntimeError for a marker contradicting the field")


def test_shm_marker_bit_disagreeing_with_field_raises():
    """The field is authoritative and the bit only mirrors it; a segment where the
    two disagree was not written by this library and is refused."""
    name = _unique("slq_ips_corrupt_a")
    unit = SlickQueue(name=name, size=64, element_size=1, items_per_slot=16)
    try:
        _set_raw_marker(unit, SLQ1)   # bit cleared, field says 16
        _expect_corrupt(name)
    finally:
        unit.close()
        unit.unlink()

    name = _unique("slq_ips_corrupt_b")
    plain = SlickQueue(name=name, size=64, element_size=1)
    try:
        _set_raw_marker(plain, SLQ3)  # bit set, field says 1
        _expect_corrupt(name)
    finally:
        plain.close()
        plain.unlink()


def run_all_tests():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print("items_per_slot tests")
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
