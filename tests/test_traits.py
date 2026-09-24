"""
Tests for the traits-based feature configuration and the shared-memory layout
marker introduced in v2.0.0 (matching C++ slick-queue v2.0.0).
"""
import os
import struct
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from multiprocessing.shared_memory import SharedMemory

from slick_queue_py import (
    SlickQueue,
    QueueTraits,
    default_queue_traits,
    validate_traits,
    HEADER_SIZE,
    HEADER_MAGIC,
    HEADER_MAGIC_OFFSET,
    SIZE_OFFSET,
    SLOT_SIZE,
    INIT_STATE_OFFSET,
    INIT_STATE_READY,
    K_INVALID_INDEX,
)


class NoReadLast(QueueTraits):
    enable_read_last = False


class ResetCheck(QueueTraits):
    enable_reset_check = True


class NoLossDetection(QueueTraits):
    enable_loss_detection = False


class LossDetection(QueueTraits):
    enable_loss_detection = True


SLQ0 = HEADER_MAGIC & ~0x1  # 'SLQ0' - last-published index not maintained


def _unique(prefix):
    return f"{prefix}_{os.getpid()}"


# ---------------------------------------------------------------- traits type


def test_default_traits_values():
    """There is one traits type and one default. Loss detection is on, unlike the
    C++ Release default, and is not keyed off __debug__ - see QueueTraits."""
    assert default_queue_traits is QueueTraits
    assert default_queue_traits.enable_read_last is True
    assert default_queue_traits.enable_reset_check is False
    assert default_queue_traits.enable_cpu_relax is True
    assert default_queue_traits.enable_loss_detection is True

    # the counter must not depend on the interpreter's -O flag
    import slick_queue_py
    assert not hasattr(slick_queue_py, "DebugQueueTraits"), \
        "DebugQueueTraits is C++ ODR scaffolding and has no purpose here"


def test_validate_traits_rejects_missing_attribute():
    class Incomplete:
        enable_read_last = True

    try:
        validate_traits(Incomplete)
    except TypeError as e:
        assert "enable_reset_check" in str(e)
    else:
        raise AssertionError("expected TypeError for a traits type missing an attribute")


def test_validate_traits_rejects_non_bool():
    class WrongType(QueueTraits):
        enable_cpu_relax = 1  # truthy, but not a bool

    try:
        validate_traits(WrongType)
    except TypeError as e:
        assert "enable_cpu_relax" in str(e)
        assert "bool" in str(e)
    else:
        raise AssertionError("expected TypeError for a non-bool trait")


def test_constructor_rejects_malformed_traits():
    class Bogus:
        pass

    try:
        SlickQueue(size=8, element_size=8, traits=Bogus)
    except TypeError:
        pass
    else:
        raise AssertionError("expected TypeError for a malformed traits type")


# ------------------------------------------------- snapshot at construction


class _Mutable(QueueTraits):
    """A traits type the tests flip after a queue has been built from it."""


def _restore(cls, **values):
    for name, value in values.items():
        setattr(cls, name, value)


def test_traits_attribute_is_a_read_only_snapshot():
    """q.traits describes what the queue actually does, and cannot be rewritten to
    say otherwise."""
    q = SlickQueue(size=8, element_size=8)
    try:
        assert q.traits.enable_read_last is True

        try:
            q.traits.enable_read_last = False
        except AttributeError as e:
            assert "read-only" in str(e)
        else:
            raise AssertionError("q.traits should be read-only")

        try:
            del q.traits.enable_read_last
        except AttributeError:
            pass
        else:
            raise AssertionError("q.traits should not allow deletion")

        assert q.traits.enable_read_last is True
    finally:
        q.close()


def test_traits_snapshot_is_not_the_caller_class():
    """The queue must not retain the caller's class, or a later mutation would
    change its behaviour."""
    _Mutable.enable_read_last = True
    q = SlickQueue(size=8, element_size=8, traits=_Mutable)
    try:
        assert q.traits is not _Mutable
        _Mutable.enable_read_last = False
        assert q.traits.enable_read_last is True, \
            "the snapshot must not follow the class it was taken from"
    finally:
        _restore(_Mutable, enable_read_last=True)
        q.close()


def test_publish_survives_read_last_flipped_on_after_construction():
    """Flipping enable_read_last False->True used to raise AttributeError from
    publish(), because the last-published atomic was never created."""
    _Mutable.enable_read_last = False
    q = SlickQueue(size=16, element_size=8, traits=_Mutable)
    try:
        _Mutable.enable_read_last = True

        idx = q.reserve()
        q[idx][:8] = (7).to_bytes(8, 'little')
        q.publish(idx)                      # must not raise

        q.reset()                           # nor here

        # and read_last() still refuses, because this queue does not maintain it
        try:
            q.read_last()
        except RuntimeError as e:
            assert "enable_read_last" in str(e)
        else:
            raise AssertionError("read_last() should still refuse")
    finally:
        _restore(_Mutable, enable_read_last=True)
        q.close()


def test_publish_keeps_maintaining_index_when_flipped_off():
    """Flipping enable_read_last True->False used to stop advancing the index on a
    segment whose marker says 'SLQ1', silently freezing read_last() for every
    peer - exactly what the layout marker exists to prevent."""
    _Mutable.enable_read_last = True
    q = SlickQueue(size=16, element_size=8, traits=_Mutable)
    try:
        idx = q.reserve()
        q[idx][:8] = (1).to_bytes(8, 'little')
        q.publish(idx)
        assert int.from_bytes(q.read_last()[0][:8], 'little') == 1

        _Mutable.enable_read_last = False

        idx = q.reserve()
        q[idx][:8] = (2).to_bytes(8, 'little')
        q.publish(idx)

        data, size = q.read_last()
        assert int.from_bytes(data[:8], 'little') == 2, \
            "the segment is 'SLQ1'; publish() must keep advancing the index"
    finally:
        _restore(_Mutable, enable_read_last=True)
        q.close()


def test_reset_check_and_loss_detection_are_also_snapshotted():
    """The process-local traits are snapshotted too, so read() behaviour cannot
    change under a running consumer."""
    _Mutable.enable_reset_check = False
    _Mutable.enable_loss_detection = False
    q = SlickQueue(size=8, element_size=8, traits=_Mutable)
    try:
        _Mutable.enable_reset_check = True
        _Mutable.enable_loss_detection = True

        assert q.traits.enable_reset_check is False
        assert q.traits.enable_loss_detection is False

        # lap the queue: loss detection is off for this instance, so 0
        for i in range(24):
            idx = q.reserve()
            q[idx][:8] = i.to_bytes(8, 'little')
            q.publish(idx)
        data, size, cursor = q.read(0)
        assert data is not None
        assert q.loss_count() == 0, "loss detection was off when this queue was built"

        # reset check is off, so a stale cursor is not rewound
        q.reset()
        for i in range(3):
            idx = q.reserve()
            q[idx][:8] = (100 + i).to_bytes(8, 'little')
            q.publish(idx)
        data, size, cursor2 = q.read(cursor)
        assert data is None, "reset check was off when this queue was built"
    finally:
        _restore(_Mutable, enable_reset_check=False, enable_loss_detection=False)
        q.close()


def test_shm_marker_matches_the_snapshot_not_the_mutated_class():
    """An attacher validates against the snapshot it took, so a class mutated
    between the two constructions cannot smuggle a mismatch past the check."""
    name = _unique("slq_tr_snap")
    _Mutable.enable_read_last = True
    creator = SlickQueue(name=name, size=8, element_size=8, traits=_Mutable)
    try:
        magic = struct.unpack_from("<I", creator._buf, HEADER_MAGIC_OFFSET)[0]
        assert magic == HEADER_MAGIC

        # the class now says False, so a queue built from it must be rejected
        _Mutable.enable_read_last = False
        try:
            SlickQueue(name=name, element_size=8, traits=_Mutable)
        except RuntimeError as e:
            assert "feature mismatch" in str(e), str(e)
        else:
            raise AssertionError("expected the attacher to be rejected")

        # while the creator, holding its snapshot, is unaffected
        assert creator.traits.enable_read_last is True
        idx = creator.reserve()
        creator[idx][:8] = (5).to_bytes(8, 'little')
        creator.publish(idx)
        assert int.from_bytes(creator.read_last()[0][:8], 'little') == 5
    finally:
        _restore(_Mutable, enable_read_last=True)
        creator.close()
        creator.unlink()


# ------------------------------------------------------------- read_last gate


def test_read_last_requires_the_trait():
    """read_last() is an error without enable_read_last - there is no fallback to
    the old reserved-cursor heuristic."""
    q = SlickQueue(size=16, element_size=8, traits=NoReadLast)
    try:
        idx = q.reserve()
        q[idx][:8] = (7).to_bytes(8, 'little')
        q.publish(idx)

        try:
            q.read_last()
        except RuntimeError as e:
            assert "enable_read_last" in str(e)
        else:
            raise AssertionError("expected RuntimeError from read_last()")

        # the queue is otherwise fully functional
        data, size, cursor = q.read(0)
        assert data is not None
        assert int.from_bytes(data[:8], 'little') == 7
    finally:
        q.close()


def test_disabled_read_last_drops_the_atomic():
    q = SlickQueue(size=8, element_size=8, traits=NoReadLast)
    try:
        assert q._atomic_last_published is None
    finally:
        q.close()


# ---------------------------------------------------------------- loss counter


def test_loss_count_zero_when_disabled():
    """With loss detection off the counter is not maintained and reads back 0,
    even after a producer laps the reader."""
    q = SlickQueue(size=8, element_size=8, traits=NoLossDetection)
    try:
        for i in range(24):  # three laps of an 8-slot queue
            idx = q.reserve()
            q[idx][:8] = i.to_bytes(8, 'little')
            q.publish(idx)

        data, size, cursor = q.read(0)
        assert data is not None          # the reader was overrun
        assert cursor > 8
        assert q.loss_count() == 0
    finally:
        q.close()


def test_loss_count_counted_when_enabled():
    q = SlickQueue(size=8, element_size=8, traits=LossDetection)
    try:
        for i in range(24):
            idx = q.reserve()
            q[idx][:8] = i.to_bytes(8, 'little')
            q.publish(idx)

        data, size, cursor = q.read(0)
        assert data is not None
        assert q.loss_count() > 0
    finally:
        q.close()


# ------------------------------------------------------------- layout marker


def test_marker_records_read_last_on():
    q = SlickQueue(size=8, element_size=8)
    try:
        magic = struct.unpack_from("<I", q._buf, HEADER_MAGIC_OFFSET)[0]
        assert magic == HEADER_MAGIC, f"expected SLQ1, got 0x{magic:08X}"
    finally:
        q.close()


def test_marker_records_read_last_off():
    q = SlickQueue(size=8, element_size=8, traits=NoReadLast)
    try:
        magic = struct.unpack_from("<I", q._buf, HEADER_MAGIC_OFFSET)[0]
        assert magic == SLQ0, f"expected SLQ0, got 0x{magic:08X}"
    finally:
        q.close()


def test_attach_rejects_creator_with_read_last_off():
    """Creator maintains the index, attacher does not: the attacher would freeze
    read_last() for the creator, so it is rejected."""
    name = _unique("slq_tr_on_off")
    creator = SlickQueue(name=name, size=8, element_size=8)
    try:
        try:
            SlickQueue(name=name, element_size=8, traits=NoReadLast)
        except RuntimeError as e:
            msg = str(e)
            assert "feature mismatch" in msg, msg
            assert "enable_read_last=True" in msg, msg
            assert "enable_read_last=False" in msg, msg
        else:
            raise AssertionError("expected RuntimeError on enable_read_last mismatch")
    finally:
        creator.close()
        creator.unlink()


def test_attach_rejects_creator_with_read_last_on_mismatch():
    """The reverse direction: the creator does not maintain the index, so an
    attacher that expects it would read a counter nobody writes."""
    name = _unique("slq_tr_off_on")
    creator = SlickQueue(name=name, size=8, element_size=8, traits=NoReadLast)
    try:
        try:
            SlickQueue(name=name, element_size=8)
        except RuntimeError as e:
            msg = str(e)
            assert "feature mismatch" in msg, msg
            assert "enable_read_last=False" in msg, msg
            assert "enable_read_last=True" in msg, msg
        else:
            raise AssertionError("expected RuntimeError on enable_read_last mismatch")
    finally:
        creator.close()
        creator.unlink()


def test_create_or_open_path_also_validates_the_marker():
    """The mismatch is caught on the create-or-open constructor too, not only on
    the open-existing one - they are separate attach paths."""
    name = _unique("slq_tr_create_open")
    creator = SlickQueue(name=name, size=8, element_size=8)
    try:
        try:
            # same size, so this lands in the create-or-open branch and loses the
            # init_state CAS rather than taking the opener constructor
            SlickQueue(name=name, size=8, element_size=8, traits=NoReadLast)
        except RuntimeError as e:
            assert "feature mismatch" in str(e), str(e)
        else:
            raise AssertionError("expected RuntimeError on enable_read_last mismatch")
    finally:
        creator.close()
        creator.unlink()


def test_attach_allows_differing_local_traits():
    """The other traits are process-local and mix freely on one segment."""
    name = _unique("slq_tr_local")
    creator = SlickQueue(name=name, size=8, element_size=8, traits=LossDetection)
    try:
        opener = SlickQueue(name=name, element_size=8, traits=ResetCheck)
        try:
            idx = creator.reserve()
            creator[idx][:8] = (42).to_bytes(8, 'little')
            creator.publish(idx)

            data, size = opener.read_last()
            assert data is not None
            assert int.from_bytes(data[:8], 'little') == 42
            assert opener.loss_count() == 0  # opener has loss detection off
        finally:
            opener.close()
    finally:
        creator.close()
        creator.unlink()


def test_differently_configured_queues_coexist():
    """Two differently configured queues in one program, on separate segments."""
    lean = SlickQueue(size=8, element_size=8, traits=NoReadLast)
    standard = SlickQueue(size=8, element_size=8)
    try:
        for q in (lean, standard):
            idx = q.reserve()
            q[idx][:8] = (1).to_bytes(8, 'little')
            q.publish(idx)

        assert standard.read_last()[0] is not None
        try:
            lean.read_last()
        except RuntimeError:
            pass
        else:
            raise AssertionError("lean queue should refuse read_last()")
    finally:
        lean.close()
        standard.close()


# --------------------------------------------------- hand-built raw segments


def _make_raw_segment(name, size, element_size, magic):
    """Build a segment by hand so a marker the library never writes can be tested."""
    total = HEADER_SIZE + SLOT_SIZE * size + element_size * size
    shm = SharedMemory(name=name, create=True, size=total)
    buf = shm.buf
    buf[:HEADER_SIZE] = bytes(HEADER_SIZE)
    struct.pack_into("<I I", buf, SIZE_OFFSET, size, element_size)
    if magic is not None:
        struct.pack_into("<I", buf, HEADER_MAGIC_OFFSET, magic)
    for i in range(size):
        struct.pack_into("<Q I 4x", buf, HEADER_SIZE + i * SLOT_SIZE, K_INVALID_INDEX, 1)
    struct.pack_into("<I", buf, INIT_STATE_OFFSET, INIT_STATE_READY)
    return shm


def test_attach_rejects_segment_without_marker():
    """Segments created before v1.4.0 carry no layout marker and are no longer
    served by the reserved-cursor fallback."""
    name = _unique("slq_tr_nomark")
    shm = _make_raw_segment(name, 8, 8, magic=None)
    try:
        try:
            SlickQueue(name=name, element_size=8)
        except RuntimeError as e:
            msg = str(e)
            assert "layout marker" in msg, msg
            assert "v1.4.0" in msg, msg
        else:
            raise AssertionError("expected RuntimeError for an unmarked segment")
    finally:
        shm.close()
        try:
            shm.unlink()
        except Exception:
            pass


def test_attach_rejects_unknown_feature_bits():
    """A marker carrying a feature bit this build does not know about is rejected
    rather than guessed at. Bit 2 - bit 1 now means items_per_slot != 1."""
    name = _unique("slq_tr_unknown")
    shm = _make_raw_segment(name, 8, 8, magic=HEADER_MAGIC | 0x4)  # 'SLQ5'
    try:
        try:
            SlickQueue(name=name, element_size=8)
        except RuntimeError as e:
            msg = str(e)
            assert "unknown layout features" in msg, msg
        else:
            raise AssertionError("expected RuntimeError for unknown feature bits")
    finally:
        shm.close()
        try:
            shm.unlink()
        except Exception:
            pass


# ------------------------------------------------------------------ reserve()


def test_reserve_zero_raises():
    q = SlickQueue(size=8, element_size=8)
    try:
        try:
            q.reserve(0)
        except ValueError as e:
            assert "must be > 0" in str(e)
        else:
            raise AssertionError("expected ValueError from reserve(0)")
    finally:
        q.close()


def run_all_tests():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print("Traits and layout marker tests")
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
