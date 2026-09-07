"""Test modern format features and compatibility."""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue
import struct


def test_modern_format_header():
    """Verify modern format queue has correct header fields."""
    print("Testing modern format header structure...")

    q = SlickQueue(size=16, element_size=64)

    # Verify it maintains the last-published index
    assert q.traits.enable_read_last is True, "Queue should maintain last_published"
    assert q._atomic_last_published is not None, "Should have last_published atomic"

    # Check header constants
    from slick_queue_py import (
        HEADER_MAGIC, HEADER_MAGIC_OFFSET,
        INIT_STATE_OFFSET, INIT_STATE_READY,
        LAST_PUBLISHED_OFFSET, SIZE_OFFSET
    )

    # Read header fields directly from buffer
    buf = q._buf

    # Check magic number
    magic = struct.unpack_from("<I", buf, HEADER_MAGIC_OFFSET)[0]
    assert magic == HEADER_MAGIC, f"Magic should be 0x534C5131, got 0x{magic:X}"

    # Check init state
    init_state = struct.unpack_from("<I", buf, INIT_STATE_OFFSET)[0]
    assert init_state == INIT_STATE_READY, f"Init state should be READY (3), got {init_state}"

    # Check last_published is initialized to invalid
    last_pub = struct.unpack_from("<Q", buf, LAST_PUBLISHED_OFFSET)[0]
    K_INVALID_INDEX = 2**64 - 1
    assert last_pub == K_INVALID_INDEX, f"last_published should be invalid, got {last_pub}"

    # Check size and element_size
    size, elem_size = struct.unpack_from("<I I", buf, SIZE_OFFSET)
    assert size == 16, f"Size should be 16, got {size}"
    assert elem_size == 64, f"Element size should be 64, got {elem_size}"

    q.close()
    print("[PASS] Modern format header has all required fields")


def test_last_published_tracking():
    """Test that last_published atomic is updated correctly."""
    print("Testing last_published atomic tracking...")

    from slick_queue_py import LAST_PUBLISHED_OFFSET, K_INVALID_INDEX

    q = SlickQueue(size=16, element_size=64)

    # Initially should be invalid
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub == K_INVALID_INDEX, "Initially should be invalid"

    # Publish first item at index 0
    idx1 = q.reserve()
    data1 = b'one'
    q[idx1][:len(data1)] = data1
    q.publish(idx1)

    # last_published should now be 0
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub == 0, f"After first publish, should be 0, got {last_pub}"

    # Publish second item at index 1
    idx2 = q.reserve()
    data2 = b'two'
    q[idx2][:len(data2)] = data2
    q.publish(idx2)

    # last_published should now be 1
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub == 1, f"After second publish, should be 1, got {last_pub}"

    # Publish third item at index 2
    idx3 = q.reserve()
    data3 = b'thr'
    q[idx3][:len(data3)] = data3
    q.publish(idx3)

    # last_published should now be 2
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub == 2, f"After third publish, should be 2, got {last_pub}"

    q.close()
    print("[PASS] last_published atomic tracks correctly")


def test_memory_layout_matches_cpp():
    """Verify memory layout matches C++ exactly."""
    print("Testing memory layout compatibility with C++...")

    from slick_queue_py import (
        HEADER_SIZE, SLOT_SIZE, SLOT_SIZE_OFFSET,
        SIZE_OFFSET, ELEMENT_SIZE_OFFSET,
        LAST_PUBLISHED_OFFSET, HEADER_MAGIC_OFFSET,
        INIT_STATE_OFFSET, HEADER_MAGIC,
        HEADER_MAGIC_FEATURE_MASK, HEADER_MAGIC_READ_LAST
    )

    # Verify offsets match C++ queue.h:107-135
    assert SIZE_OFFSET == 8, "size_ offset should be 8"
    assert ELEMENT_SIZE_OFFSET == 12, "element_size offset should be 12"
    assert LAST_PUBLISHED_OFFSET == 16, "last_published offset should be 16"
    assert HEADER_MAGIC_OFFSET == 24, "header_magic offset should be 24"
    assert INIT_STATE_OFFSET == 48, "init_state offset should be 48"
    assert HEADER_SIZE == 64, "Header size should be 64 bytes"
    assert SLOT_SIZE == 16, "Slot size should be 16 bytes"
    assert SLOT_SIZE_OFFSET == 8, "slot.size offset within a slot should be 8"

    # The layout marker is part of the cross-process contract
    assert HEADER_MAGIC == 0x534C5131, "header magic should be 'SLQ1'"
    assert HEADER_MAGIC_FEATURE_MASK == 0x0000000F, "feature nibble should be the low 4 bits"
    assert HEADER_MAGIC_READ_LAST == 0x1, "read_last should be feature bit 0"
    assert HEADER_MAGIC & ~HEADER_MAGIC_READ_LAST == 0x534C5130, "cleared bit 0 should be 'SLQ0'"

    print("[PASS] Memory layout offsets match C++ implementation")


def test_shared_memory_modern_format():
    """Test shared memory queue uses modern format."""
    print("Testing shared memory with modern format...")

    import os

    queue_name = f"test_modern_{os.getpid()}"

    try:
        # Create queue
        q1 = SlickQueue(name=queue_name, size=8, element_size=32)
        assert q1.traits.enable_read_last is True, "Creator should maintain last_published"

        # Publish some data
        idx = q1.reserve()
        q1[idx][:4] = b'test'
        q1.publish(idx)

        # Open from another "process"
        q2 = SlickQueue(name=queue_name, element_size=32)
        assert q2.traits.enable_read_last is True, "Opener should maintain last_published"

        # Both should see the same data via read_last
        data1, size1 = q1.read_last()
        data2, size2 = q2.read_last()

        assert data1 == data2, "Both instances should see same data"
        assert size1 == size2 == 1, "Both should see size 1"
        assert data1[:4] == b'test', "Data should match"

        q2.close()
        q1.close()
        q1.unlink()

        print("[PASS] Shared memory uses modern format correctly")
    except Exception as e:
        # Cleanup on error
        try:
            from multiprocessing.shared_memory import SharedMemory
            shm = SharedMemory(name=queue_name, create=False)
            shm.close()
            shm.unlink()
        except:
            pass
        raise e


def test_cas_based_ownership():
    """Test that ownership is determined via CAS on init_state."""
    print("Testing CAS-based ownership detection...")

    import os
    from slick_queue_py import INIT_STATE_OFFSET, INIT_STATE_READY

    queue_name = f"test_cas_{os.getpid()}"

    try:
        # Create first instance
        q1 = SlickQueue(name=queue_name, size=8, element_size=32)
        assert q1._own == True, "First instance should be owner"

        # Check init_state is READY
        init_state = struct.unpack_from("<I", q1._buf, INIT_STATE_OFFSET)[0]
        assert init_state == INIT_STATE_READY, "Init state should be READY"

        # Open second instance (simulating another process opening existing)
        q2 = SlickQueue(name=queue_name, size=8, element_size=32)
        assert q2._own == False, "Second instance should not be owner"

        # Both should work correctly
        idx = q1.reserve()
        q1[idx][:4] = struct.pack("<I", 42)
        q1.publish(idx)

        data, size = q2.read_last()
        value = struct.unpack("<I", data[:4])[0]
        assert value == 42, "Second instance should read data from first"

        q2.close()
        q1.close()
        q1.unlink()

        print("[PASS] CAS-based ownership works correctly")
    except Exception as e:
        # Cleanup on error
        try:
            from multiprocessing.shared_memory import SharedMemory
            shm = SharedMemory(name=queue_name, create=False)
            shm.close()
            shm.unlink()
        except:
            pass
        raise e


def test_reset_clears_last_published():
    """Test that reset() clears last_published to invalid."""
    print("Testing reset clears last_published...")

    from slick_queue_py import LAST_PUBLISHED_OFFSET, K_INVALID_INDEX

    q = SlickQueue(size=8, element_size=32)

    # Publish something
    idx = q.reserve()
    q[idx][:4] = b'data'
    q.publish(idx)

    # Verify last_published is set
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub != K_INVALID_INDEX, "Should have valid last_published"

    # Reset
    q.reset()

    # Verify last_published is cleared
    last_pub = struct.unpack_from("<Q", q._buf, LAST_PUBLISHED_OFFSET)[0]
    assert last_pub == K_INVALID_INDEX, "Reset should clear last_published"

    # read_last should return (None, 0)
    data, size = q.read_last()
    assert data is None, "read_last should return None after reset"
    assert size == 0, "read_last should return size 0 after reset"

    q.close()
    print("[PASS] Reset clears last_published correctly")


def run_all_tests():
    """Run all modern format tests."""
    print("=" * 60)
    print("Running Modern Format Compatibility Tests")
    print("=" * 60)

    tests = [
        test_modern_format_header,
        test_last_published_tracking,
        test_memory_layout_matches_cpp,
        test_shared_memory_modern_format,
        test_cas_based_ownership,
        test_reset_clears_last_published,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
