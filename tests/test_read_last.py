"""Test read_last() functionality with new return signature."""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue
import struct


def test_read_last_empty_queue():
    """Test read_last on empty queue returns (None, 0)."""
    print("Testing read_last on empty queue...")

    q = SlickQueue(size=16, element_size=64)
    data, size = q.read_last()

    assert data is None, f"Expected None for empty queue, got {data}"
    assert size == 0, f"Expected size 0 for empty queue, got {size}"

    q.close()
    print("[PASS] read_last returns (None, 0) for empty queue")


def test_read_last_single_item():
    """Test read_last with a single published item."""
    print("Testing read_last with single item...")

    q = SlickQueue(size=16, element_size=64)

    # Publish one item
    idx = q.reserve()
    test_data = b'hello world'
    q[idx][:len(test_data)] = test_data
    q.publish(idx)

    # Read last should return that item with size 1
    data, size = q.read_last()
    assert data is not None, "Expected data, got None"
    assert data[:len(test_data)] == test_data, f"Data mismatch: expected {test_data}, got {data[:len(test_data)]}"
    assert size == 1, f"Expected size 1, got {size}"

    q.close()
    print("[PASS] read_last returns correct (data, 1) for single item")


def test_read_last_multiple_items():
    """Test read_last returns the most recent item after multiple publishes."""
    print("Testing read_last with multiple items...")

    q = SlickQueue(size=16, element_size=64)

    # Publish multiple items
    for i in range(5):
        idx = q.reserve()
        data = struct.pack("<I", i * 100)
        q[idx][:len(data)] = data
        q.publish(idx)

    # Read last should return the most recent (i=4, value=400)
    data, size = q.read_last()
    assert data is not None, "Expected data, got None"
    value = struct.unpack("<I", data[:4])[0]
    assert value == 400, f"Expected 400, got {value}"
    assert size == 1, f"Expected size 1, got {size}"

    q.close()
    print("[PASS] read_last returns most recent item after multiple publishes")


def test_read_last_multi_slot_publish():
    """Test read_last with multi-slot (n > 1) publish."""
    print("Testing read_last with multi-slot publish...")

    q = SlickQueue(size=16, element_size=64)

    # Publish with size 1
    idx1 = q.reserve()
    q[idx1][:5] = b'item1'
    q.publish(idx1)

    # Publish with size 3
    idx2 = q.reserve(3)
    q[idx2][:5] = b'item2'
    q.publish(idx2, 3)

    # Read last should return the second item with size 3
    data, size = q.read_last()
    assert data is not None, "Expected data, got None"
    assert data[:5] == b'item2', f"Expected b'item2', got {data[:5]}"
    assert size == 3, f"Expected size 3, got {size}"

    q.close()
    print("[PASS] read_last returns correct size for multi-slot publish")


def test_read_last_concurrent_publishes():
    """Test read_last with concurrent publishes from different positions."""
    print("Testing read_last with concurrent-style publishes...")

    q = SlickQueue(size=16, element_size=64)

    # Simulate out-of-order publishes (reserve multiple, publish in different order)
    idx1 = q.reserve()
    idx2 = q.reserve()
    idx3 = q.reserve()

    # Publish idx2 first
    test_data2 = b'sec'
    q[idx2][:len(test_data2)] = test_data2
    q.publish(idx2)

    # Publish idx3 next
    test_data3 = b'third'
    q[idx3][:len(test_data3)] = test_data3
    q.publish(idx3)

    # Publish idx1 last
    test_data1 = b'first'
    q[idx1][:len(test_data1)] = test_data1
    q.publish(idx1)

    # Read last should return idx3 (highest index published)
    data, size = q.read_last()
    assert data is not None, "Expected data, got None"
    # The last_published should track the maximum index published
    # Since idx3 > idx1, it should be idx3
    assert data[:5] == b'third', f"Expected b'third', got {data[:5]}"

    q.close()
    print("[PASS] read_last tracks maximum published index")


def test_read_last_after_reset():
    """Test read_last after queue reset."""
    print("Testing read_last after reset...")

    q = SlickQueue(size=16, element_size=64)

    # Publish some data
    idx = q.reserve()
    q[idx][:5] = b'data1'
    q.publish(idx)

    # Verify data exists
    data, size = q.read_last()
    assert data is not None, "Expected data before reset"

    # Reset queue
    q.reset()

    # Read last should return (None, 0) after reset
    data, size = q.read_last()
    assert data is None, f"Expected None after reset, got {data}"
    assert size == 0, f"Expected size 0 after reset, got {size}"

    q.close()
    print("[PASS] read_last returns (None, 0) after reset")


def test_read_last_shared_memory():
    """Test read_last with shared memory queue."""
    print("Testing read_last with shared memory...")

    import os
    import time

    queue_name = f"test_read_last_{os.getpid()}"

    try:
        # Create queue with shared memory
        q = SlickQueue(name=queue_name, size=16, element_size=64)

        # Should be empty initially
        data, size = q.read_last()
        assert data is None and size == 0, "Expected (None, 0) for new shared queue"

        # Publish data
        idx = q.reserve()
        test_data = b'shared memory test'
        q[idx][:len(test_data)] = test_data
        q.publish(idx)

        # Read last should return the data
        data, size = q.read_last()
        assert data is not None, "Expected data from shared memory"
        assert data[:len(test_data)] == test_data, "Data mismatch in shared memory"
        assert size == 1, f"Expected size 1, got {size}"

        # Open the same queue from "another process" (simulated)
        q2 = SlickQueue(name=queue_name, element_size=64)

        # Second instance should also see the data
        data2, size2 = q2.read_last()
        assert data2 is not None, "Second instance should see data"
        assert data2[:len(test_data)] == test_data, "Data mismatch in second instance"
        assert size2 == 1, f"Expected size 1 in second instance, got {size2}"

        q2.close()
        q.close()
        q.unlink()

        print("[PASS] read_last works correctly with shared memory")
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


def test_read_last_wraparound():
    """Test read_last behavior when queue wraps around."""
    print("Testing read_last with queue wraparound...")

    q = SlickQueue(size=8, element_size=32)

    # Fill the queue past capacity to cause wraparound
    for i in range(12):
        idx = q.reserve()
        data = struct.pack("<I", i)
        q[idx][:len(data)] = data
        q.publish(idx)

    # Read last should return the most recent item (i=11)
    data, size = q.read_last()
    assert data is not None, "Expected data after wraparound"
    value = struct.unpack("<I", data[:4])[0]
    assert value == 11, f"Expected 11 after wraparound, got {value}"
    assert size == 1, f"Expected size 1, got {size}"

    q.close()
    print("[PASS] read_last works correctly after wraparound")


def test_read_last_variable_sizes():
    """Test read_last with variable-sized publishes."""
    print("Testing read_last with variable sizes...")

    q = SlickQueue(size=16, element_size=64)

    # Publish items with different sizes
    sizes = [1, 2, 3, 1, 4, 2]
    for i, n in enumerate(sizes):
        idx = q.reserve(n)
        data = struct.pack("<I", i * 10)
        q[idx][:len(data)] = data
        q.publish(idx, n)

    # Read last should return the final item (i=5, size=2)
    data, size = q.read_last()
    assert data is not None, "Expected data"
    value = struct.unpack("<I", data[:4])[0]
    assert value == 50, f"Expected value 50, got {value}"
    assert size == 2, f"Expected size 2, got {size}"

    q.close()
    print("[PASS] read_last handles variable-sized publishes correctly")


def test_read_last_return_signature():
    """Verify read_last returns Tuple[Optional[bytes], int]."""
    print("Testing read_last return signature...")

    q = SlickQueue(size=16, element_size=64)

    # Empty queue
    result = q.read_last()
    assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
    assert len(result) == 2, f"Expected tuple of length 2, got {len(result)}"
    data, size = result
    assert data is None, "Expected None for empty queue"
    assert isinstance(size, int), f"Expected int for size, got {type(size)}"

    # With data
    idx = q.reserve()
    test_data = b'test'
    q[idx][:len(test_data)] = test_data
    q.publish(idx)

    result = q.read_last()
    assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
    assert len(result) == 2, f"Expected tuple of length 2, got {len(result)}"
    data, size = result
    assert isinstance(data, bytes), f"Expected bytes for data, got {type(data)}"
    assert isinstance(size, int), f"Expected int for size, got {type(size)}"

    q.close()
    print("[PASS] read_last has correct return signature")


def run_all_tests():
    """Run all read_last tests."""
    print("=" * 60)
    print("Running read_last() tests")
    print("=" * 60)

    tests = [
        test_read_last_empty_queue,
        test_read_last_single_item,
        test_read_last_multiple_items,
        test_read_last_multi_slot_publish,
        test_read_last_concurrent_publishes,
        test_read_last_after_reset,
        test_read_last_shared_memory,
        test_read_last_wraparound,
        test_read_last_variable_sizes,
        test_read_last_return_signature,
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
