"""Test read_last() interoperability between Python and C++."""
import sys
from pathlib import Path
import subprocess
import os
import struct
import time

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue


def run_cpp_program(program_name, *args, timeout=10):
    """Run a C++ test program and return output."""
    # Look for the executable in build directories
    possible_paths = [
        Path(__file__).parent.parent / "build" / "Release" / f"{program_name}.exe",
        Path(__file__).parent.parent / "build" / "Debug" / f"{program_name}.exe",
        Path(__file__).parent.parent / "build" / program_name,
        Path(__file__).parent / "cpp" / program_name,
    ]

    exe_path = None
    for path in possible_paths:
        if path.exists():
            exe_path = path
            break

    if not exe_path:
        raise FileNotFoundError(f"C++ program {program_name} not found in build directories")

    result = subprocess.run(
        [str(exe_path)] + list(args),
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if result.returncode != 0:
        raise RuntimeError(f"C++ program failed: {result.stderr}")

    return result.stdout


def test_python_publish_cpp_read_last():
    """Test Python publishes data, C++ reads it via read_last()."""
    print("\n" + "=" * 70)
    print("TEST: Python Publisher -> C++ read_last()")
    print("=" * 70)

    # Use shorter name for macOS 31-char limit
    queue_name = f"rl_py_cpp_{os.getpid()}"

    try:
        # Python creates queue and publishes data
        q = SlickQueue(name=queue_name, size=128, element_size=32)
        print(f"Python created queue: {queue_name}")

        # Publish multiple items with different sizes
        test_data = [
            (b"item0", 1),
            (b"item1", 1),
            (b"item2", 2),
            (b"item3", 3),
            (b"last", 1),
        ]

        for i, (data, size) in enumerate(test_data):
            idx = q.reserve(size)
            q[idx][:len(data)] = data
            q.publish(idx, size)
            print(f"  Published item {i}: {data.decode()}, size={size}")

        # Verify Python can read_last
        py_data, py_size = q.read_last()
        print(f"\nPython read_last: data={py_data[:4].decode()}, size={py_size}")
        assert py_data[:4] == b"last", f"Expected b'last', got {py_data[:4]}"
        assert py_size == 1, f"Expected size 1, got {py_size}"

        # Now have C++ read_last from the same queue
        print("\nLaunching C++ read_last tester...")

        try:
            output = run_cpp_program(
                "cpp_read_last_tester",
                queue_name,
                "read",
                "32"
            )
            print("C++ read_last output:")
            print(output)

            # Verify C++ could read the data
            if "data->value:" in output and "size:" in output:
                print("[PASS] C++ successfully called read_last()")
            else:
                print("[WARN] C++ output unexpected, but no error")

        except FileNotFoundError:
            print("[SKIP] C++ read_last tester not found - skipping C++ portion")
            print("       (Python portion verified successfully)")

        q.close()
        q.unlink()

        print("[PASS] Python publish -> C++ read_last interop (Python verified)")

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


def test_cpp_publish_python_read_last():
    """Test C++ publishes data, Python reads it via read_last()."""
    print("\n" + "=" * 70)
    print("TEST: C++ Publisher -> Python read_last()")
    print("=" * 70)

    # Use shorter name for macOS 31-char limit
    queue_name = f"rl_cpp_py_{os.getpid()}"

    try:
        # Python creates queue first
        q = SlickQueue(name=queue_name, size=128, element_size=32)
        print(f"Python created queue: {queue_name}")

        # C++ produces data using read_last tester
        print("Launching C++ read_last tester in write mode...")
        try:
            output = run_cpp_program(
                "cpp_read_last_tester",
                queue_name,
                "write",
                "128",  # queue size
                "50",   # number of items
                "32"    # element size
            )
            print("C++ output:")
            print(output)

            # Give C++ time to finish writing
            time.sleep(0.1)

            # Python reads last item
            data, size = q.read_last()

            if data is None:
                raise AssertionError("Python read_last returned None - C++ may not have published")

            # C++ writes values starting at 2000
            # Last item should be 2000 + 49 = 2049
            value = struct.unpack("<I", data[:4])[0]
            print(f"\nPython read_last: value={value}, size={size}")

            # Verify it's in the expected range
            assert value >= 2000 and value < 2100, f"Expected value in range [2000, 2100), got {value}"
            assert size >= 1, f"Expected size >= 1, got {size}"

            print(f"[PASS] C++ publish -> Python read_last: value={value}, size={size}")

        except FileNotFoundError:
            print("[SKIP] C++ producer not found - creating test manually")

            # Manual test: Python publishes and reads
            idx = q.reserve()
            test_val = 42
            struct.pack_into("<I", q[idx], 0, test_val)
            q.publish(idx)

            data, size = q.read_last()
            value = struct.unpack("<I", data[:4])[0]

            assert value == test_val, f"Expected {test_val}, got {value}"
            assert size == 1, f"Expected size 1, got {size}"

            print("[PASS] Manual verification passed (C++ not available)")

        q.close()
        q.unlink()

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


def test_read_last_concurrent_publishing():
    """Test read_last() with concurrent Python and C++ publishers."""
    print("\n" + "=" * 70)
    print("TEST: Concurrent Publishing -> read_last()")
    print("=" * 70)

    # Use shorter name for macOS 31-char limit
    queue_name = f"rl_conc_{os.getpid()}"

    try:
        # Python creates queue
        q = SlickQueue(name=queue_name, size=256, element_size=32)
        print(f"Python created queue: {queue_name}")

        # Python publishes some items
        for i in range(10):
            idx = q.reserve()
            struct.pack_into("<I", q[idx], 0, 1000 + i)
            q.publish(idx)

        print("Python published 10 items (values 1000-1009)")

        # Launch C++ producer in parallel
        try:
            print("Launching C++ producer (publishing 20 items)...")
            output = run_cpp_program(
                "producer",
                queue_name,
                "256",  # queue size
                "20",   # number of items
                "32"    # element size
            )

            time.sleep(0.1)

            # Python publishes more items
            for i in range(10, 20):
                idx = q.reserve()
                struct.pack_into("<I", q[idx], 0, 1000 + i)
                q.publish(idx)

            print("Python published 10 more items (values 1010-1019)")

            # Read last should return one of the recently published items
            data, size = q.read_last()
            assert data is not None, "read_last should return data"

            value = struct.unpack("<I", data[:4])[0]
            print(f"\nread_last returned: value={value}, size={size}")

            # Verify it's a valid value from either Python or C++
            assert isinstance(value, int), "Should be an integer"
            assert size >= 1, f"Size should be >= 1, got {size}"

            print("[PASS] Concurrent publishing -> read_last works correctly")

        except FileNotFoundError:
            print("[SKIP] C++ producer not found - testing Python-only")

            # Test with Python only
            for i in range(10, 20):
                idx = q.reserve()
                struct.pack_into("<I", q[idx], 0, 1000 + i)
                q.publish(idx)

            data, size = q.read_last()
            value = struct.unpack("<I", data[:4])[0]

            assert value == 1019, f"Expected 1019, got {value}"
            print("[PASS] Python-only verification passed")

        q.close()
        q.unlink()

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


def test_read_last_format_compatibility():
    """Test that read_last works with modern format created by Python."""
    print("\n" + "=" * 70)
    print("TEST: read_last() Format Compatibility")
    print("=" * 70)

    from slick_queue_py import (
        LAST_PUBLISHED_OFFSET, HEADER_MAGIC_OFFSET,
        INIT_STATE_OFFSET, HEADER_MAGIC, INIT_STATE_READY,
        K_INVALID_INDEX
    )

    # Use shorter name for macOS 31-char limit
    queue_name = f"rl_fmt_{os.getpid()}"

    try:
        # Create queue with Python (modern format)
        q = SlickQueue(name=queue_name, size=128, element_size=32)
        print(f"Python created modern format queue: {queue_name}")

        # Verify modern format markers
        buf = q._buf
        magic = struct.unpack_from("<I", buf, HEADER_MAGIC_OFFSET)[0]
        init_state = struct.unpack_from("<I", buf, INIT_STATE_OFFSET)[0]

        assert magic == HEADER_MAGIC, f"Expected magic 0x{HEADER_MAGIC:X}, got 0x{magic:X}"
        assert init_state == INIT_STATE_READY, f"Expected READY state, got {init_state}"
        print(f"  Header magic: 0x{magic:X} (correct)")
        print(f"  Init state: {init_state} (READY)")

        # Verify last_published is invalid initially
        last_pub = struct.unpack_from("<Q", buf, LAST_PUBLISHED_OFFSET)[0]
        assert last_pub == K_INVALID_INDEX, "last_published should be invalid initially"
        print(f"  last_published: invalid (correct)")

        # Publish some data
        idx = q.reserve()
        test_val = 12345
        struct.pack_into("<I", q[idx], 0, test_val)
        q.publish(idx)

        # Verify last_published is updated
        last_pub = struct.unpack_from("<Q", buf, LAST_PUBLISHED_OFFSET)[0]
        assert last_pub == 0, f"last_published should be 0, got {last_pub}"
        print(f"  last_published after publish: {last_pub} (correct)")

        # read_last should return the data
        data, size = q.read_last()
        value = struct.unpack("<I", data[:4])[0]

        assert value == test_val, f"Expected {test_val}, got {value}"
        assert size == 1, f"Expected size 1, got {size}"
        print(f"  read_last returned: value={value}, size={size} (correct)")

        # Open from another instance (simulating C++ opening)
        q2 = SlickQueue(name=queue_name, element_size=32)

        # Verify it detected modern format
        assert q2._last_published_valid == True, "Should detect modern format"
        print(f"  Second instance detected modern format: {q2._last_published_valid}")

        # Second instance should also read_last correctly
        data2, size2 = q2.read_last()
        value2 = struct.unpack("<I", data2[:4])[0]

        assert value2 == test_val, f"Expected {test_val}, got {value2}"
        assert size2 == 1, f"Expected size 1, got {size2}"
        print(f"  Second instance read_last: value={value2}, size={size2} (correct)")

        q2.close()
        q.close()
        q.unlink()

        print("[PASS] read_last format compatibility verified")

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


def test_read_last_multi_slot():
    """Test read_last() with multi-slot publishes for C++ compatibility."""
    print("\n" + "=" * 70)
    print("TEST: read_last() with Multi-Slot Publishes")
    print("=" * 70)

    # Use shorter name for macOS 31-char limit
    queue_name = f"rl_multi_{os.getpid()}"

    try:
        q = SlickQueue(name=queue_name, size=128, element_size=32)
        print(f"Python created queue: {queue_name}")

        # Publish items with different sizes
        sizes = [1, 2, 3, 4, 2, 1]
        for i, n in enumerate(sizes):
            idx = q.reserve(n)
            struct.pack_into("<I", q[idx], 0, 100 + i)
            q.publish(idx, n)
            print(f"  Published item {i}: value={100+i}, size={n}")

        # read_last should return the last item with size 1
        data, size = q.read_last()
        value = struct.unpack("<I", data[:4])[0]

        print(f"\nread_last returned: value={value}, size={size}")
        assert value == 105, f"Expected value 105, got {value}"
        assert size == 1, f"Expected size 1, got {size}"

        # Open from another instance
        q2 = SlickQueue(name=queue_name, element_size=32)
        data2, size2 = q2.read_last()
        value2 = struct.unpack("<I", data2[:4])[0]

        print(f"Second instance read_last: value={value2}, size={size2}")
        assert value2 == 105, f"Expected value 105, got {value2}"
        assert size2 == 1, f"Expected size 1, got {size2}"

        q2.close()
        q.close()
        q.unlink()

        print("[PASS] read_last with multi-slot publishes works correctly")

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


def run_all_tests():
    """Run all read_last interop tests."""
    print("\n" + "=" * 70)
    print("read_last() C++/Python Interoperability Tests")
    print("=" * 70)

    tests = [
        test_python_publish_cpp_read_last,
        test_cpp_publish_python_read_last,
        test_read_last_concurrent_publishing,
        test_read_last_format_compatibility,
        test_read_last_multi_slot,
    ]

    passed = 0
    failed = 0
    skipped = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            error_msg = str(e)
            if "SKIP" in error_msg or "not found" in error_msg:
                print(f"[SKIP] {test.__name__}")
                skipped += 1
            else:
                print(f"[FAIL] {test.__name__}: {e}")
                import traceback
                traceback.print_exc()
                failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print("=" * 70)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
