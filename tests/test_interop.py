"""
Comprehensive C++/Python interoperability tests for SlickQueue.

Tests bidirectional communication between C++ and Python processes
to verify memory layout compatibility and atomic operations.
"""
import argparse
import os
import struct
import subprocess
import sys
import time
from multiprocessing import Process, Queue as MPQueue
from pathlib import Path
from queue import Empty
import random

# Add parent directory to path to import slick_queue_py
sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue


def get_result(results_queue, what, timeout=15):
    """Read one result, bounded.

    A child that was killed - or that died before reaching its own except
    handler - never queues anything, so an unbounded get() here turns a slow or
    stuck child into a test run that hangs forever rather than failing.
    """
    try:
        return results_queue.get(timeout=timeout)
    except Empty:
        raise AssertionError(
            f"{what} produced no result within {timeout}s; it was killed or died "
            f"before reporting"
        )


def find_cpp_executable(name):
    """Find C++ executable in build directory."""
    # Check common build directories
    search_paths = [
        Path(__file__).parent.parent / "build" / name,
        Path(__file__).parent.parent / "build" / "tests" / name,
        Path(__file__).parent.parent / "build" / f"{name}.exe",
        Path(__file__).parent.parent / "build" / "tests" / "Debug" / f"{name}.exe",
        Path(__file__).parent.parent / "build" / "tests" / "Release" / f"{name}.exe",
    ]

    for path in search_paths:
        if path.exists():
            return str(path)

    raise FileNotFoundError(f"C++ executable '{name}' not found. Please build the project first with CMake.")


def python_producer(queue_name, num_items, worker_id, results_queue, size=64):
    """Python producer process."""
    try:
        while not os.path.exists('ready'):
            time.sleep(0.001)

        time.sleep(random.uniform(0.01, 0.03))
    
        # On Windows, we need to specify size when opening existing shared memory
        q = SlickQueue(name=queue_name, size=size, element_size=32)

        for i in range(num_items):
            idx = q.reserve()
            data = struct.pack("<I I", worker_id, worker_id * 100 +i)
            slot = q[idx]
            slot[:len(data)] = data
            q.publish(idx)
            # print(f"PRODUCER {worker_id}: published item {worker_id * 100 + i} at index {idx}", flush=True)
            time.sleep(random.uniform(0.001, 0.003))

        print(f"Python producer {worker_id} completed.", flush=True)

        results_queue.put(('success', worker_id, num_items))
        q.close()
    except Exception as e:
        print(f"Python producer {worker_id} error: {str(e)}", flush=True)
        results_queue.put(('error', worker_id, str(e)))


def python_consumer(queue_name, num_items, results_queue, starting_index):
    """Python consumer process."""
    try:
        # Signal that consumer is ready (create/touch the file, then close it immediately)
        with open('ready', 'a') as f:
            pass  # Just create/touch the file

        # On Windows, we need to specify size when opening existing shared memory
        # q = SlickQueue(name=queue_name, size=size, element_size=32)
        q = SlickQueue(name=queue_name, element_size=32)
        consumed = []
        # read_index = 0
        read_index = starting_index
        attempts = 0
        max_attempts = 10000

        while len(consumed) < num_items and attempts < max_attempts:
            attempts += 1
            prev_read_index = read_index
            data, size, read_index = q.read(read_index)

            if data is not None:
                worker_id, item_num = struct.unpack("<I I", data[:8])
                consumed.append((worker_id, item_num))
                # print(f"consume {worker_id} {item_num} (from index {prev_read_index}, consumed: {len(consumed)})", flush=True)
            else:
                time.sleep(0.001)

        # print(f"CONSUMER: About to put {len(consumed)} items into results queue", flush=True)
        results_queue.put(('success', consumed))
        # print(f"CONSUMER: Successfully put results into queue", flush=True)
        q.close()
        time.sleep(1)
        # print(f"CONSUMER: Exiting normally", flush=True)
    except Exception as e:
        print(f"CONSUMER: Exception - {str(e)}", flush=True)
        results_queue.put(('error', str(e)))
    try:
        os.remove('ready')
    except Exception:
        pass

def test_python_producer_cpp_consumer():
    """Test: Python produces, C++ consumes."""
    print("\n" + "="*70)
    print("TEST: Python Producer -> C++ Consumer")
    print("="*70)

    queue_name = "test_interop_py_to_cpp"
    size = 128  # Increased from 64 to accommodate 100 items without wrapping
    element_size = 32
    num_items = 100

    # Create queue
    q = SlickQueue(name=queue_name, size=size, element_size=element_size)

    # Get actual shared memory name (may differ on Linux due to psm_ prefix)
    actual_shm_name = q.get_shm_name()
    if not actual_shm_name:
        raise RuntimeError("Failed to get shared memory name")
    print(f"Python created queue with name: {actual_shm_name}")

    try:
        with open('ready', 'a'):
            # Start Python producer
            results = MPQueue()
            producer = Process(target=python_producer, args=(queue_name, num_items, 1, results, size))
            producer.start()
            producer.join(timeout=30)

            if producer.is_alive():
                producer.kill()
                raise RuntimeError("Python producer timeout")

            # Check producer result
            result = get_result(results, "Python producer")
            assert result[0] == 'success', f"Producer failed: {result}"

            # Debug: On Linux, verify shared memory exists before calling C++
            if sys.platform != 'win32':
                print(f"Checking for shared memory file in /dev/shm/...")
                ls_result = subprocess.run(['ls', '-la', '/dev/shm/'], capture_output=True, text=True)
                for line in ls_result.stdout.split('\n'):
                    if actual_shm_name.lstrip('/') in line:
                        print(f"  Found: {line}")

                # Also check permissions
                shm_file = f"/dev/shm/{actual_shm_name.lstrip('/')}"
                if Path(shm_file).exists():
                    print(f"Shared memory file exists at: {shm_file}")
                    stat_result = subprocess.run(['stat', shm_file], capture_output=True, text=True)
                    print(f"File permissions:\n{stat_result.stdout}")

            # Start C++ consumer - use actual shared memory name for Linux compatibility
            cpp_consumer = find_cpp_executable("cpp_consumer")
            output_file = Path(__file__).parent / "cpp_consumer_output.txt"

            proc = subprocess.run(
                [cpp_consumer, actual_shm_name, str(num_items), str(element_size), str(output_file)],
                capture_output=True,
                text=True,
                timeout=15
            )

            print(proc.stdout)
            print(proc.stderr)
            if proc.returncode != 0:
                print(proc.stderr, file=sys.stderr)
                raise RuntimeError(f"C++ consumer failed with code {proc.returncode}")

            # Verify consumed data
            consumed = []
            with open(output_file, 'r') as f:
                for line in f:
                    worker_id, item_num = map(int, line.strip().split())
                    consumed.append((worker_id, item_num))

            # Check all items consumed
            assert len(consumed) == num_items, f"Expected {num_items} items, got {len(consumed)}"

            # Check data integrity
            expected = set((1, 1 * 100 +i) for i in range(num_items))
            actual = set(consumed)
            assert expected == actual, f"Data mismatch between produced and consumed"

            print(f"PASSED: {num_items} items transferred Python -> C++")

    finally:
        q.close()
        q.unlink()
        if output_file.exists():
            output_file.unlink()

    try:
        os.remove('ready')
    except Exception:
        pass


def test_cpp_producer_python_consumer():
    """Test: C++ produces, Python consumes."""
    print("\n" + "="*70)
    print("TEST: C++ Producer -> Python Consumer")
    print("="*70)

    try:
        os.remove('ready')
    except Exception:
        pass

    queue_name = "test_interop_cpp_to_py"
    size = 128  # Increased from 64 to accommodate 100 items without wrapping
    element_size = 32
    num_items = 100

    # Create queue
    q = SlickQueue(name=queue_name, size=size, element_size=element_size)

    # Get actual shared memory name (may differ on Linux due to psm_ prefix)
    actual_shm_name = q.get_shm_name()
    if not actual_shm_name:
        raise RuntimeError("Failed to get shared memory name")
    print(f"Python created queue with name: {actual_shm_name}")

    try:
        # Start Python consumer
        starting_index, _ = q._read_reserved()
        results = MPQueue()
        consumer = Process(target=python_consumer, args=(queue_name, num_items, results, starting_index))
        consumer.start()

        # Small delay to ensure consumer is ready
        time.sleep(0.5)

        # Start C++ producer - use actual shared memory name for Linux compatibility
        cpp_producer = find_cpp_executable("cpp_producer")
        proc = subprocess.run(
            [cpp_producer, actual_shm_name, str(size), str(num_items), str(element_size)],
            capture_output=True,
            text=True,
            timeout=15
        )

        print(proc.stdout)
        if proc.returncode != 0:
            print(proc.stderr, file=sys.stderr)
            raise RuntimeError(f"C++ producer failed with code {proc.returncode}")

        # Wait for consumer
        consumer.join(timeout=30)
        if consumer.is_alive():
            consumer.kill()
            raise RuntimeError("Python consumer timeout")

        # Check consumer result
        result = get_result(results, "Python consumer")
        assert result[0] == 'success', f"Consumer failed: {result}"
        consumed = result[1]

        # Check all items consumed
        assert len(consumed) == num_items, f"Expected {num_items} items, got {len(consumed)}"

        # Check data integrity (C++ uses worker_id=999)
        expected = set((999, i) for i in range(num_items))
        actual = set(consumed)
        if expected != actual:
            missing = expected - actual
            extra = actual - expected
            print(f"  Expected {len(expected)} unique items, got {len(actual)}")
            if missing:
                print(f"  Missing items (first 10): {sorted(list(missing))[:10]}")
            if extra:
                print(f"  Extra items (first 10): {sorted(list(extra))[:10]}")
        assert expected == actual, "Data mismatch between produced and consumed"

        print(f"PASSED: {num_items} items transferred C++ -> Python")

    finally:
        q.close()
        q.unlink()
        time.sleep(1)


def test_multi_producer_interop():
    """Test: Multiple C++ and Python producers, Python consumer."""
    print("\n" + "="*70)
    print("TEST: Multi-Producer Interop (C++ + Python -> Python)")
    print("="*70)

    try:
        os.remove('ready')
    except Exception:
        pass

    queue_name = "test_interop_multi_prod"
    size = 128
    element_size = 32

    num_cpp_threads = 2
    num_python_procs = 2
    items_per_producer = 50
    total_items = (num_cpp_threads + num_python_procs) * items_per_producer

    # Create queue
    q = SlickQueue(name=queue_name, size=size, element_size=element_size)

    # Get actual shared memory name (may differ on Linux due to psm_ prefix)
    actual_shm_name = q.get_shm_name()
    if not actual_shm_name:
        raise RuntimeError("Failed to get shared memory name")
    print(f"Python created queue with name: {actual_shm_name}")

    try:
        # Start Python consumer
        starting_index, _ = q._read_reserved()
        results = MPQueue()
        consumer = Process(target=python_consumer, args=(queue_name, total_items, results, starting_index))
        consumer.start()

        # Small delay to ensure consumer is ready
        time.sleep(0.5)

        # Start C++ multi-producer - use actual shared memory name for Linux compatibility
        cpp_multi_producer = find_cpp_executable("cpp_multi_producer")
        cpp_proc = subprocess.Popen(
            [cpp_multi_producer, actual_shm_name, str(size), str(num_cpp_threads), str(items_per_producer)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Start Python producers immediately (don't wait for C++ to finish)
        python_producers = []
        python_producers_results = MPQueue()
        for i in range(num_python_procs):
            p = Process(target=python_producer, args=(queue_name, items_per_producer, i, python_producers_results, size))
            p.start()
            python_producers.append(p)

        # Wait for Python producers. The timeout has to cover process spawn, which
        # on Windows re-imports this module in each child and can be slow when the
        # machine is loaded - the producers themselves only do ~150ms of work.
        killed = []
        for i, p in enumerate(python_producers):
            p.join(timeout=30)
            if p.is_alive():
                p.kill()
                killed.append(i)

        assert not killed, f"Python producer(s) {killed} timed out and were killed"

        # Check Python producer results
        for _ in range(num_python_procs):
            result = get_result(python_producers_results, "Python producer")
            assert result[0] == 'success', f"Python producer failed: {result}"

        # Wait for C++ producers
        cpp_stdout, cpp_stderr = cpp_proc.communicate(timeout=20)
        print(cpp_stdout)
        if cpp_proc.returncode != 0:
            print(cpp_stderr, file=sys.stderr)
            raise RuntimeError(f"C++ multi-producer failed with code {cpp_proc.returncode}")

        # Wait for consumer
        consumer.join(timeout=30)
        if consumer.is_alive():
            consumer.kill()
            raise RuntimeError("Consumer timeout")

        # Check consumer result
        result = get_result(results, "Python consumer")
        assert result[0] == 'success', f"Consumer failed: {result}"
        consumed = result[1]

        # Check all items consumed
        assert len(consumed) == total_items, f"Expected {total_items} items, got {len(consumed)}"

        # Verify no duplicates
        assert len(consumed) == len(set(consumed)), "Duplicate items detected"

        # Count items by producer
        cpp_items = sum(1 for wid, _ in consumed if wid >= 1000)
        python_items = sum(1 for wid, _ in consumed if wid < 1000)

        print(f"  C++ produced: {cpp_items} items")
        print(f"  Python produced: {python_items} items")
        print(f"  Total consumed: {len(consumed)} items")

        assert cpp_items == num_cpp_threads * items_per_producer, "C++ item count mismatch"
        assert python_items == num_python_procs * items_per_producer, "Python item count mismatch"

        print(f"PASSED: {total_items} items from {num_cpp_threads} C++ + {num_python_procs} Python producers")

    finally:
        q.close()
        q.unlink()


def test_stress_interop():
    """Stress test: High volume C++/Python interop."""
    print("\n" + "="*70)
    print("TEST: Stress Test - High Volume Interop")
    print("="*70)

    try:
        os.remove('ready')
    except Exception:
        pass

    queue_name = "test_interop_stress"
    size = 256
    element_size = 32

    num_cpp_threads = 4
    num_python_procs = 4
    items_per_producer = 200
    total_items = (num_cpp_threads + num_python_procs) * items_per_producer

    # Create queue
    q = SlickQueue(name=queue_name, size=size, element_size=element_size)

    # Get actual shared memory name (may differ on Linux due to psm_ prefix)
    actual_shm_name = q.get_shm_name()
    if not actual_shm_name:
        raise RuntimeError("Failed to get shared memory name")
    print(f"Python created queue with name: {actual_shm_name}")

    start_time = time.time()

    try:
        # Start Python consumer
        starting_index, _ = q._read_reserved()
        results = MPQueue()
        consumer = Process(target=python_consumer, args=(queue_name, total_items, results, starting_index))
        consumer.start()

        # Small delay to ensure consumer is ready
        time.sleep(0.5)

        # Start C++ multi-producer - use actual shared memory name for Linux compatibility
        cpp_multi_producer = find_cpp_executable("cpp_multi_producer")
        cpp_proc = subprocess.Popen(
            [cpp_multi_producer, actual_shm_name, str(size), str(num_cpp_threads), str(items_per_producer)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Start Python producers
        python_producers = []
        for i in range(num_python_procs):
            p = Process(target=python_producer, args=(queue_name, items_per_producer, i + 2000, results, size))
            p.start()
            python_producers.append(p)

        # Collect results concurrently while processes run to prevent queue blocking
        all_results = []
        results_complete = False

        def collect_results():
            nonlocal results_complete
            # Expect num_python_procs producer results + 1 consumer result
            for _ in range(num_python_procs + 1):
                try:
                    result = results.get(timeout=60)
                    all_results.append(result)
                except Exception as e:
                    print(f'Error collecting result: {e}')
                    break
            results_complete = True

        # Start result collection in background thread
        import threading
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()

        # Wait for all producers
        cpp_stdout, cpp_stderr = cpp_proc.communicate(timeout=30)
        print(cpp_stdout)
        if cpp_proc.returncode != 0:
            print(cpp_stderr, file=sys.stderr)
            raise RuntimeError(f"C++ multi-producer failed")

        for p in python_producers:
            p.join(timeout=15)
            if p.is_alive():
                p.kill()

        # Wait for consumer
        consumer.join(timeout=30)
        if consumer.is_alive():
            consumer.kill()
            raise RuntimeError("Consumer timeout")

        # Wait for result collection to complete
        collector_thread.join(timeout=10)

        elapsed = time.time() - start_time

        # Check results
        if len(all_results) != num_python_procs + 1:
            raise RuntimeError(f"Expected {num_python_procs + 1} results, got {len(all_results)}")

        # Separate producer and consumer results
        # Producer: ('success', worker_id, num_items) - 3 elements
        # Consumer: ('success', consumed_list) - 2 elements
        producer_results = [r for r in all_results if r[0] == 'success' and len(r) == 3]
        consumer_results = [r for r in all_results if r[0] == 'success' and len(r) == 2]

        assert len(producer_results) == num_python_procs, f"Expected {num_python_procs} producer results, got {len(producer_results)}"
        assert len(consumer_results) == 1, f"Expected 1 consumer result, got {len(consumer_results)}"

        consumed = consumer_results[0][1]

        assert len(consumed) == total_items, f"Expected {total_items}, got {len(consumed)}"
        assert len(consumed) == len(set(consumed)), "Duplicate items detected"

        throughput = total_items / elapsed
        print(f"  Total items: {total_items}")
        print(f"  Duration: {elapsed:.2f}s")
        print(f"  Throughput: {throughput:.0f} items/sec")
        print(f"PASSED: Stress test completed successfully")

    finally:
        q.close()
        q.unlink()

def test_cpp_shm_creation():
    """Stress test: High volume C++/Python interop."""
    print("\n" + "="*70)
    print("TEST: SHM created by C++")
    print("="*70)

    queue_name = "test_cpp_shm_creation"
    size = 128
    element_size = 32

    num_cpp_threads = 2
    num_python_procs = 2
    items_per_producer = 100
    total_items = (num_cpp_threads + num_python_procs) * items_per_producer

    start_time = time.time()

    # Start C++ multi-producer - use actual shared memory name for Linux compatibility
    cpp_multi_producer = find_cpp_executable("cpp_multi_producer")
    cpp_proc = subprocess.Popen(
        [cpp_multi_producer, queue_name, str(size), str(num_cpp_threads), str(items_per_producer)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    time.sleep(0.5)

    # Start Python consumer
    results = MPQueue()
    consumer = Process(target=python_consumer, args=(queue_name, total_items, results, 0))
    consumer.start()

    # Small delay to ensure consumer is ready
    time.sleep(0.5)

    # Start Python producers
    python_producers = []
    for i in range(num_python_procs):
        p = Process(target=python_producer, args=(queue_name, items_per_producer, i + 2000, results, size))
        p.start()
        python_producers.append(p)

    # Collect results concurrently while processes run to prevent queue blocking
    all_results = []
    results_complete = False

    def collect_results():
        nonlocal results_complete
        # Expect num_python_procs producer results + 1 consumer result
        for _ in range(num_python_procs + 1):
            try:
                result = results.get(timeout=60)
                all_results.append(result)
            except Exception as e:
                print(f'Error collecting result: {e}')
                break
        results_complete = True

    # Start result collection in background thread
    import threading
    collector_thread = threading.Thread(target=collect_results, daemon=True)
    collector_thread.start()

    for p in python_producers:
        p.join(timeout=15)
        if p.is_alive():
            p.kill()

    # Wait for consumer
    consumer.join(timeout=30)
    if consumer.is_alive():
        consumer.kill()
        raise RuntimeError("Consumer timeout")

    # Wait for all producers
    cpp_stdout, cpp_stderr = cpp_proc.communicate(timeout=30)
    print(cpp_stdout)
    print(cpp_stderr)
    if cpp_proc.returncode != 0:
        print(cpp_stderr, file=sys.stderr)
        raise RuntimeError(f"C++ multi-producer failed")

    # Wait for result collection to complete
    collector_thread.join(timeout=10)

    elapsed = time.time() - start_time

    # Check results
    if len(all_results) != num_python_procs + 1:
        raise RuntimeError(f"Expected {num_python_procs + 1} results, got {len(all_results)}")

    # Separate producer and consumer results
    # Producer: ('success', worker_id, num_items) - 3 elements
    # Consumer: ('success', consumed_list) - 2 elements
    producer_results = [r for r in all_results if r[0] == 'success' and len(r) == 3]
    consumer_results = [r for r in all_results if r[0] == 'success' and len(r) == 2]

    assert len(producer_results) == num_python_procs, f"Expected {num_python_procs} producer results, got {len(producer_results)}"
    assert len(consumer_results) == 1, f"Expected 1 consumer result, got {len(consumer_results)}"

    consumed = consumer_results[0][1]

    assert len(consumed) == total_items, f"Expected {total_items}, got {len(consumed)}"
    assert len(consumed) == len(set(consumed)), "Duplicate items detected"

    throughput = total_items / elapsed
    print(f"  Total items: {total_items}")
    print(f"  Duration: {elapsed:.2f}s")
    print(f"  Throughput: {throughput:.0f} items/sec")
    print(f"PASSED: SHM created by C++ test completed successfully")


def run_all_tests():
    """Run all interoperability tests."""
    print("\n" + "="*70)
    print("C++/Python Interoperability Tests for SlickQueue")
    print("="*70)

    tests = [
        ("Python -> C++", test_python_producer_cpp_consumer),
        ("C++ -> Python", test_cpp_producer_python_consumer),
        ("Multi-Producer Interop", test_multi_producer_interop),
        ("Stress Interop", test_stress_interop),
        ("SHM created by C++", test_cpp_shm_creation),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\nX FAILED: {name}")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "="*70)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*70)

    return failed == 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='C++/Python interoperability tests')
    parser.add_argument('--test', choices=[
        'python_producer_cpp_consumer',
        'cpp_producer_python_consumer',
        'multi_producer_interop',
        'stress_interop',
        'cpp_shm_creation',
        'all'
    ], default='all', help='Specific test to run')

    args = parser.parse_args()

    if args.test == 'all':
        success = run_all_tests()
    elif args.test == 'python_producer_cpp_consumer':
        test_python_producer_cpp_consumer()
        success = True
    elif args.test == 'cpp_producer_python_consumer':
        test_cpp_producer_python_consumer()
        success = True
    elif args.test == 'multi_producer_interop':
        test_multi_producer_interop()
        success = True
    elif args.test == 'stress_interop':
        test_stress_interop()
        success = True
    elif args.test == 'cpp_shm_creation':
        test_cpp_shm_creation()
        success = True

    sys.exit(0 if success else 1)
