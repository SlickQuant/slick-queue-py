"""
Multi-producer multi-consumer tests for SlickQueue.

Tests concurrent access from multiple processes to verify atomic operations
and lock-free semantics.
"""
import struct
import sys
import time
from pathlib import Path
from multiprocessing import Process, Queue, Value, shared_memory
import random
import os

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_queue_py import SlickQueue


def cleanup_shm_segment(name):
    """Clean up a shared memory segment if it exists.

    On Windows, if orphaned processes are holding the segment,
    unlink() won't actually delete it. We try anyway.
    """
    try:
        shm = shared_memory.SharedMemory(name=name)
        try:
            shm.close()
        except BufferError:
            pass  # Ignore BufferError during close
        try:
            shm.unlink()
        except:
            pass  # May fail on Windows if processes hold it
    except FileNotFoundError:
        pass  # Already cleaned up
    except Exception:
        pass  # Ignore other errors


def create_or_open_queue(name, size, element_size):
    """Create a queue, or forcibly recreate if it already exists.

    Handles the case where cleanup failed due to orphaned processes.
    """
    # Try cleanup first
    cleanup_shm_segment(name)

    try:
        # Try to create new
        return SlickQueue(name=name, size=size, element_size=element_size)
    except FileExistsError:
        # Still exists (cleanup failed due to orphaned processes on Windows)
        # Force unlink and retry
        try:
            shm = shared_memory.SharedMemory(name=name)
            try:
                shm.close()
            except:
                pass
            shm.unlink()
        except:
            pass

        # Final attempt to create
        try:
            return SlickQueue(name=name, size=size, element_size=element_size)
        except FileExistsError:
            # Give up and provide helpful error message
            raise RuntimeError(
                f"Shared memory '{name}' exists and cannot be cleaned up. "
                f"This usually means orphaned Python processes are holding it. "
                f"Please run 'python cleanup_shm.py' or restart your terminal."
            )


# def producer_worker(shm_name, element_size, num_items, worker_id, results_queue):
#     """Producer worker: reserves slots, writes data, and publishes."""
#     try:
#         time.sleep(random.uniform(0.01, 0.030))

#         # Open existing queue
#         q = SlickQueue(name=shm_name, element_size=element_size)

#         produced = []
#         for i in range(num_items):
#             # Reserve a slot
#             time.sleep(random.uniform(0.001, 0.003))
#             idx = q.reserve(1)

#             # Write unique data: worker_id + item_number
#             data = struct.pack("<I I", worker_id, i)
#             slot = q[idx]
#             slot[:len(data)] = data

#             # Publish
#             q.publish(idx, 1)
#             print(f'{worker_id} produce: {idx} {i}')

#             produced.append((worker_id, i, idx))

#         results_queue.put(('producer', worker_id, produced))
#         q.close()
#     except Exception as e:
#         results_queue.put(('error', worker_id, str(e)))

def make_flow_control(size, batch_sizes):
    """Shared consumer position and the lead producers may keep over it.

    SlickQueue is lossy: a producer never waits, so one that runs a whole ring ahead
    overwrites records the consumer has not read yet. A descheduled consumer on a
    loaded CI runner gets lapped that way, never reaches its expected count, and the
    test times out. These tests check reservation, wrap and publish correctness, not
    overrun, so producers hold back until the consumer is far enough behind.

    Every producer can pass the check at once before any of them reserves, and a
    reservation of b items can use up to 2b - 1 indices (b plus up to b - 1 skipped
    at the wrap), so the lead is the ring size less that worst-case total.
    """
    max_ahead = size - sum(2 * b - 1 for b in batch_sizes)
    assert max_ahead >= 0, f"queue of {size} too small for batches {batch_sizes}"
    return Value('q', 0, lock=False), max_ahead


def wait_for_room(q, consumer_pos, max_ahead):
    """Block until reserving cannot lap the consumer (see make_flow_control)."""
    # initial_reading_index() is the producers' reservation counter
    while q.initial_reading_index() - consumer_pos.value > max_ahead:
        time.sleep(0.0005)


def producer_worker(shm_name, element_size, num_items, worker_id, results_queue, publish_n_items_at_once = 1,
                    consumer_pos = None, max_ahead = 0):
    """Producer worker: reserves slots, writes data, and publishes."""
    try:
        while not os.path.exists('ready'):
            time.sleep(0.001)

        time.sleep(random.uniform(0.01, 0.03))

        # Open existing queue
        q = SlickQueue(name=shm_name, element_size=element_size)

        produced = []
        i = 0
        while i < num_items:
            # Reserve a slot
            time.sleep(random.uniform(0.002, 0.005))
            count = min(publish_n_items_at_once, num_items - i)
            if consumer_pos is not None:
                wait_for_room(q, consumer_pos, max_ahead)
            idx = q.reserve(count)
            # print(f'{worker_id} reserve: {count} {idx}')
            n = count
            index = idx
            while n > 0 and i < num_items:
                data = struct.pack("<I I", worker_id, i)
                slot = q[index]
                slot[:len(data)] = data

                # print(f'{worker_id} produce: {index} {i} ({idx} {count} {n})')
                produced.append((worker_id, i, index))

                index += 1
                i += 1
                n -= 1

            # Publish
            q.publish(idx, count)
            
        results_queue.put(('producer', worker_id, produced))
        # print(f'*** producer {worker_id} complete. produced: {len(produced)}')
        q.close()
    except Exception as e:
        results_queue.put(('error', worker_id, str(e)))


def consumer_worker(shm_name, element_size, expected_count, worker_id, results_queue, consumer_pos = None):
    """Consumer worker: reads items from queue."""
    try:
        # Open existing queue
        q = SlickQueue(name=shm_name, element_size=element_size)

        consumed = []
        read_index = 0

        # Signal that consumer is ready (create/touch the file, then close it immediately)
        with open('ready', 'a') as f:
            pass  # Just create/touch the file
        
        # Keep reading until we've consumed expected_count items
        while len(consumed) < expected_count:
            data, size, read_index = q.read(read_index)
            if consumer_pos is not None:
                # read() has already copied the record out, so its slots are free
                consumer_pos.value = read_index
            offset = 0
            if data is not None:
                index = read_index - size
                # print(f'read: {index} {size}')
                while size > 0:
                    # Parse the data
                    producer_id, item = struct.unpack("<I I", data[offset:offset + 8])
                    # Track the index where this item was read from (read_index - 1 because read() already incremented it)
                    consumed.append((producer_id, item, index))
                    # print(f'consume: {index} {producer_id} {item}. consumed: {len(consumed)} expected: {expected_count}')
                    offset += element_size
                    size -= 1
                    index += 1
            else:
                # No data available, small sleep to avoid busy-waiting
                time.sleep(0.001)

        # Send the full consumed list - the background thread in main process
        # will drain the queue so put() won't block
        results_queue.put(('consumer', worker_id, consumed))
        q.close()
        # print(f'comsumer complete. consumed: {len(consumed)}')
    except Exception as e:
        try:
            results_queue.put_nowait(('error', worker_id, str(e)))
        except:
            pass
    try:
        os.remove('ready')
    except Exception:
        pass


def test_single_producer_single_consumer():
    """Test basic single producer, single consumer."""
    print("\n[TEST] Single Producer, Single Consumer...")

    shm_name = "test_mpmc_spsc"
    size = 64
    element_size = 32
    num_items = 100

    # Create queue (with automatic cleanup if needed)
    q = create_or_open_queue(shm_name, size, element_size)
    consumer_pos, max_ahead = make_flow_control(size, [1])
    # Keep queue open while children access it

    p_proc = None
    c_proc = None

    try:
        try:
            os.remove('ready')
        except Exception:
            pass

        results = Queue()

        # Start producer
        p_proc = Process(target=producer_worker,
                        args=(shm_name, element_size, num_items, 1, results, 1, consumer_pos, max_ahead))
        p_proc.start()

        # Start consumer
        c_proc = Process(target=consumer_worker,
                        args=(shm_name, element_size, num_items, 1, results, consumer_pos))
        c_proc.start()

        # Collect results concurrently while processes run
        all_collected = []
        results_complete = False

        def collect_results():
            nonlocal results_complete
            for _ in range(2):
                try:
                    result = results.get(timeout=20)
                    all_collected.append(result)
                except Exception as e:
                    print(f'Error collecting result: {e}')
                    break
            results_complete = True

        # Start result collection in background thread
        import threading
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()

        # Wait for processes to complete
        p_proc.join(timeout=15)
        c_proc.join(timeout=15)

        # Wait for result collection to complete
        collector_thread.join(timeout=5)

        # Verify we got both results
        assert len(all_collected) == 2, f"Expected 2 results, got {len(all_collected)}"

        # Unpack results - order is not guaranteed
        result1, result2 = all_collected[0], all_collected[1]

        # Force kill if still alive
        if p_proc.is_alive():
            p_proc.terminate()
            p_proc.join(timeout=1)
            if p_proc.is_alive():
                p_proc.kill()

        if c_proc.is_alive():
            c_proc.terminate()
            c_proc.join(timeout=1)
            if c_proc.is_alive():
                c_proc.kill()

        # Identify which result is producer and which is consumer
        if result1[0] == 'producer':
            p_result, c_result = result1, result2
        else:
            p_result, c_result = result2, result1

        assert p_result[0] == 'producer', f"Producer failed: {p_result}"
        assert c_result[0] == 'consumer', f"Consumer failed: {c_result}"

        produced = p_result[2]
        consumed = c_result[2]

        assert len(produced) == num_items, f"Produced {len(produced)}, expected {num_items}"
        assert len(consumed) == num_items, f"Consumed {len(consumed)}, expected {num_items}"

        # Verify all items were consumed
        produced_items = set(produced)
        consumed_items = set(consumed)
        assert produced_items == consumed_items, "Mismatch between produced and consumed items"

        print("[PASSED]")

    finally:
        # Ensure processes are terminated
        if p_proc and p_proc.is_alive():
            p_proc.kill()
        if c_proc and c_proc.is_alive():
            c_proc.kill()

        # Cleanup
        q.close()
        q.unlink()


def test_multi_producer_single_consumer():
    """Test multiple producers, single consumer."""
    print("\n[TEST] Multi-Producer, Single Consumer...")

    shm_name = "test_mpmc_mpsc"
    size = 128
    element_size = 32
    num_producers = 4
    items_per_producer = 50
    total_items = num_producers * items_per_producer

    # Create queue (with automatic cleanup if needed)
    q = create_or_open_queue(shm_name, size, element_size)
    consumer_pos, max_ahead = make_flow_control(size, [1] * num_producers)
    # Keep queue open

    producers = []
    consumer = None

    try:
        try:
            os.remove('ready')
        except Exception:
            pass
        
        results = Queue()

        # Start single consumer
        consumer = Process(target=consumer_worker,
                          args=(shm_name, element_size, total_items, 0, results, consumer_pos))
        consumer.start()

        # Start multiple producers
        for i in range(num_producers):
            p = Process(target=producer_worker,
                       args=(shm_name, element_size, items_per_producer, i, results, 1, consumer_pos, max_ahead))
            p.start()
            producers.append(p)

        # Collect results concurrently while processes run
        all_results = []
        results_complete = False

        def collect_results():
            nonlocal results_complete
            for _ in range(num_producers + 1):
                try:
                    result = results.get(timeout=30)
                    all_results.append(result)
                except Exception as e:
                    print(f'Error collecting result: {e}')
                    break
            results_complete = True

        # Start result collection in background thread
        import threading
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()

        # Wait for all producers and consumer to complete
        for p in producers:
            p.join(timeout=20)
        consumer.join(timeout=20)

        # Wait for result collection to complete
        collector_thread.join(timeout=5)

        # Force kill any still alive
        for p in producers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    p.kill()

        if consumer and consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=1)
            if consumer.is_alive():
                consumer.kill()

        # Verify no errors
        errors = [r for r in all_results if r[0] == 'error']
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Get producer and consumer results
        producer_results = [r for r in all_results if r[0] == 'producer']
        consumer_results = [r for r in all_results if r[0] == 'consumer']

        assert len(producer_results) == num_producers
        assert len(consumer_results) == 1

        # Collect all produced items
        all_produced = set()
        for _, worker_id, produced in producer_results:
            for item in produced:
                all_produced.add((item[0], item[1]))  # (worker_id, item_num)

        # Get consumed items (extract only worker_id and item_num to match produced format)
        consumed = set()
        for item in consumer_results[0][2]:
            consumed.add((item[0], item[1]))  # (worker_id, item_num)

        # Verify all items were consumed
        assert len(all_produced) == total_items, f"Produced {len(all_produced)}, expected {total_items}"
        assert len(consumed) == total_items, f"Consumed {len(consumed)}, expected {total_items}"
        assert all_produced == consumed, "Mismatch between produced and consumed items"

        print(f"[PASSED] - {num_producers} producers, {total_items} items processed")

    finally:
        # Ensure all processes are terminated
        for p in producers:
            if p and p.is_alive():
                p.kill()
        if consumer and consumer.is_alive():
            consumer.kill()

        # Cleanup
        q.close()
        q.unlink()


def test_stress_high_contention():
    """Stress test with high contention (many producers, small queue)."""
    print("\n[TEST] Stress Test - High Contention...")

    shm_name = "test_mpmc_stress"
    size = 16  # Small queue for high contention
    element_size = 32
    num_producers = 8
    items_per_producer = 100
    total_items = num_producers * items_per_producer

    # Create queue (with automatic cleanup if needed)
    q = create_or_open_queue(shm_name, size, element_size)
    consumer_pos, max_ahead = make_flow_control(size, [1] * num_producers)
    q.reset()
    # Keep queue open

    producers = []
    consumer = None

    try:
        try:
            os.remove('ready')
        except Exception:
            pass

        results = Queue()

        # Start single consumer
        consumer = Process(target=consumer_worker,
                          args=(shm_name, element_size, total_items, 0, results, consumer_pos))
        consumer.start()

        time.sleep(0.5)

        # Start multiple producers
        start_time = time.time()

        for i in range(num_producers):
            p = Process(target=producer_worker,
                       args=(shm_name, element_size, items_per_producer, i, results, 1, consumer_pos, max_ahead))
            p.start()
            producers.append(p)

        # Collect results concurrently while processes run
        # This prevents deadlock from full queue pipes
        all_results = []
        results_complete = False

        def collect_results():
            nonlocal results_complete
            for i in range(num_producers + 1):
                try:
                    # Very long timeout since large lists take time to pickle/unpickle
                    result = results.get(timeout=60)
                    all_results.append(result)
                except Exception as e:
                    print(f'Error getting result {i}: {e}')
                    print(f'Expected {num_producers + 1} results, got {len(all_results)}')
                    break
            results_complete = True

        # Start result collection in background thread
        import threading
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()

        # Wait for processes to complete
        for p in producers:
            p.join(timeout=30)
        consumer.join(timeout=30)

        # Wait for result collection to complete
        collector_thread.join(timeout=10)

        if not results_complete:
            print(f'WARNING: Result collection incomplete. Got {len(all_results)}/{num_producers + 1} results')

        # Force kill any still alive
        for p in producers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    p.kill()

        if consumer and consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=1)
            if consumer.is_alive():
                consumer.kill()

        elapsed = time.time() - start_time

        # Verify no errors
        errors = [r for r in all_results if r[0] == 'error']
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify counts
        producer_results = [r for r in all_results if r[0] == 'producer']
        consumer_results = [r for r in all_results if r[0] == 'consumer']

        assert len(producer_results) == num_producers, f"Expected {num_producers} producer results, got {len(producer_results)}"
        assert len(consumer_results) == 1, f"Expected 1 consumer result, got {len(consumer_results)}"

        # Collect all produced items
        all_produced = set()
        for _, worker_id, produced in producer_results:
            for item in produced:
                all_produced.add((item[0], item[1]))  # (worker_id, item_num)

        # Get consumed items (extract only worker_id and item_num to match produced format)
        consumed = set()
        for item in consumer_results[0][2]:
            consumed.add((item[0], item[1]))  # (worker_id, item_num)

        # Verify all items were consumed
        assert len(all_produced) == total_items, f"Produced {len(all_produced)}, expected {total_items}"
        assert len(consumer_results[0][2 == len(consumed)]), f'duplicated items'
        assert len(consumed) == total_items, f"Consumed {len(consumed)}, expected {total_items}"
        assert all_produced == consumed, "Mismatch between produced and consumed items"

        throughput = total_items / elapsed
        print(f"[PASSED] - {total_items} items in {elapsed:.2f}s ({throughput:.0f} items/sec)")

    finally:
        # Ensure all processes are terminated
        for p in producers:
            if p and p.is_alive():
                p.kill()
        if consumer and consumer.is_alive():
            consumer.kill()

        # Cleanup
        q.close()
        q.unlink()


def test_wrap_around():
    """Test wrap-around behavior with multiple producers."""
    print("\n[TEST] Wrap-Around with Multiple Producers...")

    shm_name = "test_mpmc_wrap"
    size = 8  # Very small queue to force wrapping
    element_size = 32
    num_producers = 2
    items_per_producer = 40  # Much more than queue size
    total_items = num_producers * items_per_producer

    # Create queue (with automatic cleanup if needed)
    q = create_or_open_queue(shm_name, size, element_size)
    batch_sizes = [i * 2 + 1 for i in range(num_producers)]
    consumer_pos, max_ahead = make_flow_control(size, batch_sizes)
    q.reset()
    # Keep queue open

    producers = []
    consumer = None

    try:
        try:
            os.remove('ready')
        except Exception:
            pass

        results = Queue()

        # Start consumer
        consumer = Process(target=consumer_worker,
                          args=(shm_name, element_size, total_items, 0, results, consumer_pos))
        consumer.start()

        # Start producers
        for i in range(num_producers):
            p = Process(target=producer_worker,
                       args=(shm_name, element_size, items_per_producer, i, results, batch_sizes[i],
                             consumer_pos, max_ahead))
            p.start()
            producers.append(p)

        # Collect results concurrently while processes run
        all_results = []
        results_complete = False

        def collect_results():
            nonlocal results_complete
            for _ in range(num_producers + 1):
                try:
                    result = results.get(timeout=20)
                    all_results.append(result)
                except Exception as e:
                    print(f'Error collecting result: {e}')
                    break
            results_complete = True

        # Start result collection in background thread
        import threading
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()

        # Wait for producers and consumer to complete
        for p in producers:
            p.join(timeout=15)
        consumer.join(timeout=15)

        # Wait for result collection to complete
        collector_thread.join(timeout=5)

        # Force kill any still alive
        for p in producers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    p.kill()

        if consumer and consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=1)
            if consumer.is_alive():
                consumer.kill()

        # Verify no errors
        errors = [r for r in all_results if r[0] == 'error']
        assert len(errors) == 0, f"Errors occurred: {errors}"

        # Verify all items processed correctly despite wrapping
        producer_results = [r for r in all_results if r[0] == 'producer']
        consumer_results = [r for r in all_results if r[0] == 'consumer']

        all_produced = set()
        for _, worker_id, produced in producer_results:
            for item in produced:
                all_produced.add((item[0], item[1]))

        # A consumer whose result did not arrive before the collector timed out
        # leaves this list empty; say so rather than raising IndexError from the
        # subscript below, which says nothing about what actually went wrong.
        assert consumer_results, (
            f"the consumer produced no result before the collector timed out "
            f"(got {len(all_results)} of {num_producers + 1} results: "
            f"{[r[0] for r in all_results]})"
        )

        # Extract only worker_id and item_num to match produced format
        consumed = set()
        # print(f'consumer_results: {len(consumer_results[0][2])}')
        i = 0
        for item in consumer_results[0][2]:
            # print(f'{i}: {item}')
            i += 1
            l = len(consumed)
            consumed.add((item[0], item[1]))
            if len(consumed) != l + 1:
                print(f'WARNING: Duplicate item detected: {item}')
                print(consumed)

        assert len(all_produced) == total_items
        assert len(consumed) == total_items
        assert all_produced == consumed, "Wrap-around caused data loss or corruption"

        print("[PASSED] - Wrap-around handled correctly")

    finally:
        # Ensure all processes are terminated
        for p in producers:
            if p and p.is_alive():
                p.kill()
        if consumer and consumer.is_alive():
            consumer.kill()

        # Cleanup
        q.close()
        q.unlink()


def run_all_tests():
    """Run all multi-producer/consumer tests."""
    print("=" * 70)
    print("Running Multi-Producer Multi-Consumer Tests")
    print("=" * 70)

    tests = [
        test_single_producer_single_consumer,
        test_multi_producer_single_consumer,
        test_stress_high_contention,
        test_wrap_around,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"[FAILED] - {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)

    return failed == 0


if __name__ == '__main__':
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
