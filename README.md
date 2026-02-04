# slick-queue-py

Python implementation of SlickQueue - a lock-free multi-producer multi-consumer (MPMC) queue with C++ interoperability through shared memory.

This is the Python binding for the [SlickQueue C++ library](https://github.com/SlickQuant/slick-queue). The Python implementation maintains exact binary compatibility with the C++ version, enabling seamless interprocess communication between Python and C++ applications.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/SlickQuant/slick-queue_py/actions/workflows/ci.yml/badge.svg)](https://github.com/SlickQuant/slick-queue-py/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/v/release/SlickQuant/slick-queue-py)](https://github.com/SlickQuant/slick-queue-py/releases)

## Features

- **Dual Mode Operation**:
  - **Local Memory Mode**: In-process queue using local memory (no shared memory overhead)
  - **Shared Memory Mode**: Inter-process queue for interprocess communication
- **Lock-Free Multi-Producer Multi-Consumer**: True MPMC support using atomic operations
- **C++/Python Interoperability**: Python and C++ processes can share the same queue
- **Cross-Platform**: Windows and Linux/macOS support (x86-64)
- **Memory Layout Compatible**: Exact binary compatibility with C++ `slick::SlickQueue<T>`
- **High Performance**: Hardware atomic operations for minimal overhead

## Requirements

- Python 3.8+ (uses `multiprocessing.shared_memory`)
- 64-bit platform
- For true lock-free operation: x86-64 CPU with CMPXCHG16B support (most CPUs since 2006)

## Installation

```bash
pip install -e .
```

Or just copy the Python files to your project.

## Quick Start

### Local Memory Mode (Single Process)

```python
from slick_queue_py import SlickQueue

# Create a queue in local memory (no shared memory)
q = SlickQueue(size=1024, element_size=256)

# Producer: Reserve a slot, write data, and publish
idx = q.reserve()
buf = q[idx]
buf[:len(b'hello')] = b'hello'
q.publish(idx)

# Consumer: Read data
read_index = 0
data, size, read_index = q.read(read_index)
if data is not None:
    print(f"Received: {data[:size]}")

q.close()  # unlink() does nothing for local mode
```

### Shared Memory Mode (Multi-Process)

```python
from slick_queue_py import SlickQueue

# Create a new shared memory queue (size must be power of two)
q = SlickQueue(name='my_queue', size=1024, element_size=256)

# Producer: Reserve a slot, write data, and publish
idx = q.reserve()
buf = q[idx]
buf[:len(b'hello')] = b'hello'
q.publish(idx)

# Consumer: Read data
read_index = 0
data, size, read_index = q.read(read_index)
if data is not None:
    print(f"Received: {data[:size]}")

q.close()
q.unlink()  # Delete shared memory segment
```

### Multi-Producer Usage

```python
from multiprocessing import Process
from slick_queue_py import SlickQueue
import struct

def producer_worker(queue_name, worker_id, num_items):
    # Open existing queue
    q = SlickQueue(name=queue_name, element_size=32)

    for i in range(num_items):
        # Reserve slot (thread-safe with atomic CAS)
        idx = q.reserve(1)

        # Write unique data
        data = struct.pack("<I I", worker_id, i)
        slot = q[idx]
        slot[:len(data)] = data

        # Publish (makes data visible to consumers)
        q.publish(idx, 1)

    q.close()

# Create queue
q = SlickQueue(name='mpmc_queue', size=64, element_size=32)

# Start multiple producers
producers = []
for i in range(4):
    p = Process(target=producer_worker, args=('mpmc_queue', i, 100))
    p.start()
    producers.append(p)

# Wait for completion
for p in producers:
    p.join()

q.close()
q.unlink()
```

### Multi-Consumer Work-Stealing

For multiple consumers sharing work from a single queue, use an `AtomicCursor` to enable work-stealing patterns where each item is consumed by exactly one consumer.

#### Local Mode (Multi-Threading)

```python
from threading import Thread
from slick_queue_py import SlickQueue, AtomicCursor
import struct

def consumer_worker(q, cursor, worker_id, results):
    items_processed = 0
    while True:
        # Atomically claim next item (work-stealing)
        data, size, index = q.read(cursor)

        if data is None:
            break  # No more data

        # Process the claimed item
        worker, seq = struct.unpack("<I I", data[:8])
        items_processed += 1

    results[worker_id] = items_processed

# Create local queue and cursor
q = SlickQueue(size=64, element_size=32)
cursor_buf = bytearray(8)
cursor = AtomicCursor(cursor_buf, 0)
cursor.store(0)  # Initialize cursor to 0

# Producer writes items
for i in range(100):
    idx = q.reserve()
    data = struct.pack("<I I", 0, i)
    q[idx][:len(data)] = data
    q.publish(idx)

# Start multiple consumer threads that share the work
results = {}
threads = []
for i in range(4):
    t = Thread(target=consumer_worker, args=(q, cursor, i, results))
    t.start()
    threads.append(t)

# Wait for all consumers
for t in threads:
    t.join()

print(f"Total items processed: {sum(results.values())}")
q.close()
```

#### Shared Memory Mode (Multi-Process)

```python
from multiprocessing import Process, shared_memory
from slick_queue_py import SlickQueue, AtomicCursor
import struct

def consumer_worker(queue_name, cursor_name, worker_id):
    # Open shared queue and cursor
    q = SlickQueue(name=queue_name, element_size=32)
    cursor_shm = shared_memory.SharedMemory(name=cursor_name)
    cursor = AtomicCursor(cursor_shm.buf, 0)

    items_processed = 0
    while True:
        # Atomically claim next item (work-stealing)
        data, size, index = q.read(cursor)

        if data is None:
            break  # No more data

        # Process the claimed item
        worker, seq = struct.unpack("<I I", data[:8])
        items_processed += 1

    print(f"Worker {worker_id} processed {items_processed} items")
    cursor_shm.close()
    q.close()

# Create queue and shared cursor
q = SlickQueue(name='work_queue', size=64, element_size=32)
cursor_shm = shared_memory.SharedMemory(name='work_cursor', create=True, size=8)
cursor = AtomicCursor(cursor_shm.buf, 0)
cursor.store(0)  # Initialize cursor to 0

# Producer writes items
for i in range(100):
    idx = q.reserve()
    data = struct.pack("<I I", 0, i)
    q[idx][:len(data)] = data
    q.publish(idx)

# Start multiple consumer processes that share the work
consumers = []
for i in range(4):
    p = Process(target=consumer_worker, args=('work_queue', 'work_cursor', i))
    p.start()
    consumers.append(p)

# Wait for all consumers
for p in consumers:
    p.join()

cursor_shm.close()
cursor_shm.unlink()
q.close()
q.unlink()
```

### C++/Python Interoperability

The Python implementation is fully compatible with the C++ [SlickQueue](https://github.com/SlickQuant/slick_queue) library. Python and C++ processes can produce and consume from the same queue with:

- **Exact memory layout compatibility**: Binary-compatible with `slick::SlickQueue<T>`
- **Atomic operation compatibility**: Same 16-byte and 8-byte CAS semantics
- **Bidirectional communication**: C++ ↔ Python in both directions
- **Multi-producer support**: Mix C++ and Python producers on the same queue

**Platform Support for C++/Python Interop:**
- ✅ **Linux/macOS**: Full interoperability (both use POSIX `shm_open`)
- ✅ **Windows**: Full interoperability
- ✅ **Python-only**: Works on all platforms (Windows/Linux/macOS)

#### Basic C++ → Python Example

**C++ Producer:**
```cpp
#include "queue.h"

int main() {
    // Open existing queue created by Python
    slick::SlickQueue<uint8_t> q(32, "shared_queue");

    for (int i = 0; i < 100; i++) {
        auto idx = q.reserve();
        uint32_t value = i;
        std::memcpy(q[idx], &value, sizeof(value));
        q.publish(idx);
    }
}
```

**Python Consumer:**
```python
from slick_queue_py import SlickQueue
import struct

# Create queue that C++ will write to
q = SlickQueue(name='shared_queue', size=64, element_size=32)

read_index = 0
for _ in range(100):
    data, size, read_index = q.read(read_index)
    if data is not None:
        value = struct.unpack("<I", data[:4])[0]
        print(f"Received from C++: {value}")

q.close()
q.unlink()
```

#### Building C++ Programs

To use the C++ SlickQueue library with your Python queues:

```bash
# Clone the C++ library
git clone https://github.com/SlickQuant/slick_queue.git

# Build your C++ program
g++ -std=c++17 -I slick_queue/include my_program.cpp -o my_program
```

Or use CMake (see [CMakeLists.txt](CMakeLists.txt) for reference):

```cmake
include(FetchContent)
FetchContent_Declare(
    slick_queue
    GIT_REPOSITORY https://github.com/SlickQuant/slick_queue.git
    GIT_TAG main
)
FetchContent_MakeAvailable(slick_queue)

add_executable(my_program my_program.cpp)
target_link_libraries(my_program PRIVATE slick_queue)
```

See [tests/test_interop.py](tests/test_interop.py) and [tests/cpp_*.cpp](tests/) for comprehensive examples.

## API Reference

### SlickQueue

#### `__init__(*, name=None, size=None, element_size=None)`

Create a queue in local memory or shared memory mode.

**Parameters:**
- `name` (str, optional): Shared memory segment name. If None, uses local memory mode (single process).
- `size` (int): Queue capacity (must be power of 2). Required for local mode or when creating shared memory.
- `element_size` (int, required): Size of each element in bytes

**Examples:**
```python
# Local memory mode (single process)
q = SlickQueue(size=256, element_size=64)

# Create new shared memory queue
q = SlickQueue(name='my_queue', size=256, element_size=64)

# Open existing shared memory queue
q2 = SlickQueue(name='my_queue', element_size=64)
```

#### `reserve(n=1) -> int`

Reserve `n` elements for writing. **Multi-producer safe** using atomic CAS.

**Parameters:**
- `n` (int): Number of elements to reserve (default 1)

**Returns:**
- `int`: Starting index of reserved space

**Example:**
```python
idx = q.reserve(1)  # Reserve 1 elements
```

#### `publish(index, n=1)`

Publish data written to reserved space. Uses atomic operations with release memory ordering.

**Parameters:**
- `index` (int): Index returned by `reserve()`
- `n` (int): Number of elements to publish (default 1)

**Example:**
```python
idx = q.reserve()
q[idx][:data_len] = data
q.publish(idx)
```

#### `read(read_index) -> Tuple[Optional[bytes], int, int]` or `read(atomic_cursor) -> Tuple[Optional[bytes], int]`

Read from queue with two modes:

**Single-Consumer Mode** (when `read_index` is `int`):
Uses a plain int cursor for single-consumer scenarios. Returns the new read_index.

**Multi-Consumer Mode** (when `read_index` is `AtomicCursor`):
Uses an atomic cursor for work-stealing/load-balancing across multiple consumers.
Each consumer atomically claims items, ensuring each item is consumed exactly once.

**Parameters:**
- `read_index` (int or AtomicCursor): Current read position or shared atomic cursor

**Returns:**
- Single-consumer: `Tuple[Optional[bytes], int, int]` - (data or None, size, new_read_index)
- Multi-consumer: `Tuple[Optional[bytes], int]` - (data or None, size)

**API Difference from C++:**
Unlike C++ where `read_index` is updated by reference, the Python single-consumer version returns the new index.
This is the Pythonic pattern since Python doesn't have true pass-by-reference.

```python
# Python single-consumer (returns new index)
data, size, read_index = q.read(read_index)

# Python multi-consumer (atomic cursor)
from slick_queue_py import AtomicCursor
cursor = AtomicCursor(cursor_shm.buf, 0)
data, size, index = q.read(cursor)  # Atomically claim next item

# C++ (updates by reference for both)
auto [data, size] = queue.read(read_index);  // read_index modified in-place
auto [data, size] = queue.read(atomic_cursor);  // atomic_cursor modified in-place
```

**Single-Consumer Example:**
```python
read_index = 0
while True:
    data, size, read_index = q.read(read_index)
    if data is not None:
        process(data)
```

**Multi-Consumer Example (Local Mode - Threading):**
```python
from slick_queue_py import AtomicCursor

# Create local cursor for multi-threading
cursor_buf = bytearray(8)
cursor = AtomicCursor(cursor_buf, 0)
cursor.store(0)

# Multiple threads can share this cursor
while True:
    data, size, index = q.read(cursor)  # Each thread atomically claims items
    if data is not None:
        process(data)
```

**Multi-Consumer Example (Shared Memory Mode - Multiprocess):**
```python
from multiprocessing import shared_memory
from slick_queue_py import AtomicCursor

# Create shared cursor for multi-process
cursor_shm = shared_memory.SharedMemory(name='cursor', create=True, size=8)
cursor = AtomicCursor(cursor_shm.buf, 0)
cursor.store(0)

# Multiple processes can share this cursor
while True:
    data, size, index = q.read(cursor)  # Each process atomically claims items
    if data is not None:
        process(data)
```

#### `read_last() -> Optional[bytes]`

Read the most recently published item.

**Returns:**
- `Optional[bytes]`: Last published data or None

#### `__getitem__(index) -> memoryview`

Get memoryview for writing to reserved slot.

**Parameters:**
- `index` (int): Index from `reserve()`

**Returns:**
- `memoryview`: View into the data array

#### `close()`

Close the shared memory connection. Always call this before unlinking.

#### `unlink()`

Delete the shared memory segment. Only call from the process that created it.

### AtomicCursor

The `AtomicCursor` class enables multi-consumer work-stealing patterns by providing an atomic read cursor that multiple consumers can coordinate through. Works in both local mode (multi-threading) and shared memory mode (multi-process).

#### `__init__(buffer, offset=0)`

Create an atomic cursor wrapper around a memory buffer.

**Parameters:**
- `buffer` (memoryview or bytearray): Memory buffer
  - For local mode (threading): use `bytearray(8)`
  - For shared memory mode (multiprocess): use `SharedMemory.buf`
- `offset` (int, optional): Byte offset in buffer (default 0)

**Local Mode Example (Multi-Threading):**
```python
from slick_queue_py import AtomicCursor

# Create local cursor for multi-threading
cursor_buf = bytearray(8)
cursor = AtomicCursor(cursor_buf, 0)
cursor.store(0)  # Initialize to 0
```

**Shared Memory Mode Example (Multi-Process):**
```python
from multiprocessing import shared_memory
from slick_queue_py import AtomicCursor

# Create shared cursor for multi-process
cursor_shm = shared_memory.SharedMemory(name='cursor', create=True, size=8)
cursor = AtomicCursor(cursor_shm.buf, 0)
cursor.store(0)  # Initialize to 0
```

#### `load() -> int`

Load the cursor value with atomic acquire semantics.

**Returns:**
- `int`: Current cursor value

#### `store(value)`

Store a new cursor value with atomic release semantics.

**Parameters:**
- `value` (int): New cursor value

#### `compare_exchange_weak(expected, desired) -> Tuple[bool, int]`

Atomically compare and swap the cursor value.

**Parameters:**
- `expected` (int): Expected cursor value
- `desired` (int): Desired cursor value

**Returns:**
- `Tuple[bool, int]`: (success, actual_value)

**Note:** This is used internally by `read(atomic_cursor)` and typically doesn't need to be called directly.

## Memory Layout

The queue uses the same memory layout as C++ `slick::SlickQueue<T>`:

```
Offset | Size          | Content
-------|---------------|------------------
0      | 16 bytes      | reserved_info (atomic)
       |   0-7         |   uint64_t index_
       |   8-11        |   uint32_t size_
       |   12-15       |   padding
16     | 4 bytes       | uint32_t size_ (queue capacity)
20     | 44 bytes      | padding (to 64 bytes)
64     | 16*size bytes | slot array
       | per slot:     |
       |   0-7         |   uint64_t data_index (atomic)
       |   8-11        |   uint32_t size
       |   12-15       |   padding
64+... | elem*size     | data array
```

## Platform Support

### Fully Supported (Lock-Free)
- **Windows x86-64**: Uses C++ extension (`atomic_ops_ext.pyd`) with `std::atomic`
- **Linux x86-64**: Uses C++ extension (`atomic_ops_ext.so`) with `std::atomic`, fallback to `libatomic`
- **macOS x86-64**: Uses C++ extension (`atomic_ops_ext.so`) with `std::atomic`, fallback to compiler builtins

**Platform-specific atomic operation implementations:**
- **All platforms**: The `atomic_ops_ext` C++ extension is now used on all platforms for the most reliable cross-process atomic operations
- **Fallback support**: Linux/macOS can fall back to `libatomic` or compiler builtins if the extension isn't available

### Building and Installation

The C++ extension is built automatically during installation:

```bash
# Install with automatic extension build
pip install -e .

# Or build manually first
python setup.py build_ext --inplace
pip install -e .
```

**Build requirements:**
- **Windows**: Visual Studio 2017+ or MSVC build tools
- **Linux**: GCC 5+ or Clang 3.8+
- **macOS**: Xcode command line tools (clang)
- **All platforms**: Python development headers (included with standard Python installation)

The extension will be built as:
- Windows: `atomic_ops_ext.cp3XX-win_amd64.pyd`
- Linux: `atomic_ops_ext.cpython-3XX-x86_64-linux-gnu.so`
- macOS: `atomic_ops_ext.cpython-3XX-darwin.so`

(where `XX` is your Python version, e.g., `312` for Python 3.12)

### Requirements for Lock-Free Operation

**All platforms require hardware support for lock-free atomic operations:**
- x86-64 CPU with CMPXCHG16B instruction (Intel since ~2006, AMD since ~2007)
- For C++/Python interoperability, both must use the same atomic hardware instructions
- No fallback implementation exists - lock-free atomics are mandatory for multi-producer queues

**Why no fallback?**
The queue requires true atomic CAS operations for correctness in multi-producer scenarios. A lock-based fallback would:
- Break binary compatibility with C++ SlickQueue
- Fail to work correctly in multi-process scenarios (Python ↔ C++)
- Not provide the performance guarantees of a lock-free queue

### Not Supported
- 32-bit platforms (no 16-byte atomic CAS)
- ARM64 (requires ARMv8.1+ CASP instruction - future support planned)
- CPUs without CMPXCHG16B support (very old x86-64 CPUs from before 2006)

Check platform support:
```python
from atomic_ops import check_platform_support

supported, message = check_platform_support()
print(f"Platform: {message}")
```

## Performance

Typical throughput on modern hardware (x86-64):
- Single producer/consumer: ~5-10M items/sec
- 4 producers/1 consumer: ~3-8M items/sec
- High contention (8+ producers): ~1-5M items/sec

Performance depends on:
- CPU cache topology
- Queue size (smaller = more contention)
- Item size
- Memory bandwidth

## Advanced Usage

### Batch Operations

Reserve and publish multiple elements at once:

```python
# Reserve 10 elements
idx = q.reserve(10)

# Write data to each slot
for i in range(10):
    element = q[idx + i]
    element[:data_len] = data[i]

# Publish all 10 elements at once
q.publish(idx, 10)
```

### Wrap-Around Handling

The queue automatically handles ring buffer wrap-around:

```python
# Queue with size=8
q = SlickQueue(name='wrap_test', size=8, element_size=32)

# Reserve more items than queue size - wraps automatically
for i in range(100):
    idx = q.reserve()
    q[idx][:4] = struct.pack("<I", i)
    q.publish(idx)
```

## Testing

### Python Tests

Run the Python test suite:

```bash
# Atomic operations tests (clean output)
python tests/run_test.py tests/test_atomic_ops.py

# Basic queue tests (clean output)
python tests/run_test.py tests/test_queue.py

# Local mode tests
python tests/test_local_mode.py

# Multi-producer/consumer tests
# Note: If tests fail with "File exists" errors, run cleanup first:
python tests/cleanup_shm.py
python tests/test_multi_producer.py
```

### C++/Python Interoperability Tests

Build and run comprehensive interop tests:

```bash
# 1. Build C++ test programs with CMake
mkdir build && cd build
cmake ..
cmake --build .

# 2. Run interoperability test suite
cd ..
python tests/test_interop.py

# Or run specific tests:
python tests/test_interop.py --test python_producer_cpp_consumer
python tests/test_interop.py --test cpp_producer_python_consumer
python tests/test_interop.py --test multi_producer_interop
python tests/test_interop.py --test stress_interop
python tests/test_interop.py --test cpp_shm_creation
```

The interop tests verify:
- **Python → C++**: Python producers write data that C++ consumers read
- **C++ → Python**: C++ producers write data that Python consumers read
- **Mixed Multi-Producer**: Multiple C++ and Python producers writing to same queue
- **Stress Test**: High-volume bidirectional communication
- **SHM created by C++**: C++ producers create the SHM and write data that Python consumers read

**Note on Windows**: If child processes from previous test runs don't terminate properly, you may need to manually kill orphaned python.exe processes before running tests again.

## Known Issues

1. **Buffer Cleanup Warning**: You may see a `BufferError: cannot close exported pointers exist` warning during garbage collection. This is a **harmless warning** caused by Python's ctypes creating internal buffer references that persist beyond explicit cleanup. It occurs during program exit and **does not affect functionality, performance, or correctness**. The queue works perfectly despite this warning.

2. **UserWarning**: On Linux you may see `UserWarning: resource_tracker: There appear to be 4 leaked shared_memory objects to clean up at shutdown`. This is a **harmless warning** caused by Python's ctypes creating internal buffer references that persist beyond explicit cleanup. It occurs during program exit and **does not affect functionality, performance, or correctness**. The queue works perfectly despite this warning.

## Architecture

### Atomic Operations

The queue uses platform-specific atomic operations:

- **8-byte CAS**: For `reserved_info` structure (multi-producer coordination)
- **8-byte CAS**: For slot `data_index` fields (publish/read synchronization)
- **Memory barriers**: Acquire/release semantics for proper ordering

### Memory Ordering

- `reserve()`: Uses `memory_order_release` on successful CAS
- `publish()`: Uses `memory_order_release` for data_index store
- `read()`: Uses `memory_order_acquire` for data_index load

This ensures:
- All writes to data are visible before publishing
- All reads of data happen after acquiring the index
- No reordering that could cause data races

## Comparison with C++

| Feature | C++ | Python |
|---------|-----|--------|
| Multi-producer | ✅ | ✅ |
| Multi-consumer (work-stealing) | ✅ | ✅ (with AtomicCursor) |
| Lock-free (x86-64) | ✅ | ✅ |
| Memory layout | Reference | Matches exactly |
| Performance | Baseline | ~50-80% of C++ |
| Ease of use | Medium | High |
| read(int) single-consumer | ✅ | ✅ |
| read(atomic cursor) multi-consumer | ✅ | ✅ |

## Contributing

Issues and pull requests welcome at [SlickQuant/slick-queue-py](https://github.com/SlickQuant/slick-queue-py).

## License

MIT License - see LICENSE file for details.

**Made with ⚡ by [SlickQuant](https://github.com/SlickQuant)**