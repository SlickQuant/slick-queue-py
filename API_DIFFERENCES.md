# C++ / Python API Differences

This document explains the intentional API differences between the C++ `slick::queue<T>` and Python `SlickQueue` implementations.

Both track the same version line; this document describes slick-queue v2.1.0.

## Core Compatibility

Both implementations:
- ✅ Use identical memory layouts
- ✅ Use identical atomic operations
- ✅ Are fully interoperable (C++ ↔ Python communication works)
- ✅ Support lock-free multi-producer multi-consumer semantics

## API Differences

### 1. Constructor / Opening Queues

**C++:**
```cpp
// Create new queue in shared memory
slick::queue<uint8_t> q(queue_size, queue_name);

// Open existing queue
slick::queue<uint8_t> q(queue_name);

// Local memory mode
slick::queue<uint8_t> q(queue_size); // No shared memory

// Minimum of 64 items per reservation (one control slot per 64 items)
slick::queue<uint8_t> q(queue_size, 64, queue_name);
```

**Python:**
```python
# Create new queue in shared memory
q = SlickQueue(name=queue_name, size=queue_size, element_size=element_size, create=True)

# Open existing queue
q = SlickQueue(name=queue_name, element_size=element_size)

# Local memory mode
q = SlickQueue(size=queue_size, element_size=element_size)  # No shared memory

# Minimum of 64 items per reservation (one control slot per 64 items)
q = SlickQueue(name=queue_name, size=queue_size, element_size=1, items_per_slot=64)
```

**Rationale:** Python uses keyword arguments for clarity and supports both shared memory and local memory modes.

`items_per_slot` is the one place the opener differs: C++ `queue(name)` can only adopt the
segment's value, while Python's opener also accepts `items_per_slot=` and raises `ValueError`
if the segment disagrees. Omitting it adopts the segment's value, exactly as C++ does.

---

### 2. read() Method - Return Value vs. Reference Parameter

**C++:**
```cpp
uint64_t read_index = 0;

// read() returns (data, size) and updates read_index by reference
auto [data, size] = queue.read(read_index);  // read_index modified in-place

if (data != nullptr) {
    process(data, size);
    // read_index has been updated automatically
}
```

**Python:**
```python
read_index = 0

# read() returns (data, size, new_read_index)
data, size, read_index = q.read(read_index)  # Returns new index

if data is not None:
    process(data, size)
    # read_index now holds the new value
```

**Rationale:**
- Python doesn't have true pass-by-reference for primitive types
- Returning the new index is the Pythonic pattern (like `list.pop()`, `str.split()`, etc.)
- The assignment `read_index = q.read(read_index)` is clear and explicit

**Important:** This is a **usage difference**, not a compatibility issue. The underlying memory operations are identical. When C++ and Python processes communicate:
- C++ `read()` and Python `read()` both read from the same atomic slots
- Both update their local `read_index` variable correctly
- The memory layout and atomic semantics are identical

---

### 3. Feature Configuration (Traits)

**C++:**
```cpp
// A template parameter - differently configured queues are different types
struct my_traits : slick::queue_traits {
    static constexpr bool enable_reset_check = true;
    static constexpr bool enable_read_last   = false;
};

slick::queue<int, my_traits> lean(1024);
slick::queue<int>            standard(1024);  // default_queue_traits
```

**Python:**
```python
# A constructor argument - the configuration is per-instance
class MyTraits(QueueTraits):
    enable_reset_check = True
    enable_read_last = False

lean = SlickQueue(size=1024, element_size=4, traits=MyTraits)
standard = SlickQueue(size=1024, element_size=4)  # default_queue_traits
```

**Rationale:** Python has no templates, so the traits type is passed as a value
rather than a template argument. The consequences differ slightly:

| | C++ | Python |
|---|---|---|
| Malformed traits type | `queue_traits_type` concept, compile error | `validate_traits()`, `TypeError` at construction |
| Mutating the traits after use | Impossible - `static constexpr` members | Snapshotted at construction; `q.traits` is read-only |
| `read_last()` without `enable_read_last` | `static_assert`, compile error | `RuntimeError` at the call |
| Traits types | Two (`queue_traits`, `debug_queue_traits`) so a debug/release mismatch fails to link | One - no translation units, so nothing for a second type to protect |
| Default loss detection | Keyed off `NDEBUG` (Debug on, Release off) | Always on (+0.2%); not keyed off `__debug__`, which would zero `loss_count()` under `python -O` |
| Layout when a feature is off | Its counter is dropped from the object | No object-layout effect |

**Important:** `enable_read_last` is part of the *shared-memory* contract in both
languages, and they agree byte for byte. The creator records it in the segment's
layout marker - `'SLQ1'` when the last-published index is maintained, `'SLQ0'`
when it is not - and every attacher matches it against its own configuration,
raising `RuntimeError` (C++ `std::runtime_error`) on a mismatch in either
direction. A Python `'SLQ0'` segment is therefore rejected by a C++ peer built
with the feature on, and vice versa. The remaining traits are process-local and
mix freely on one segment, so a Python reader with `enable_reset_check` on can
share a segment with a C++ writer that has it off.

---

### 4. Lossy Reads: Pointer vs. Copy

**C++:**
```cpp
auto [data, size] = queue.read(read_index);   // data is a T* into the ring
process(data, size);                          // producer may overwrite as you read
```

**Python:**
```python
data, size, read_index = q.read(read_index)   # data is a bytes copy
```

**Rationale:** Python cannot hand out a raw pointer into the ring safely, so
`read()` copies. This changes what "lossy" feels like, without changing the
guarantee.

Both implementations guarantee that the cursor and the returned size describe one
and the same record - that is what the v2.0.0 seqlock validation fixed. Neither
guarantees the *contents*: a producer writes an element's data before it publishes
the slot's new index, so a consumer that has been lapped can read bytes the
producer is midway through overwriting, and no check on the slot can detect it
because the index has not changed yet.

The difference is only in how visible this is. A C++ caller holds a pointer into a
ring buffer and can see that its target is volatile. A Python caller holds what
looks like a snapshot, but under overrun those bytes may belong to a newer record
than the cursor names. Use `loss_count()` to detect it: a non-zero count means this
consumer was overrun and bytes may have been overwritten in flight. If your data
must be self-identifying, put a sequence number in the payload.

---

### 5. Element Access

**C++:**
```cpp
auto idx = queue.reserve();
uint8_t* slot = queue[idx];  // Returns pointer
std::memcpy(slot, data, data_len);
queue.publish(idx);
```

**Python:**
```python
idx = q.reserve()
slot = q[idx]  # Returns memoryview
slot[:data_len] = data
q.publish(idx)
```

**Rationale:** Python uses `memoryview` for safe buffer access instead of raw pointers.

---

### 6. Type Safety

**C++:**
```cpp
// Template-based, compile-time type safety
slick::queue<int32_t> q(1024, "queue");
auto idx = q.reserve();
*q[idx] = 42;  // Type-safe int32_t access
```

**Python:**
```python
# Element size specified, runtime packing/unpacking
q = SlickQueue(name='queue', size=1024, element_size=4)
idx = q.reserve()
import struct
q[idx][:4] = struct.pack("<i", 42)  # Manual packing
```

**Rationale:** Python doesn't have templates. Users explicitly handle serialization with `struct` module.

---

### 7. Resource Management

**C++:**
```cpp
{
    slick::queue<T> q(size, "name");
    // ... use queue ...
}  // Destructor automatically cleans up
```

**Python:**
```python
# Option 1: Manual cleanup
q = SlickQueue(name='name', size=size, element_size=elem_size, create=True)
try:
    # ... use queue ...
finally:
    q.close()
    q.unlink()  # Only call from creator process

# Option 2: Context manager (recommended)
with SlickQueue(name='name', size=size, element_size=elem_size, create=True) as q:
    # ... use queue ...
    pass  # Automatic cleanup
```

**Rationale:** Python's garbage collection is non-deterministic, so explicit cleanup or context managers are needed.

---

## Interoperability Examples

### Example 1: C++ Producer → Python Consumer

**C++ (producer.cpp):**
```cpp
#include <slick/queue.hpp>
#include <cstring>

int main() {
    slick::queue<uint8_t> queue("my_queue");  // Open existing queue

    for (int i = 0; i < 100; i++) {
        auto idx = queue.reserve();
        uint32_t value = i;
        std::memcpy(queue[idx], &value, sizeof(value));
        queue.publish(idx);
    }
}
```

**Python (consumer.py):**
```python
from slick_queue_py import SlickQueue
import struct

# Create queue (C++ will open it)
q = SlickQueue(name='my_queue', size=64, element_size=32, create=True)

read_index = 0
for _ in range(100):
    data, size, read_index = q.read(read_index)
    if data:
        value = struct.unpack("<I", data[:4])[0]
        print(f"Received: {value}")

q.close()
q.unlink()
```

### Example 2: Python Producer → C++ Consumer

**Python (producer.py):**
```python
from slick_queue_py import SlickQueue
import struct

q = SlickQueue(name='my_queue', size=64, element_size=32, create=True)

for i in range(100):
    idx = q.reserve()
    q[idx][:4] = struct.pack("<I", i)
    q.publish(idx)

q.close()
# Don't unlink yet - C++ will use it
```

**C++ (consumer.cpp):**
```cpp
#include <slick/queue.hpp>
#include <iostream>
#include <cstring>

int main() {
    slick::queue<uint8_t> queue("my_queue");  // Open existing queue

    uint64_t read_index = 0;
    for (int i = 0; i < 100; i++) {
        auto [data, size] = queue.read(read_index);  // Updates read_index

        if (data) {
            uint32_t value;
            std::memcpy(&value, data, sizeof(value));
            std::cout << "Received: " << value << std::endl;
        }
    }
}
```

## Summary

| Feature | C++ | Python | Reason for Difference |
|---------|-----|--------|---------------------|
| `read()` return | `(data, size)` | `(data, size, new_index)` | Python has no pass-by-reference |
| Constructor | `(size, name)` or `(name)` | Named parameters with `create` flag | Python idioms |
| Element access | Pointer | `memoryview` | Python memory safety |
| Type safety | Templates | `struct` module | Language difference |
| Cleanup | RAII (destructor) | Explicit or context manager | Python GC is non-deterministic |
| `read()` data | `T*` into the ring | `bytes` copy | No safe raw pointers in Python |
| Overwritten-in-flight data | Caller sees a volatile pointer | Copy may hold a newer record's bytes | Detect with `loss_count()` |
| Feature configuration | `Traits` template parameter | `traits=` argument | Python has no templates |
| Misconfigured traits | Compile error (concept / `static_assert`) | `TypeError` / `RuntimeError` at runtime | No compile step |
| Default loss detection | Off in Release | Always on | +0.2% here, and the only overrun signal a caller has |
| Layout marker | `'SLQ1'` / `'SLQ0'` | Identical, and validated against C++ peers | Shared contract |

**Bottom Line:** The Python API is designed to be Pythonic while maintaining **100% binary compatibility** with C++. All atomic operations and memory layouts are identical, enabling seamless C++/Python interoperability.
