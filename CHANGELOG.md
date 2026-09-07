# Changelogs

## [v2.0.0] - 2026-09-07

Tracks C++ slick-queue v2.0.0. Feature configuration moves to a `traits`
argument, `enable_read_last` becomes part of the shared-memory contract so a
mismatched peer is rejected at attach time instead of degrading silently, and
several read-path correctness bugs are fixed.

### Breaking Changes

- **BREAKING:** `read_last()` requires `traits.enable_read_last` and raises
  `RuntimeError` otherwise. The legacy reserved-cursor fallback is gone with it;
  it reported reservations that were never published and truncated sizes above
  65,535.
- **BREAKING:** `enable_read_last` is part of the shared-memory contract. The
  header magic carries a feature nibble - `'SLQ1'` when the last-published index
  is maintained, `'SLQ0'` when it is not - and every attacher matches it against
  its own configuration, raising `RuntimeError` on a mismatch in either
  direction. The other traits stay process-local and mix freely on one segment.
- **BREAKING:** Segments created before slick-queue v1.4.0 carry no layout marker
  and are rejected at attach time, rather than served by the reserved-cursor
  fallback.
- **BREAKING:** Reset detection is opt-in via `enable_reset_check` (default off),
  matching C++. `read()` no longer loads the producer's reservation counter on
  every call.
- **BREAKING:** `q.traits` is a read-only snapshot, not the class passed in, so
  `q.traits is MyTraits` is now False and its attributes cannot be assigned.
- **BREAKING:** The private `_last_published_valid` attribute is gone; use
  `q.traits.enable_read_last`.
- `reserve(0)` raises `ValueError` instead of reserving nothing.

### Added

- `QueueTraits`, `default_queue_traits`, `validate_traits()`, and a `traits=`
  argument on `SlickQueue`. Subclass `QueueTraits` to override
  `enable_read_last`, `enable_reset_check`, `enable_loss_detection` or
  `enable_cpu_relax`; differently configured queues coexist in one process.
  `validate_traits()` rejects a malformed traits type at construction, as the C++
  `queue_traits_type` concept does, and like it cannot catch a misspelled
  override.
- Constants `HEADER_MAGIC_FEATURE_MASK`, `HEADER_MAGIC_READ_LAST` and
  `SLOT_SIZE_OFFSET`.
- 40 tests. `tests/test_traits.py` (24) covers traits validation, the marker each
  configuration writes, both mismatch directions, unmarked and unknown-feature
  segments, and mutation of a traits class after construction.
  `tests/test_reset_detection.py` (9) asserts a stale reader actually rewinds
  onto the new generation, across both read paths and both memory modes.
  `tests/test_wrap_invariants.py` (7) pins the cursor/size invariant under a
  wrapping producer; three drive the recycle deterministically, because CPython
  switches threads far too rarely to hit the window by racing.

### Fixed

- Both `read()` overloads loaded `slot.size` twice, unvalidated, so a producer
  recycling the slot between the loads left the cursor describing a different
  record than the caller was handed - in the shared-cursor overload the two loads
  straddled the claiming CAS. Both now load it once and re-validate the slot
  before committing, as `read_last()` does.
- `read_last()` could return a `(data, size)` pair describing two different
  records; the slot is now validated before and after the size load.
- Reset detection never fired for the case it exists to handle. It compared the
  *slot's* index against the reservation counter, but `reset()` clears the
  control array before rewinding the counter, so a stale reader always landed on
  a slot holding an invalid or fresh-low index - neither ahead of the counter.
  The test is now on the reader's cursor, shared by both read paths through
  `_was_reset()`.
- The reset branch in the shared-cursor `read()` overwrote the cursor with a
  blind store, discarding other consumers' progress; it now rewinds via
  compare-exchange.
- The single-consumer `read()` charged the loss counter when the overrun was
  spotted rather than on commit, so a retry could count the same overrun twice.
- `reset()` did not rewind the reservation counter in local memory mode.
- Traits were re-read from the caller's class on every `publish()` and `read()`,
  while the layout marker and the optional atomics were created once. Mutating
  the class afterwards froze `read_last()` on an `'SLQ1'` segment, or raised
  `AttributeError` from `publish()` and `reset()` because the last-published
  atomic had never been created. They are snapshotted at construction now.

### Changed

- Loss detection is gated on `enable_loss_detection`, on by default. C++ keys its
  equivalent off `NDEBUG` and needs two traits types so a debug/release mismatch
  fails to link instead of violating the ODR; Python has no translation units to
  protect, and `__debug__` was rejected because it would zero `loss_count()`
  under `python -O`. It costs +0.2% on a reader that keeps up.
- `publish()` updates the last-published index unconditionally when the feature
  is on - validating the nibble once at attach time makes the old per-call
  `_last_published_valid` check redundant.
- Contended CAS loops in `reserve()` and the shared-cursor `read()` back off
  through `_cpu_relax()`, gated on `enable_cpu_relax`.
- C++ interop programs include the renamed `<slick/queue.hpp>` and use the
  `slick::queue` alias; the old `<slick/queue.h>` path is a deprecation shim.
- `tests/CMakeLists.txt` registers `test_loss_count.py` (missed in v1.2.0) and
  the three new files. `tests/test_interop.py` bounds each child-result `get()`,
  which previously hung a run forever when a child was killed on a join timeout.

### Notes

`slot.size` became a `std::atomic<uint32_t>` in C++ but stays a plain struct
field here: that change fixed C++ undefined behaviour and compiler re-loading,
neither of which exists in CPython, and the field is already an aligned 4-byte
access with an identical wire format.

The seqlock validation guarantees the cursor and the returned size describe one
and the same record. It does **not** make the returned bytes a snapshot: a
producer writes an element's data before it publishes the slot's new index, so a
lapped consumer can copy bytes mid-overwrite and no check on the slot can detect
it. C++ has the identical hazard behind the pointer it returns; `loss_count()` is
how a consumer detects it. See README.md and API_DIFFERENCES.md.

### Compatibility

Verified against C++ slick-queue v2.0.0 binaries: all 10 interop tests pass, and
a Python-created `'SLQ0'` segment is rejected by a C++ peer built with
`enable_read_last` on, with the message the C++ side raises.

## [v1.2.0] - 2026-07-09

### Added
- `loss_count()`: per-instance count of items skipped due to overwrite, matching
  C++ slick-queue v1.5.0 (queue.h:230-236). Counted in both the single-consumer
  read (slot overwritten by a newer generation at the same position) and the
  AtomicCursor work-stealing read (overrun attributed to the claiming consumer).
  Cleared by `reset()`. Note: Python always counts; C++ counts only when
  `SLICK_QUEUE_ENABLE_LOSS_DETECTION` is enabled (debug builds by default).
- `initial_reading_index()`: cursor for a late-joining consumer - 0 for a newly
  created queue, or the current writing index of an opened queue, matching C++
  slick-queue v1.5.0 (queue.h:242-244).
- `tests/test_loss_count.py`: 7 tests covering both new methods.

These are per-instance API additions only - the shared memory layout is unchanged
and remains fully compatible with C++ slick-queue v1.x ('SLQ1' format).

## [v1.1.0] - 2026-02-04

### Breaking Changes
- **BREAKING:** `read_last()` return signature changed from `Optional[bytes]` to `Tuple[Optional[bytes], int]`
  - Now returns `(data, size)` tuple where `size` indicates the number of slots the item occupies
  - Migration: Change `data = q.read_last()` to `data, size = q.read_last()`
  - Provides actual slot size information instead of always returning full `element_size`

### Added
- Modern format support with efficient `last_published_` atomic tracking
  - New `last_published_` atomic field at offset 16 for O(1) `read_last()` performance
  - Header magic number `0x534C5131` ('SLQ1') at offset 24 for format detection
  - Init state atomic at offset 48 for CAS-based ownership detection
  - Automatic format detection distinguishes modern vs legacy queues
- CAS-based queue ownership detection matching C++ implementation (queue.h:618-648)
- Helper methods `_wait_for_shared_memory_ready()` and `_detect_format_version()`
- Comprehensive test suites:
  - `test_read_last.py`: 10 tests covering empty queue, single/multiple items, multi-slot publishes, concurrent publishes, reset, shared memory, wraparound, variable sizes
  - `test_read_last_interop.py`: 5 tests for Python ↔ C++ interoperability
  - `test_modern_format.py`: 6 tests for format compatibility and memory layout verification
- C++ test program `cpp_read_last_tester.cpp` for read_last() interoperability testing
- New constants: `SIZE_OFFSET`, `ELEMENT_SIZE_OFFSET`, `LAST_PUBLISHED_OFFSET`, `HEADER_MAGIC_OFFSET`, `HEADER_MAGIC`, `INIT_STATE_OFFSET`, `INIT_STATE_*`, `K_INVALID_INDEX`
- Documentation: `READ_LAST_INTEROP.md` comprehensive interoperability guide

### Changed
- Updated shared memory header layout to match C++ slick-queue v1.x:
  - Reserved info: 8 bytes at offset 0-7 (compact from previous 32-byte padded format)
  - Size: 4 bytes at offset 8-11 (moved from offset 32)
  - Element size: 4 bytes at offset 12-15 (moved from offset 36)
  - Last published: 8 bytes at offset 16-23 (NEW)
  - Header magic: 4 bytes at offset 24-27 (NEW)
  - Init state: 4 bytes at offset 48-51 (NEW)
- `publish()` now updates `last_published_` atomic using CAS loop in modern format (queue.h:331-337)
- `reset()` now resets `last_published_` to `K_INVALID_INDEX` when in modern format
- `close()` properly releases `_atomic_last_published` wrapper
- `AtomicReservedInfo.RESERVED_INFO_FMT` simplified from 32-byte to 8-byte format
- Repository name changed from slick_queue_py to slick-queue-py
- Updated README.md with new `read_last()` API documentation
- Enhanced CMakeLists.txt with new test targets

### Fixed
- `read_last()` now returns actual slot size instead of always returning full `element_size`
- Test queue names shortened to comply with macOS 31-character shared memory name limit

### Performance
- O(1) `read_last()` via direct atomic load (improved from reserved_info calculation)
- Reduced header size: 8-byte reserved_info vs 32-byte padded (24 bytes saved)
- Lock-free `last_published_` tracking enables efficient concurrent access

### Compatibility
- **Backward Compatible:** Automatically detects and supports legacy queue format
- **C++ Interop:** Full compatibility with C++ slick-queue modern format
- Python and C++ can share queues created by either language
- All existing tests continue to pass (atomic ops, cursors, local mode, multi-producer, interop)

## [v1.0.1] - 2025-12-27

### Fixed
- Fixed extra `/` prefix being added to shared memory names on POSIX systems (Linux/macOS)
- Fixed test failures on Linux and macOS platforms

### Added
- macOS shared memory name length validation (31 character limit including `/` prefix)
- C++ extension support for Linux/macOS platforms with automatic fallback to native methods if unavailable
- `atomic_store_64` and `atomic_cas_64` functions to C++ extension for improved cross-platform consistency

### Changed
- Linux/macOS now prioritize C++ extension for atomic operations, falling back to native methods (`__sync_val_compare_and_swap` or `libatomic`) if extension is not available
- Improved atomic operation reliability across all platforms
- Enhanced test suite for better cross-platform compatibility
- Publishing: Updated PyPI builds to use `cibuildwheel` for proper manylinux wheel generation
- Ignore conda publish failing for now

## [v1.0.0] - 2025-12-26

- Initial release
- Python implementation of SlickQueue - a lock-free multi-producer multi-consumer (MPMC) queue with C++ interoperability through shared memory.
- Windows, Linux, and macOS support