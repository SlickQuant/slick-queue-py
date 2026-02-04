# Changelogs

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