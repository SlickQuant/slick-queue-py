# Changelogs

## [unreleased]

### Changed
- Changed repository name from slick_queue_py to slick-queue-py

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