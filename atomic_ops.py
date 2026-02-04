"""
Platform-specific atomic operations for lock-free multi-producer multi-consumer queue.

This module provides atomic compare-and-swap (CAS) operations and memory barriers
that match the C++ std::atomic semantics used in slick::SlickQueue.

As of slick-queue v1.2.0+, reserved_info is a packed uint64_t (not a struct):
- Bits 0-15: size (16-bit, max 65535)
- Bits 16-63: index (48-bit, max 281 trillion)

Supported platforms:
- Windows x86-64: Uses C++ std::atomic<uint64_t> via extension
- Linux x86-64: Uses __atomic_compare_exchange_8 from libatomic
- macOS x86-64: Uses __atomic_compare_exchange_8 from libatomic

Memory ordering semantics:
- RELAXED: No synchronization or ordering constraints
- ACQUIRE: Subsequent loads/stores cannot be reordered before this operation
- RELEASE: Prior loads/stores cannot be reordered after this operation
- SEQ_CST: Sequential consistency (strongest guarantee)
"""
from __future__ import annotations

__version__ = '1.0.1'

import sys
import struct
import ctypes
from typing import Tuple, Optional, Union
from enum import IntEnum


# Bit packing/unpacking helpers for reserved_info (matches C++ slick-queue v1.2.0+)
# reserved_info is a uint64_t with: [48-bit index | 16-bit size]
def make_reserved_info(index: int, size: int) -> int:
    """Pack index (48-bit) and size (16-bit) into uint64_t.

    Matches C++: ((index & 0xFFFFFFFFFFFFULL) << 16) | (size & 0xFFFF)

    Args:
        index: Queue index (0 to 2^48-1)
        size: Reserved size (0 to 65535)

    Returns:
        Packed uint64_t value
    """
    return ((index & 0xFFFFFFFFFFFF) << 16) | (size & 0xFFFF)


def get_index(reserved: int) -> int:
    """Extract index (upper 48 bits) from reserved_info.

    Matches C++: reserved >> 16

    Args:
        reserved: Packed uint64_t reserved_info

    Returns:
        Queue index
    """
    return reserved >> 16


def get_size(reserved: int) -> int:
    """Extract size (lower 16 bits) from reserved_info.

    Matches C++: static_cast<uint32_t>(reserved & 0xFFFF)

    Args:
        reserved: Packed uint64_t reserved_info

    Returns:
        Reserved size
    """
    return reserved & 0xFFFF


class MemoryOrder(IntEnum):
    """Memory ordering constants matching C++ std::memory_order."""
    RELAXED = 0  # memory_order_relaxed
    ACQUIRE = 2  # memory_order_acquire
    RELEASE = 3  # memory_order_release
    SEQ_CST = 5  # memory_order_seq_cst


# Platform detection
IS_WINDOWS = sys.platform == 'win32'
IS_LINUX = sys.platform.startswith('linux')
IS_MACOS = sys.platform == 'darwin'
IS_64BIT = sys.maxsize > 2**32


def check_platform_support() -> Tuple[bool, str]:
    """
    Check if current platform supports required atomic operations.

    Returns:
        Tuple of (supported: bool, message: str)
    """
    if not IS_64BIT:
        return False, "64-bit platform required for atomic operations"

    if IS_WINDOWS:
        if _USE_EXTENSION:
            return True, "Windows x86-64 with C++ std::atomic extension"
        else:
            return False, "Windows requires C++ atomic_ops_ext extension for cross-process synchronization"
    elif IS_LINUX or IS_MACOS:
        import platform
        machine = platform.machine().lower()
        if machine in ('x86_64', 'amd64'):
            return True, f"{sys.platform} x86-64 with 64-bit atomic operations"
        elif machine in ('aarch64', 'arm64'):
            return True, f"{sys.platform} ARM64 with 64-bit atomic operations"
        else:
            return False, f"Unsupported architecture: {machine}"
    else:
        return False, f"Unsupported platform: {sys.platform}"


# Initialize platform-specific atomic functions
if IS_WINDOWS:
    # ===== Windows Implementation =====
    # Try to use our C++ extension for std::atomic operations
    try:
        import atomic_ops_ext
        _atomic_ops_ext = atomic_ops_ext
        _USE_EXTENSION = True
    except ImportError as e:
        # Try adding current file's directory to path and retry
        # (helps with multiprocessing on Windows where child processes may not have correct path)
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
            try:
                import atomic_ops_ext
                _atomic_ops_ext = atomic_ops_ext
                _USE_EXTENSION = True
            except ImportError as e2:
                _USE_EXTENSION = False
        else:
            _USE_EXTENSION = False

    _HAS_64BIT_CAS = _USE_EXTENSION  # Requires C++ extension for proper atomic ops

    def _platform_cas_64(buffer: memoryview, offset: int,
                        expected: int, desired: int) -> Tuple[bool, int]:
        """Windows-specific 64-bit CAS using C++ std::atomic<uint64_t> wrapper.

        Requires atomic_ops_ext C++ extension for cross-process synchronization.
        """
        if not _USE_EXTENSION:
            raise RuntimeError(
                "atomic_ops_ext C++ extension is required for cross-process atomic operations. "
                "Build it with: python setup.py build_ext --inplace"
            )

        # Use C++ extension that wraps std::atomic<uint64_t>
        # This ensures Python and C++ use the SAME atomic synchronization
        buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
        addr = ctypes.addressof(buf_array) + offset

        success, actual = _atomic_ops_ext.atomic_compare_exchange_64(
            addr, expected, desired
        )
        return bool(success), actual

    def _atomic_store_64(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 64-bit store with release semantics.

        Requires atomic_ops_ext C++ extension for cross-process synchronization.
        """
        if not _USE_EXTENSION:
            raise RuntimeError(
                "atomic_ops_ext C++ extension is required for cross-process atomic operations. "
                "Build it with: python setup.py build_ext --inplace"
            )

        buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
        addr = ctypes.addressof(buf_array) + offset

        # Use our C extension for atomic exchange (implements store via exchange)
        _atomic_ops_ext.atomic_exchange_64(addr, value)

    def _atomic_load_64(buffer: memoryview, offset: int) -> int:
        """Atomic 64-bit load with acquire semantics.

        Requires atomic_ops_ext C++ extension for cross-process synchronization.
        """
        if not _USE_EXTENSION:
            raise RuntimeError(
                "atomic_ops_ext C++ extension is required for cross-process atomic operations. "
                "Build it with: python setup.py build_ext --inplace"
            )

        buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
        addr = ctypes.addressof(buf_array) + offset

        # Use C++ extension for proper atomic load with acquire semantics
        return _atomic_ops_ext.atomic_load_64(addr)

elif IS_LINUX or IS_MACOS:
    # ===== Linux/macOS Implementation =====
    import ctypes.util

    # Try to use C++ extension first (provides most reliable atomic operations)
    try:
        import atomic_ops_ext
        _atomic_ops_ext = atomic_ops_ext
        _USE_EXTENSION = True
    except ImportError:
        # Fall back to native atomic operations
        _USE_EXTENSION = False

    # Try to load libatomic
    libatomic_path = ctypes.util.find_library('atomic')
    if libatomic_path:
        try:
            libatomic = ctypes.CDLL(libatomic_path)
            # Set up __atomic_compare_exchange_8 function signature
            try:
                libatomic.__atomic_compare_exchange_8.argtypes = [
                    ctypes.POINTER(ctypes.c_uint64),  # ptr
                    ctypes.POINTER(ctypes.c_uint64),  # expected
                    ctypes.c_uint64,                   # desired
                    ctypes.c_int,                      # success_memorder
                    ctypes.c_int                       # failure_memorder
                ]
                libatomic.__atomic_compare_exchange_8.restype = ctypes.c_bool
                _HAS_LIBATOMIC = True
            except AttributeError:
                _HAS_LIBATOMIC = False
        except OSError:
            _HAS_LIBATOMIC = False
    else:
        _HAS_LIBATOMIC = False

    # Try to load libc for basic atomic operations
    libc_name = 'c.so.6' if IS_LINUX else 'c'
    try:
        libc = ctypes.CDLL(ctypes.util.find_library(libc_name) or libc_name)

        # __sync_val_compare_and_swap for 64-bit CAS
        try:
            _sync_val_cas_8 = libc.__sync_val_compare_and_swap_8
            _sync_val_cas_8.argtypes = [
                ctypes.POINTER(ctypes.c_uint64),
                ctypes.c_uint64,
                ctypes.c_uint64
            ]
            _sync_val_cas_8.restype = ctypes.c_uint64
        except AttributeError:
            _sync_val_cas_8 = None

        # __sync_synchronize for memory barrier
        try:
            _sync_synchronize = libc.__sync_synchronize
            _sync_synchronize.restype = None
        except AttributeError:
            _sync_synchronize = None
    except OSError:
        libc = None
        _sync_val_cas_8 = None
        _sync_synchronize = None

    def _platform_cas_64(buffer: memoryview, offset: int,
                        expected: int, desired: int) -> Tuple[bool, int]:
        """Linux/macOS-specific 64-bit CAS using C++ extension, __sync_val_compare_and_swap, or libatomic."""
        # Get pointer to buffer location
        buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
        addr = ctypes.addressof(buf_array) + offset

        # Try C++ extension first (most reliable)
        if _USE_EXTENSION:
            return _atomic_ops_ext.atomic_cas_64(addr, expected, desired)

        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint64))

        # Try __sync_val_compare_and_swap (available on most systems)
        if _sync_val_cas_8 is not None:
            actual = _sync_val_cas_8(ptr, expected, desired)
            success = (actual == expected)
            return success, actual

        # Fallback to libatomic if available
        if _HAS_LIBATOMIC:
            try:
                # __atomic_compare_exchange_8(ptr, &expected, desired, success_order, failure_order)
                expected_ref = ctypes.c_uint64(expected)
                success = libatomic.__atomic_compare_exchange_8(
                    ptr,
                    ctypes.byref(expected_ref),
                    ctypes.c_uint64(desired),
                    ctypes.c_int(3),  # __ATOMIC_RELEASE
                    ctypes.c_int(0)   # __ATOMIC_RELAXED
                )
                return bool(success), expected_ref.value
            except AttributeError:
                pass

        raise RuntimeError("64-bit atomic CAS not available (neither __sync_val_compare_and_swap_8 nor libatomic found)")

    def _memory_fence_acquire():
        """Acquire memory fence."""
        if _sync_synchronize:
            _sync_synchronize()

    def _memory_fence_release():
        """Release memory fence."""
        if _sync_synchronize:
            _sync_synchronize()

    def _atomic_store_64(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 64-bit store with release semantics."""
        buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
        addr = ctypes.addressof(buf_array) + offset

        # Use C++ extension first
        if _USE_EXTENSION:
            _atomic_ops_ext.atomic_store_64(addr, value)
            return

        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint64))

        # For simplicity, use CAS as atomic store
        if _sync_val_cas_8:
            # Read current value and swap with new value
            while True:
                current = ptr.contents.value
                if _sync_val_cas_8(ptr, current, value) == current:
                    break
        else:
            _memory_fence_release()
            struct.pack_into("<Q", buffer, offset, value)

    def _atomic_load_64(buffer: memoryview, offset: int) -> int:
        """Atomic 64-bit load with acquire semantics."""
        # Use C++ extension first
        if _USE_EXTENSION:
            buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
            addr = ctypes.addressof(buf_array) + offset
            return _atomic_ops_ext.atomic_load_64(addr)

        # On x86-64, aligned 64-bit reads are atomic
        value = struct.unpack_from("<Q", buffer, offset)[0]
        _memory_fence_acquire()
        return value

else:
    # Unsupported platform
    _USE_EXTENSION = False

    def _platform_cas_64(*args, **kwargs):
        raise RuntimeError(f"64-bit atomic CAS not supported on {sys.platform}")

    def _memory_fence_acquire():
        pass

    def _memory_fence_release():
        pass

    def _atomic_store_64(*args, **kwargs):
        raise RuntimeError(f"64-bit atomic store not supported on {sys.platform}")

    def _atomic_load_64(*args, **kwargs):
        raise RuntimeError(f"64-bit atomic load not supported on {sys.platform}")


class AtomicReservedInfo:
    """
    Atomic operations on reserved_info (uint64_t with packed index/size).

    As of slick-queue v1.2.0, reserved_info is a packed uint64_t:
    - Bits 0-15: size (16-bit, max 65535)
    - Bits 16-63: index (48-bit, max 281 trillion)

    Memory layout:
    - Offset 0-7: std::atomic<uint64_t> (single 64-bit value)
    """

    # Single uint64_t at offset 0 (matches C++ std::atomic<uint64_t>)
    RESERVED_INFO_FMT = "Q"  # 8 bytes

    def __init__(self, buffer: memoryview, offset: int = 0):
        """
        Initialize atomic reserved_info wrapper.

        Args:
            buffer: Memory buffer (typically SharedMemory.buf)
            offset: Byte offset in buffer (typically 0 for header)
        """
        # Store a weak reference to avoid holding the buffer
        # This prevents "exported pointers exist" errors during cleanup
        self.buffer = buffer
        self.offset = offset

        # Verify platform support
        supported, msg = check_platform_support()
        if not supported:
            raise RuntimeError(f"Platform not supported for atomic operations: {msg}")

    def release(self):
        """Release buffer reference to allow proper cleanup."""
        self.buffer = None

    def load(self) -> Tuple[int, int]:
        """
        Load reserved_info with memory_order_relaxed.

        Returns:
            Tuple of (index: int, size: int)
        """
        # Use atomic load to avoid torn reads during concurrent updates
        packed = _atomic_load_64(self.buffer, self.offset)
        return get_index(packed), get_size(packed)

    def compare_exchange_weak(
        self,
        expected: Tuple[int, int],
        desired: Tuple[int, int]
    ) -> Tuple[bool, Tuple[int, int]]:
        """
        Atomic compare-and-swap with memory_order_release on success,
        memory_order_relaxed on failure (matching C++ queue.h:201).

        This implements the weak version (may spuriously fail) to match
        the C++ compare_exchange_weak semantics.

        Args:
            expected: Tuple of (expected_index, expected_size)
            desired: Tuple of (desired_index, desired_size)

        Returns:
            Tuple of (success: bool, actual: Tuple[int, int])
            If success is False, actual contains the current value.
        """
        expected_index, expected_size = expected
        desired_index, desired_size = desired

        # Pack to uint64_t (48-bit index in upper bits, 16-bit size in lower bits)
        expected_packed = make_reserved_info(expected_index, expected_size)
        desired_packed = make_reserved_info(desired_index, desired_size)

        # Perform platform-specific 64-bit CAS
        success, actual_packed = _platform_cas_64(
            self.buffer, self.offset,
            expected_packed, desired_packed
        )

        # Unpack actual value
        actual_index = get_index(actual_packed)
        actual_size = get_size(actual_packed)

        return success, (actual_index, actual_size)


class AtomicUInt64:
    """
    Atomic operations on 8-byte uint64_t value.

    This matches the C++ std::atomic<uint64_t> used for slot data_index
    in queue.h.
    """

    def __init__(self, buffer: memoryview, offset: int):
        """
        Initialize atomic uint64_t wrapper.

        Args:
            buffer: Memory buffer (typically SharedMemory.buf)
            offset: Byte offset in buffer
        """
        self.buffer = buffer
        self.offset = offset

    def release(self):
        """Release buffer reference to allow proper cleanup."""
        self.buffer = None

    def load_acquire(self) -> int:
        """
        Load with memory_order_acquire (matching C++ queue.h:256, 292).

        Acquire semantics ensure that subsequent loads/stores cannot be
        reordered before this load.

        Returns:
            uint64_t value
        """
        return _atomic_load_64(self.buffer, self.offset)

    def store_release(self, value: int) -> None:
        """
        Store with memory_order_release (matching C++ queue.h:211, 242).

        Release semantics ensure that prior loads/stores cannot be
        reordered after this store.

        Args:
            value: uint64_t value to store
        """
        _atomic_store_64(self.buffer, self.offset, value)

    def compare_exchange_weak(
        self,
        expected: int,
        desired: int
    ) -> Tuple[bool, int]:
        """
        Atomic compare-and-swap with memory_order_release on success,
        memory_order_relaxed on failure (matching C++ queue.h:306, 312).

        Args:
            expected: Expected uint64_t value
            desired: Desired uint64_t value

        Returns:
            Tuple of (success: bool, actual: int)
            If success is False, actual contains the current value.
        """
        success, actual = _platform_cas_64(self.buffer, self.offset, expected, desired)
        return success, actual


class AtomicCursor:
    """
    An atomic cursor for multi-consumer work-stealing patterns.

    This class wraps an atomic uint64_t that can be used for coordinating multiple
    consumers. Each consumer atomically claims items to process, ensuring each item
    is consumed exactly once.

    Matches C++ std::atomic<uint64_t>& parameter in queue.h:283.

    Supports two modes:
    - **Local mode**: Pass a bytearray for single-process multi-threaded usage
    - **Shared memory mode**: Pass SharedMemory.buf for multi-process usage

    Examples:
        # Local mode (multi-threading in single process)
        from atomic_ops import AtomicCursor

        cursor_buf = bytearray(8)
        cursor = AtomicCursor(cursor_buf, 0)
        cursor.store(0)
        # Multiple threads can share this cursor

        # Shared memory mode (multi-process)
        from multiprocessing.shared_memory import SharedMemory
        from atomic_ops import AtomicCursor

        cursor_shm = SharedMemory(name='cursor', create=True, size=8)
        cursor = AtomicCursor(cursor_shm.buf, 0)
        cursor.store(0)
        # Multiple processes can share this cursor
    """

    def __init__(self, buffer: Union[memoryview, bytearray], offset: int = 0):
        """
        Initialize atomic cursor wrapper.

        Args:
            buffer: Memory buffer (SharedMemory.buf for shared memory mode,
                   or bytearray for local mode)
            offset: Byte offset in buffer (default 0)
        """
        # Convert bytearray to memoryview for consistency
        if isinstance(buffer, bytearray):
            buffer = memoryview(buffer)
        self._atomic: Optional[AtomicUInt64] = AtomicUInt64(buffer, offset)

    def release(self):
        """Release buffer reference to allow proper cleanup."""
        if self._atomic:
            self._atomic.release()
        self._atomic = None

    def load(self) -> int:
        """
        Load cursor value with memory_order_acquire (matching C++ queue.h:285).

        Returns:
            Current cursor value
        """
        if self._atomic is None:
            raise RuntimeError("AtomicCursor has been released")
        return self._atomic.load_acquire()

    def store(self, value: int) -> None:
        """
        Store cursor value with memory_order_release (matching C++ queue.h:292).

        Args:
            value: New cursor value
        """
        if self._atomic is None:
            raise RuntimeError("AtomicCursor has been released")
        self._atomic.store_release(value)

    def compare_exchange_weak(self, expected: int, desired: int) -> Tuple[bool, int]:
        """
        Atomic compare-and-swap with memory_order_release on success,
        memory_order_relaxed on failure (matching C++ queue.h:302, 308).

        Args:
            expected: Expected cursor value
            desired: Desired cursor value

        Returns:
            Tuple of (success: bool, actual: int)
            If success is False, actual contains the current value.
        """
        if self._atomic is None:
            raise RuntimeError("AtomicCursor has been released")
        return self._atomic.compare_exchange_weak(expected, desired)


__all__ = [
    'AtomicReservedInfo',
    'AtomicUInt64',
    'AtomicCursor',
    'MemoryOrder',
    'check_platform_support',
    'make_reserved_info',
    'get_index',
    'get_size'
]
