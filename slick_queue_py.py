"""
Python implementation of SlickQueue-compatible shared memory queue.

This implements the same memory layout as the C++ `slick::SlickQueue<T>`
header (64 bytes), an array of `slot` structures starting at offset 64, and
the data array immediately after the slot array.

Multi-Producer Multi-Consumer Support:
- This implementation now uses atomic operations via the atomic_ops module
- On platforms with hardware 128-bit CAS support (x86-64 with CMPXCHG16B),
  provides true lock-free multi-producer and multi-consumer semantics
- On other platforms, falls back to lock-based synchronization

C++/Python Interoperability:
- Python processes can produce/consume to queues created by C++
- C++ processes can produce/consume to queues created by Python
- Memory layout and atomic operations match exactly

Feature Configuration (Traits):
- Optional features are selected through a ``traits`` constructor argument
  rather than being always-on, mirroring the C++ ``Traits`` template parameter
  added in slick-queue v2.0.0. See :class:`QueueTraits`.
- ``enable_read_last`` is part of the shared-memory contract: the creator records
  it in the segment's layout marker ('SLQ1' when the last published index is
  maintained, 'SLQ0' when it is not) and every attacher matches it against its
  own configuration, raising RuntimeError on a mismatch.

Supported on Python 3.8+ (uses multiprocessing.shared_memory).
"""
from __future__ import annotations

__version__ = '2.0.0'

import struct
import sys
import time
from typing import Optional, Tuple, Union
from atomic_ops import AtomicReservedInfo, AtomicUInt64, AtomicCursor, check_platform_support, make_reserved_info, get_index, get_size

# Use Python's built-in shared memory (available in Python 3.8+)
from multiprocessing.shared_memory import SharedMemory

# Layout constants
# Shared memory header layout (64 bytes total):
# Offset 0-7:   std::atomic<reserved_info> (8 bytes)
# Offset 8-11:  size_ (uint32_t)
# Offset 12-15: element_size (uint32_t)
# Offset 16-23: std::atomic<uint64_t> last_published_ (8 bytes)
# Offset 24-27: header_magic (uint32_t) - value 0x534C5131 ('SLQ1')
# Offset 28-47: PADDING (20 bytes)
# Offset 48-51: init_state (atomic uint32_t)
# Offset 52-63: PADDING (12 bytes)
HEADER_SIZE = 64
RESERVED_INFO_SIZE = struct.calcsize(AtomicReservedInfo.RESERVED_INFO_FMT)  # 8 bytes
SIZE_OFFSET = 8
ELEMENT_SIZE_OFFSET = 12
LAST_PUBLISHED_OFFSET = 16
HEADER_MAGIC_OFFSET = 24
# Layout marker: the bytes 'SLQ' followed by an ASCII digit whose low nibble carries
# the features the creator was built with. Only options that change the shared header
# protocol get a bit; the purely local ones (reset check, loss detection, cpu relax)
# cost nothing to mix on one segment and deliberately have none.
#   bit 0    - the last-published index at LAST_PUBLISHED_OFFSET is maintained
#   bits 1-3 - reserved for future shared-layout features, must be 0
HEADER_MAGIC = 0x534C5131  # 'SLQ1' in little-endian
HEADER_MAGIC_FEATURE_MASK = 0x0000000F  # feature nibble
HEADER_MAGIC_READ_LAST = 0x1           # feature bit 0
INIT_STATE_OFFSET = 48

# Init state constants (matches C++ queue.h)
INIT_STATE_UNINITIALIZED = 0
INIT_STATE_LEGACY = 1
INIT_STATE_INITIALIZING = 2
INIT_STATE_READY = 3

# Invalid index constant
K_INVALID_INDEX = 2**64 - 1

# slot: atomic_uint64 data_index; uint32 size; 4 bytes padding => 16 bytes
#
# slot.size became a std::atomic<uint32_t> on the C++ side in v2.0.0. It is
# deliberately NOT wrapped in an atomic here, and stays a plain struct field:
#
#  - The two hazards that change fixed are C++-specific. A non-atomic field written
#    and read concurrently is undefined behaviour in C++, and the optimiser was free
#    to sink or duplicate the plain load outside the seqlock bracket. CPython has no
#    UB for buffer access and no optimiser that can move a struct.unpack_from call:
#    it reads the bytes that are there, once, at that bytecode.
#  - The access is already atomic in hardware terms. The field sits at slot+8 in a
#    16-byte slot after a 64-byte header, so it is always 8-byte aligned in both
#    local and shared memory, and struct reads it as one aligned 4-byte load.
#  - The wire format is identical either way - std::atomic<uint32_t> has the same
#    size, alignment and representation as uint32_t - so C++ interop is unaffected.
#  - Routing it through the atomic extension would cost ~5x per read on the hottest
#    path in this module, and would need new 32-bit ops in atomic_ops_ext plus
#    rebuilt binaries for every shipped platform.
#
# What did matter in v2.0.0 is the bracket, not the atomic, and that is ported: load
# size once, then re-validate data_index before using it. The residual gap both
# implementations share - the producer writes size before it advances data_index, so
# a reader can in principle catch a newer size under an older index - is the one the
# C++ header calls out as theoretical and not closable by making the field atomic.
SLOT_FMT = "<Q I 4x"
SLOT_SIZE = struct.calcsize(SLOT_FMT)
SLOT_SIZE_OFFSET = 8  # offset of slot.size within a slot


class QueueTraits:
    """Feature configuration for :class:`SlickQueue`.

    Mirrors C++ ``slick::queue_traits`` (slick-queue v2.0.0). Subclass and override
    only what you need::

        class MyTraits(QueueTraits):
            enable_reset_check = True   # opt in
            enable_read_last = False    # opt out

        q = SlickQueue(size=1024, element_size=8, traits=MyTraits)

    Because the configuration is per-instance, two differently configured queues can
    coexist in one process.
    """

    #: Enable read_last(). Tracks the last published index.
    #: Cost: one CAS per publish().
    enable_read_last = True

    #: Detect a concurrent reset() during read() and rewind the cursor once.
    #: Cost: one load of the producer's reservation counter per read().
    enable_reset_check = False

    #: Per-instance counter of items skipped when a producer overruns a reader,
    #: reported by loss_count().
    #:
    #: On, unlike the C++ Release default. C++ pays a cacheline and an atomic
    #: fetch_add for this; here it measures at +0.2% on a reader that keeps up, and
    #: loss_count() is the only signal a caller has that it was overrun - which also
    #: makes it the only way to know the bytes read() returned may have been
    #: overwritten in flight (see read()). Turn it off for maximum throughput.
    enable_loss_detection = True

    #: Yield-based backoff on contended CAS loops.
    enable_cpu_relax = True


# C++ needs two traits *types* here, selected by NDEBUG, so that a debug/release
# mismatch across translation units differs in the template-id and fails to link
# rather than silently violating the ODR. Python has no translation units, no ODR
# and no linker, so there is nothing for a second type to protect - and no
# debug/release build to key it off either. __debug__ is not a substitute: it is
# about stripping asserts, and keying the counter to it would make loss_count()
# silently return 0 under `python -O`, hiding exactly the condition it exists to
# report. One traits type, one default.
default_queue_traits = QueueTraits

_TRAIT_NAMES = (
    "enable_read_last",
    "enable_reset_check",
    "enable_loss_detection",
    "enable_cpu_relax",
)


class _FrozenTraits:
    """An immutable snapshot of a traits type, taken at queue construction.

    A traits type is an ordinary class, so its attributes stay writable after a queue
    has been built from it. The queue reads its configuration on every publish() and
    read(), but commits to it exactly once - the shared-memory layout marker is
    written from it, and the optional atomics are created from it - so letting a
    later mutation through meant the two could disagree: flipping enable_read_last
    off froze read_last() for every peer of an 'SLQ1' segment, and flipping it on
    raised AttributeError from publish() and reset() because the last-published
    atomic had never been created.

    Snapshotting removes the whole class of bug rather than guarding each site.
    """

    __slots__ = _TRAIT_NAMES

    def __init__(self, traits):
        # object.__setattr__ to get past this class's own read-only __setattr__.
        # The values are already known to be bools: validate_traits() runs first.
        for name in _TRAIT_NAMES:
            object.__setattr__(self, name, getattr(traits, name))

    def __setattr__(self, name, value):
        raise AttributeError(
            f"traits are read-only after construction; build a new queue to change "
            f"{name!r}"
        )

    def __delattr__(self, name):
        raise AttributeError("traits are read-only after construction")

    def __repr__(self):
        inner = ", ".join(f"{n}={getattr(self, n)}" for n in _TRAIT_NAMES)
        return f"traits({inner})"

    def __eq__(self, other):
        if not isinstance(other, _FrozenTraits):
            return NotImplemented
        return all(getattr(self, n) == getattr(other, n) for n in _TRAIT_NAMES)

    def __hash__(self):
        return hash(tuple(getattr(self, n) for n in _TRAIT_NAMES))


def validate_traits(traits) -> None:
    """Reject a malformed traits type, as the C++ ``queue_traits_type`` concept does.

    Requiring exactly bool (not merely something truthy) rejects a wrong type at the
    point of use with a readable error. Note this cannot catch a misspelled override
    in a subclass: the inherited attribute stays visible and silently keeps the base
    value.

    Raises:
        TypeError: If a trait is missing or is not a bool.
    """
    for name in _TRAIT_NAMES:
        if not hasattr(traits, name):
            raise TypeError(
                f"traits type {getattr(traits, '__name__', traits)!r} is missing "
                f"required attribute '{name}'; derive from QueueTraits"
            )
        value = getattr(traits, name)
        if not isinstance(value, bool):
            raise TypeError(
                f"traits attribute '{name}' must be a bool, got "
                f"{type(value).__name__}"
            )


class SlickQueue:
    """A fixed-size ring queue compatible with C++ SlickQueue.

    Supports two modes:
    - **Shared memory mode** (when name is provided): Uses shared memory for inter-process communication
    - **Local memory mode** (when name is None): Uses local memory (single process)

    Elements are fixed-length byte blobs of `element_size`.

    Args:
        name: Shared memory segment name. If None, uses local memory mode.
        size: Queue capacity (must be power of 2). Required when creating or using local mode.
        element_size: Size of each element in bytes. Required.
        create: If True, create new shared memory segment (only for shared memory mode).
        traits: Feature configuration, a :class:`QueueTraits` subclass. Defaults to
            ``default_queue_traits``. ``enable_read_last`` must match across every
            peer of a shared memory segment; the others are local to this instance.
    """

    def __init__(self, *, name: Optional[str] = None, size: Optional[int] = None,
                 element_size: Optional[int] = None, traits=None):
        # Traits first - the rest of construction, including the shared memory
        # layout marker, depends on the configuration.
        if traits is None:
            traits = default_queue_traits
        validate_traits(traits)
        # Snapshot, do not retain the caller's class. A traits type stays writable,
        # and this queue commits to the configuration exactly once: it writes the
        # layout marker from it and creates the optional atomics from it. Reading a
        # live class on every publish()/read() let the two disagree - see
        # _FrozenTraits. self.traits therefore always describes what this queue
        # actually does.
        self.traits = _FrozenTraits(traits)
        # Unpacked onto the instance as well: these are read on every publish() and
        # read(), and one attribute lookup beats two on the hot path.
        self._enable_read_last = self.traits.enable_read_last
        self._enable_reset_check = self.traits.enable_reset_check
        self._enable_loss_detection = self.traits.enable_loss_detection
        self._enable_cpu_relax = self.traits.enable_cpu_relax
        # The marker this queue writes as a creator and demands as an attacher.
        self._header_magic_expected = (
            HEADER_MAGIC if self._enable_read_last
            else HEADER_MAGIC & ~HEADER_MAGIC_READ_LAST
        )

        # Store the original user-provided name (without / prefix)
        # Python's SharedMemory will add the / prefix on POSIX systems automatically.
        # We strip any leading / to avoid double-prefixing (//name) on POSIX systems.
        self.name = name
        if self.name is not None and self.name.startswith('/'):
            # Strip leading / if user provided it - Python's SharedMemory will add it back on POSIX
            self.name = self.name[1:]

        # macOS has a 31-character limit for POSIX shared memory names (including leading /)
        # Check the length that will be used (with / prefix on POSIX systems)
        if self.name is not None and sys.platform == 'darwin':
            # On macOS, Python's SharedMemory will prepend /, so check total length
            final_name = '/' + self.name
            if len(final_name) > 31:
                raise ValueError(f"Shared memory name '{final_name}' is {len(final_name)} characters, "
                               f"but macOS has a 31-character limit. Please use a shorter name.")

        self.use_shm = name is not None
        self._shm: Optional[SharedMemory] = None
        self._local_buf: Optional[bytearray] = None
        self.size = None
        self._own = False
        self._atomic_last_published = None
        # per-instance loss counter (matches C++ queue.h:98 loss_count_; local, not shared)
        self._loss_count = 0

        # Validate parameters
        if size is not None:
            self.size = int(size)
            if self.size & (self.size - 1):
                raise ValueError("size must be a power of two")
            self.mask = self.size - 1

        if element_size is not None:
            self.element_size = int(element_size)

        if self.use_shm:
            # Shared memory mode (C++ with shm_name != nullptr)
            if self.size:
                # create shared memory
                if element_size is None:
                    raise ValueError("size and element_size required when creating")
                total = HEADER_SIZE + SLOT_SIZE * self.size + self.element_size * self.size
                try:
                    self._shm = SharedMemory(name=self.name, create=True, size=total)
                    # print(f"**** create new shm {self.name}")
                except FileExistsError:
                    # print(f"**** shm already exists, opening {self.name}")
                    self._shm = SharedMemory(name=self.name, create=False)

                # Use CAS on init_state to determine ownership (matches C++ queue.h:618-648)
                buf = self._shm.buf
                init_state_atomic = AtomicUInt64(buf, INIT_STATE_OFFSET)

                # Try to atomically claim ownership by CAS from UNINITIALIZED to INITIALIZING
                success, actual_state = init_state_atomic.compare_exchange_weak(
                    INIT_STATE_UNINITIALIZED, INIT_STATE_INITIALIZING
                )

                if success:
                    # We are the creator - initialize the queue (matches C++ queue.h:622-647)
                    self._own = True

                    # Publish this queue's feature nibble in the marker at offset 24.
                    # It is what every attacher matches itself against, so a peer that
                    # maintains the last-published index and one that does not can no
                    # longer end up sharing a segment in either direction. Stored
                    # unconditionally so a recycled segment cannot leave a stale marker.
                    struct.pack_into("<I", buf, HEADER_MAGIC_OFFSET, self._header_magic_expected)

                    # Initialize reserved_info atomic at offset 0
                    atomic_reserved = AtomicReservedInfo(buf, 0)
                    # This stores packed (index=0, size=0)
                    struct.pack_into("<Q", buf, 0, 0)

                    # Initialize last_published at offset 16 with kInvalidIndex.
                    # Unconditional, matching the C++ creator: the word is part of the
                    # header whether or not this peer maintains it.
                    struct.pack_into("<Q", buf, LAST_PUBLISHED_OFFSET, K_INVALID_INDEX)

                    # Write size and element_size at offsets 8 and 12
                    struct.pack_into("<I I", buf, SIZE_OFFSET, self.size, element_size)

                    # Initialize slots data_index to max (uint64 max)
                    for i in range(self.size):
                        off = HEADER_SIZE + i * SLOT_SIZE
                        struct.pack_into(SLOT_FMT, buf, off, K_INVALID_INDEX, 1)

                    # Mark initialization complete
                    init_state_atomic.store_release(INIT_STATE_READY)

                else:
                    # Opened existing - wait for initialization and validate (matches C++ queue.h:649-684)
                    self._own = False

                    # Wait for initialization to complete
                    if not self._wait_for_shared_memory_ready(buf):
                        self._shm.close()
                        raise RuntimeError("Timed out waiting for shared memory initialization")

                    # Reject a segment whose creator disagrees about the header protocol
                    try:
                        self._validate_header_magic(buf)
                    except Exception:
                        self._shm.close()
                        raise

                    # Read and validate metadata
                    ss = struct.unpack_from("<I I", buf, SIZE_OFFSET)
                    if ss[0] != self.size:
                        self._shm.close()
                        raise ValueError(f"size mismatch. Expected {self.size} but got {ss[0]}")
                    if ss[1] != element_size:
                        self._shm.close()
                        raise ValueError(f"element size mismatch. Expected {element_size} but got {ss[1]}")
            else:
                # print(f"**** open existing shm {self.name}")
                # open existing and read size from header
                if element_size is None:
                    raise ValueError("element_size must be provided when opening existing shared memory")

                # Open existing shared memory (size parameter not needed/ignored)
                self._shm = SharedMemory(name=self.name, create=False)
                buf = self._shm.buf

                # Wait for initialization to complete (matches C++ queue.h:558-562)
                if not self._wait_for_shared_memory_ready(buf):
                    self._shm.close()
                    raise RuntimeError("Timed out waiting for shared memory initialization")

                # Reject a segment whose creator disagrees about the header protocol
                try:
                    self._validate_header_magic(buf)
                except Exception:
                    self._shm.close()
                    raise

                # Read actual queue size from header
                ss = struct.unpack_from("<I I", buf, SIZE_OFFSET)
                self.size = ss[0]
                elem_sz = ss[1]

                if element_size != elem_sz:
                    self._shm.close()
                    raise ValueError(f"SharedMemory element_size mismatch. Expecting {element_size} but got {elem_sz}")

                self.mask = self.size - 1
                self.element_size = int(element_size)

            self._buf = self._shm.buf
            self._control_offset = HEADER_SIZE
            self._data_offset = HEADER_SIZE + SLOT_SIZE * self.size

            # Initialize atomic wrappers for lock-free operations
            self._atomic_reserved = AtomicReservedInfo(self._buf, 0)
            self._atomic_slots = []
            for i in range(self.size):
                slot_offset = HEADER_SIZE + i * SLOT_SIZE
                self._atomic_slots.append(AtomicUInt64(self._buf, slot_offset))

            # Initialize last_published atomic only when this queue maintains it
            if self._enable_read_last:
                self._atomic_last_published = AtomicUInt64(self._buf, LAST_PUBLISHED_OFFSET)
        else:
            # Local memory mode (C++ with shm_name == nullptr)
            if size is None or element_size is None:
                raise ValueError("size and element_size required for local memory mode")

            # Create local buffers (equivalent to C++ new T[size_] and new slot[size_])
            # We use a bytearray to simulate the memory layout
            total = HEADER_SIZE + SLOT_SIZE * self.size + self.element_size * self.size
            self._local_buf = bytearray(total)

            # Initialize header with modern format (local mode always uses modern format)
            self._local_buf[:HEADER_SIZE] = bytes(HEADER_SIZE)
            # Write size at offset 8
            struct.pack_into("<I I", self._local_buf, SIZE_OFFSET, self.size, element_size)
            # Initialize last_published at offset 16 with kInvalidIndex
            struct.pack_into("<Q", self._local_buf, LAST_PUBLISHED_OFFSET, K_INVALID_INDEX)
            # Write header_magic at offset 24
            struct.pack_into("<I", self._local_buf, HEADER_MAGIC_OFFSET, self._header_magic_expected)
            # Write init_state = READY at offset 48
            struct.pack_into("<I", self._local_buf, INIT_STATE_OFFSET, INIT_STATE_READY)

            # Initialize slots data_index to max
            for i in range(self.size):
                off = HEADER_SIZE + i * SLOT_SIZE
                struct.pack_into(SLOT_FMT, self._local_buf, off, K_INVALID_INDEX, 1)

            # Create a memoryview for consistency with shared memory path
            self._buf = memoryview(self._local_buf)
            self._control_offset = HEADER_SIZE
            self._data_offset = HEADER_SIZE + SLOT_SIZE * self.size

            # Initialize atomic wrappers (these work on local memory too)
            self._atomic_reserved = AtomicReservedInfo(self._buf, 0)
            self._atomic_slots = []
            for i in range(self.size):
                slot_offset = HEADER_SIZE + i * SLOT_SIZE
                self._atomic_slots.append(AtomicUInt64(self._buf, slot_offset))

            # Initialize last_published atomic only when this queue maintains it
            if self._enable_read_last:
                self._atomic_last_published = AtomicUInt64(self._buf, LAST_PUBLISHED_OFFSET)

    @staticmethod
    def _wait_for_shared_memory_ready(buf: memoryview) -> bool:
        """
        Wait for shared memory initialization to complete.
        Matches C++ queue.h:510-534.

        Args:
            buf: Memory buffer to check

        Returns:
            True if initialization completed successfully, False if timed out
        """
        init_state_atomic = AtomicUInt64(buf, INIT_STATE_OFFSET)
        max_wait_ms = 2000
        legacy_grace_ms = 5

        for i in range(max_wait_ms):
            state = init_state_atomic.load_acquire()
            if state == INIT_STATE_READY:
                return True

            if state == INIT_STATE_LEGACY and i >= legacy_grace_ms:
                # Legacy format: check if size and element_size are non-zero
                ss = struct.unpack_from("<I I", buf, SIZE_OFFSET)
                if ss[0] != 0 and ss[1] != 0:
                    return True

            time.sleep(0.001)

        return False

    @staticmethod
    def _to_hex(value: int) -> str:
        """Format a marker word the way the C++ error messages do."""
        return "0x%08X" % (value & 0xFFFFFFFF)

    def _validate_header_magic(self, buf: memoryview) -> None:
        """
        Reject an existing segment whose creator disagrees about the header protocol.

        Matches C++ queue.hpp validate_header_magic(). Traits are a property of each
        peer and never reach the segment, so the feature nibble in the marker is the
        only place a disagreement can be caught - and attach time is the only moment
        at which it is still cheap. Both directions are fatal: a queue that expects
        the last-published index would read a counter nobody writes, and one that does
        not maintain it would silently freeze read_last() for every peer that does.

        The init_state handshake has already been awaited with acquire semantics by
        the caller, which orders this plain load of the marker behind the creator's
        initialization.

        Args:
            buf: Memory buffer to check

        Raises:
            RuntimeError: If the segment carries no marker (created before v1.4.0),
                was created with unknown layout features, or disagrees about
                enable_read_last.
        """
        expected = self._header_magic_expected
        magic = struct.unpack_from("<I", buf, HEADER_MAGIC_OFFSET)[0]
        if magic == expected:
            return

        if (magic & ~HEADER_MAGIC_FEATURE_MASK) != (HEADER_MAGIC & ~HEADER_MAGIC_FEATURE_MASK):
            raise RuntimeError(
                "Shared memory does not carry a slick-queue layout marker. Expected "
                f"{self._to_hex(expected)} but got {self._to_hex(magic)}; segments "
                "created before v1.4.0 carry no marker and are not supported"
            )

        if (magic & HEADER_MAGIC_FEATURE_MASK & ~HEADER_MAGIC_READ_LAST) != 0:
            raise RuntimeError(
                "Shared memory was created with unknown layout features. Marker "
                f"{self._to_hex(magic)} is newer than this build understands "
                f"({self._to_hex(expected)})"
            )

        raise RuntimeError(
            "Shared memory feature mismatch: the segment was created with "
            f"enable_read_last={bool(magic & HEADER_MAGIC_READ_LAST)} but this queue "
            f"has enable_read_last={self._enable_read_last}"
        )

    def _cpu_relax(self) -> None:
        """
        Backoff hint on a contended CAS loop (C++ cpu_relax()).

        There is no pause intrinsic reachable from pure Python; releasing the GIL so
        another thread can make progress is the closest equivalent, and is what the
        non-x86 C++ fallback (std::this_thread::yield) does.
        """
        if self._enable_cpu_relax:
            time.sleep(0)

    def _was_reset(self, read_index: int) -> bool:
        """
        Detect that a concurrent reset() rewound the reservation counter below the
        absolute index this cursor is asking for.

        Matches C++ queue.hpp was_reset(). Shared by both read paths so the two
        recovery sites cannot drift apart.

        The test is on the *reader's cursor*, not on the slot it happens to land on.
        A cursor beyond the reservation counter is asking for an index that was never
        reserved, which can only happen if the counter was rewound by reset().

        Testing the slot's index instead would miss every case that matters: reset()
        clears the control array before rewinding the counter, so the slot a stale
        reader lands on holds either K_INVALID_INDEX or a fresh low index - neither of
        which is ahead of the counter, leaving the reader stuck returning None and
        eventually skipping the start of the new generation.

        No false positives: read_index is only ever advanced to index + slot.size, and
        reserve(n) sets the counter to index + n, so read_index <= the counter always
        holds in normal operation, with equality when the reader is caught up.
        """
        return read_index > self._atomic_reserved.load()[0]

    # low-level helpers
    def _read_reserved(self) -> Tuple[int, int]:
        buf = self._buf
        packed = struct.unpack_from(AtomicReservedInfo.RESERVED_INFO_FMT, buf, 0)[0]
        return get_index(packed), get_size(packed)

    def _write_reserved(self, index: int, sz: int) -> None:
        packed = make_reserved_info(int(index), int(sz))
        struct.pack_into(AtomicReservedInfo.RESERVED_INFO_FMT, self._buf, 0, packed)

    def _read_slot(self, idx: int) -> Tuple[int, int]:
        off = self._control_offset + idx * SLOT_SIZE
        data_index, size = struct.unpack_from(SLOT_FMT, self._buf, off)
        return int(data_index), int(size)

    def _write_slot(self, idx: int, data_index: int, size: int) -> None:
        off = self._control_offset + idx * SLOT_SIZE
        struct.pack_into(SLOT_FMT, self._buf, off, int(data_index), int(size))

    def get_shm_name(self) -> Optional[str]:
        """
        Get the actual shared memory name for C++ interop.

        Returns the name with POSIX / prefix (required by C++ shm_open).
        On POSIX systems (Linux/macOS), this returns the name with the / prefix.
        On Windows, it returns the name without modification.

        Returns:
            The shared memory name that C++ code should use to open the queue.
            On POSIX systems, this will have the / prefix that shm_open() requires.
        """
        if self._shm is not None:
            # Use the actual name from SharedMemory (which has / prefix on POSIX)
            return self._shm._name
        elif self.name is not None:
            # If SharedMemory not created yet, construct the expected name
            # On POSIX, need to add / prefix; on Windows, use as-is
            if sys.platform != 'win32':
                return '/' + self.name
            else:
                return self.name
        return None

    # Public API mirroring C++ methods
    def reserve(self, n: int = 1) -> int:
        """
        Reserve space in the queue for writing (multi-producer safe).

        Uses atomic CAS to safely reserve slots from multiple producers.
        Matches C++ queue.h:181-213.

        Args:
            n: Number of slots to reserve (default 1)

        Returns:
            Starting index of reserved space

        Raises:
            ValueError: If n is 0
            RuntimeError: If n > queue size
        """
        if n == 0:
            raise ValueError("required size must be > 0")
        if n > self.size:
            raise RuntimeError(f"required size {n} > queue size {self.size}")

        # CAS loop for multi-producer safety (matching C++ line 189-205)
        while True:
            # Load current reserved_info with memory_order_relaxed (C++ line 185)
            reserved_index, reserved_size = self._atomic_reserved.load()

            index = reserved_index
            idx = index & self.mask
            buffer_wrapped = False

            # Check if we need to wrap (C++ lines 194-204)
            if (idx + n) > self.size:
                # Wrap to beginning
                index += self.size - idx
                next_index = index + n
                next_size = n
                buffer_wrapped = True
            else:
                # Normal increment
                next_index = reserved_index + n
                next_size = n

            # Atomic CAS with memory_order_release on success (C++ line 205)
            success, actual = self._atomic_reserved.compare_exchange_weak(
                expected=(reserved_index, reserved_size),
                desired=(next_index, next_size)
            )

            if success:
                # CAS succeeded, we own this reservation
                if buffer_wrapped:
                    # Publish wrap marker (C++ lines 206-211)
                    slot_idx = reserved_index & self.mask
                    self._write_slot(slot_idx, index, n)
                return index

            # CAS failed, retry with updated value
            self._cpu_relax()

    def publish(self, index: int, n: int = 1) -> None:
        """
        Publish data written to reserved space (atomic with release semantics).

        Makes the data visible to consumers. Matches C++ queue.h:325-338.

        Args:
            index: Index returned by reserve()
            n: Number of slots to publish (default 1)
        """
        slot_idx = index & self.mask

        # Write slot size. Relaxed: the release store below is what publishes it, and
        # a reader that acquires data_index therefore sees this size.
        size_offset = self._control_offset + slot_idx * SLOT_SIZE + SLOT_SIZE_OFFSET
        struct.pack_into("<I 4x", self._buf, size_offset, n)

        # Atomic store of data_index with memory_order_release
        # This ensures all data writes are visible before the index is published
        self._atomic_slots[slot_idx].store_release(index)

        # Update the last published index. Unconditional when the feature is on: a
        # shared segment whose creator does not maintain this index is rejected at
        # attach time, so reaching here means every peer maintains it.
        if self._enable_read_last:
            current = self._atomic_last_published.load_acquire()
            while current == K_INVALID_INDEX or current < index:
                success, current = self._atomic_last_published.compare_exchange_weak(
                    current, index
                )
                if success:
                    break

    def __getitem__(self, index: int) -> memoryview:
        off = self._data_offset + (index & self.mask) * self.element_size
        return self._buf[off: off + self.element_size]

    def read(self, read_index: Union[int, AtomicCursor]) -> Union[Tuple[Optional[bytes], int, int], Tuple[Optional[bytes], int, int]]:
        """
        Read data from the queue.

        Lossy: the returned (data, size) pair and the cursor always describe one and
        the same record, but the *bytes* are only trustworthy while the reader keeps
        up. A producer writes an element's data before it publishes the slot's new
        index, so a reader that has been lapped can copy bytes the producer is midway
        through overwriting - and nothing in the slot can detect it, because the index
        has not changed yet. C++ has the identical hazard and hands back a pointer
        whose target is the producer's to overwrite; this implementation copies, so
        the copy can contain a newer record's bytes under the older record's cursor.
        Size the queue so consumers keep up, and use loss_count() to detect when they
        have not - a non-zero count means bytes may have been overwritten in flight.

        This method has two modes:
        1. Single-consumer mode: read(int) -> (data, size, new_index)
        2. Multi-consumer mode: read(AtomicCursor) -> (data, size)

        Single-consumer mode (matches C++ queue.h:246-273):
            Uses a plain int cursor for single-consumer scenarios.
            Returns the new read_index.

        Multi-consumer mode (matches C++ queue.h:283-314):
            Uses an AtomicCursor for work-stealing/load-balancing across multiple consumers.
            Each consumer atomically claims items, ensuring each item is consumed exactly once.

        Note: Unlike C++, the single-consumer version returns the new read_index rather
        than updating by reference, as Python doesn't have true pass-by-reference.

        Args:
            read_index: Either an int (single-consumer) or AtomicCursor (multi-consumer)

        Returns:
            Single-consumer: Tuple of (data_bytes or None, item_size, new_read_index)
            Multi-consumer: Tuple of (data_bytes or None, item_size)
            If no data available returns (None, 0) or (None, 0, read_index)

        Examples:
            # Single consumer
            read_index = 0
            data, size, read_index = q.read(read_index)

            # Multi-consumer work-stealing
            cursor = AtomicCursor(cursor_shm.buf, 0)
            data, size, index = q.read(cursor)  # Atomically claim next item
        """
        if isinstance(read_index, AtomicCursor):
            return self._read_atomic_cursor(read_index)
        else:
            return self._read_single_consumer(read_index)

    def _read_single_consumer(self, read_index: int) -> Tuple[Optional[bytes], int, int]:
        """
        Single-consumer read with atomic acquire semantics.

        Matches C++ queue.h:246-273. For single-consumer use only.

        Args:
            read_index: Current read position

        Returns:
            Tuple of (data_bytes or None, item_size, new_read_index).
            If no data available returns (None, 0, read_index).
        """
        while True:
            if self._enable_reset_check and self._was_reset(read_index):
                # The queue was reset out from under this cursor; restart from the
                # beginning of the new generation. Terminates after one retry:
                # _was_reset(0) tests 0 > counter, which is never true.
                read_index = 0
                continue

            idx = read_index & self.mask
            slot = self._atomic_slots[idx]

            # Atomic load with memory_order_acquire
            data_index = slot.load_acquire()

            # Recorded, not yet applied - the read below can still be sent round the
            # loop by a recycled slot, and this cursor is not advanced when that
            # happens, so charging the counter here would count the same overrun
            # again on the next pass. The shared-cursor path defers it for the same
            # reason.
            overrun = 0
            if self._enable_loss_detection:
                # The slot holds a newer generation at the same position - the items
                # in between were overwritten before this consumer read them
                if data_index != K_INVALID_INDEX and data_index > read_index and ((data_index & self.mask) == idx):
                    overrun = data_index - read_index

            # Check if data is ready
            if data_index == K_INVALID_INDEX or data_index < read_index:
                return None, 0, read_index

            # Check for wrap - skip the unused slots
            if data_index > read_index and ((data_index & self.mask) != idx):
                read_index = data_index
                continue

            # One load, then re-validate the slot (seqlock-style, as read_last does).
            # A wrapping producer writes size before it advances data_index, so
            # reading the field twice - once to advance the cursor and once to return
            # - could take the two values from different generations and leave the
            # cursor describing a different record than the caller was handed. Retry
            # rather than report "no data": the next pass sees the new index and
            # either reads it or skips the lap, and the producer must complete another
            # whole lap to trigger this again, so it cannot spin.
            size_offset = self._control_offset + idx * SLOT_SIZE + SLOT_SIZE_OFFSET
            slot_size = struct.unpack_from("<I", self._buf, size_offset)[0]
            if slot.load_acquire() != data_index:
                continue

            if overrun:
                self._loss_count += overrun

            # data_index and read_index select the same slot here: they are either
            # equal, or data_index ran ahead within this same slot, which the branch
            # above required.
            data_off = self._data_offset + idx * self.element_size
            data = bytes(self._buf[data_off: data_off + slot_size * self.element_size])
            new_read_index = data_index + slot_size
            return data, slot_size, new_read_index

    def _read_atomic_cursor(self, read_index: AtomicCursor) -> Tuple[Optional[bytes], int, int]:
        """
        Multi-consumer read using a shared atomic cursor (work-stealing pattern).

        Matches C++ queue.h:283-314. Multiple consumers share a single atomic cursor,
        atomically claiming items to process. Each item is consumed by exactly one consumer.

        Args:
            read_index: Shared AtomicCursor for coordinating multiple consumers

        Returns:
            Tuple of (data_bytes or None, item_size, data_index).
            If no data available returns (None, 0, -1).
        """
        if self._buf is None:
            raise RuntimeError("Queue buffer is not initialized")

        while True:
            # Load current cursor position
            current_index = read_index.load()

            if self._enable_reset_check and self._was_reset(current_index):
                # The queue was reset out from under this cursor. Rewind through a
                # CAS so a peer consumer that already moved it on is not clobbered;
                # either way the next iteration observes a cursor at or below the
                # reservation counter, so this cannot spin.
                read_index.compare_exchange_weak(current_index, 0)
                continue

            idx = current_index & self.mask
            slot = self._atomic_slots[idx]

            # Load slot data_index
            data_index = slot.load_acquire()

            # Check if data is ready
            if data_index == K_INVALID_INDEX or data_index < current_index:
                return None, 0, -1

            # The slot holds a newer generation at the same position; recorded here
            # but only charged once this consumer actually claims the item below.
            overrun = 0
            if self._enable_loss_detection:
                if data_index > current_index and ((data_index & self.mask) == idx):
                    overrun = data_index - current_index

            # Check for wrap - skip the unused slots
            if data_index > current_index and ((data_index & self.mask) != idx):
                # Try to atomically update cursor to skip wrapped slots
                read_index.compare_exchange_weak(current_index, data_index)
                continue

            # One load, validated before the claim, so the cursor advances by exactly
            # the size handed back to the caller. Validating after the CAS instead
            # would be wrong: the claim would already have been published to the other
            # consumers, and refusing to return the item would drop it for all of them.
            size_offset = self._control_offset + idx * SLOT_SIZE + SLOT_SIZE_OFFSET
            slot_size = struct.unpack_from("<I", self._buf, size_offset)[0]
            if slot.load_acquire() != data_index:
                continue

            # Try to atomically claim this item
            next_index = data_index + slot_size
            success, _ = read_index.compare_exchange_weak(current_index, next_index)

            if success:
                # Successfully claimed the item, read and return it
                # (loss attributed to the claiming consumer)
                if overrun:
                    self._loss_count += overrun
                data_off = self._data_offset + (current_index & self.mask) * self.element_size
                data = bytes(self._buf[data_off: data_off + slot_size * self.element_size])
                return data, slot_size, current_index

            # CAS failed, another consumer claimed it, retry
            self._cpu_relax()

    def loss_count(self) -> int:
        """
        Get the number of items skipped due to overwrite observed by this
        queue instance.

        The counter is per-instance (not shared through the segment) and is only
        maintained when ``traits.enable_loss_detection`` is set, which is the
        default.

        Returns:
            Count of skipped items observed by this instance, or 0 when
            traits.enable_loss_detection is False.
        """
        if self._enable_loss_detection:
            return self._loss_count
        return 0

    def initial_reading_index(self) -> int:
        """
        Get the initial reading index for a late-joining consumer: 0 if the
        queue is newly created, or the current writing index if opened existing.

        Matches C++ queue.h:242-244 (added in slick-queue v1.5.0).

        Returns:
            Initial reading index.
        """
        return self._atomic_reserved.load()[0]

    def read_last(self) -> Tuple[Optional[bytes], int]:
        """
        Read the last published data in the queue.

        Requires ``traits.enable_read_last``. There is no fallback: without the
        feature nothing maintains the last published index, and the reserved cursor
        is not a substitute because it reports reservations that were never published
        and truncates sizes above 65535.

        Returns:
            Tuple of (data_bytes or None, item_size).
            If no data available returns (None, 0).

        Raises:
            RuntimeError: If traits.enable_read_last is False.
        """
        if not self._enable_read_last:
            raise RuntimeError(
                "read_last() requires traits.enable_read_last. There is no fallback: "
                "without the feature nothing maintains the last published index, and "
                "the reserved cursor is not a substitute because it reports "
                "reservations that were never published and truncates sizes above 65535."
            )

        last_index = self._atomic_last_published.load_acquire()
        if last_index == K_INVALID_INDEX:
            return None, 0

        slot_idx = last_index & self.mask
        slot = self._atomic_slots[slot_idx]

        # The slot may already have been recycled by a wrapping producer, and publish()
        # writes slot.size before it advances last_published. Validate the slot before
        # and after reading size (seqlock-style) so the returned (data, size) pair
        # always describes one and the same record.
        if slot.load_acquire() != last_index:
            return None, 0

        size_offset = self._control_offset + slot_idx * SLOT_SIZE + SLOT_SIZE_OFFSET
        slot_size = struct.unpack_from("<I", self._buf, size_offset)[0]

        if slot.load_acquire() != last_index:
            return None, 0

        data_off = self._data_offset + slot_idx * self.element_size
        data = bytes(self._buf[data_off: data_off + slot_size * self.element_size])
        return data, slot_size
    
    def reset(self) -> None:
        """Reset the queue to its initial state.

        This is a low-level operation that should be used with caution.
        It is typically used in testing or when the queue needs to be reinitialized.
        Matches C++ queue.hpp reset().
        """
        # Clear the control array before rewinding the counter, so a stale reader that
        # is still using its old cursor sees an invalid slot rather than fresh data at
        # a stale index. This is the ordering _was_reset() is written against.
        for i in range(self.size):
            self._write_slot(i, K_INVALID_INDEX, 1)

        # Reset reserved_info to initial state. Unconditional: the counter lives at
        # offset 0 of the buffer in local mode too.
        self._write_reserved(0, 0)

        if self._enable_read_last:
            self._atomic_last_published.store_release(K_INVALID_INDEX)

        self._loss_count = 0

    def close(self) -> None:
        """Close the queue connection.

        For shared memory mode: releases all references to avoid 'exported pointers exist' errors.
        For local memory mode: releases local buffer.
        """
        try:
            # Release atomic wrapper references to the buffer
            if hasattr(self, '_atomic_reserved') and self._atomic_reserved:
                self._atomic_reserved.release()
            self._atomic_reserved = None

            if hasattr(self, '_atomic_slots') and self._atomic_slots:
                for slot in self._atomic_slots:
                    slot.release()
            self._atomic_slots = None

            # Release last_published atomic if it exists
            if hasattr(self, '_atomic_last_published') and self._atomic_last_published:
                self._atomic_last_published.release()
            self._atomic_last_published = None

            self._buf = None

            # Close shared memory if using it
            if self.use_shm and self._shm:
                try:
                    # prevent Exception ignored in: <function SharedMemory.__del__ at 0x00000176D1BFA8E0>
                    self._shm._mmap = None
                    self._shm.close()
                    self._shm = None
                except Exception:
                    pass

            # Clear local buffer if using it
            if not self.use_shm and self._local_buf:
                self._local_buf = None
        except Exception as e:
            print(e)
            pass

    def unlink(self) -> None:
        """Unlink (delete) the shared memory segment.

        Only applicable for shared memory mode. Does nothing for local memory mode.
        """
        if not self.use_shm:
            return  # Nothing to unlink for local memory

        try:
            if self._shm:
                self._shm.unlink()
        except Exception:
            pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: U100
        """Context manager exit - ensures proper cleanup."""
        self.close()
        return False


__all__ = [
    "SlickQueue",
    "AtomicCursor",
    "QueueTraits",
    "default_queue_traits",
    "validate_traits",
    "__version__",
]
