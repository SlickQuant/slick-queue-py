"""
Tests for shared-memory segment lifetime (matching C++ slick-queue v2.3.0).

No queue unlinks its segment on its own - not close(), not the context manager and
not process exit through multiprocessing's resource tracker. The name lives until
unlink(), remove_shm() or SlickQueue.remove() is called.
"""
import os
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import warnings
from multiprocessing import resource_tracker
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from slick_queue_py import SlickQueue

IS_WINDOWS = sys.platform == 'win32'

# A fresh interpreter, so it has its own resource tracker - a multiprocessing child
# would share the parent's, and the parent's exit would be what unlinked the name.
CHILD = r"""
import struct, sys
sys.path.insert(0, sys.argv[1])
from slick_queue_py import SlickQueue
name, mode, value = sys.argv[2], sys.argv[3], int(sys.argv[4])
if mode == 'create':
    q = SlickQueue(name=name, size=16, element_size=8)
    assert q._own, "child expected to create the segment"
else:
    q = SlickQueue(name=name, element_size=8)
idx = q.reserve()
q[idx][:8] = struct.pack('<Q', value)
q.publish(idx)
q.close()
"""


def _unique(prefix):
    return f"{prefix}_{os.getpid()}"


def _run_child(name, mode, value):
    result = subprocess.run(
        [sys.executable, "-c", CHILD, str(ROOT), name, mode, str(value)],
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"child failed:\n{result.stdout}\n{result.stderr}"
    # The tracker warns about every registered name it cleans up at shutdown
    assert "resource_tracker" not in result.stderr, result.stderr
    return result


def _values(q):
    """Every value published so far, from the start of the queue."""
    out = []
    cursor = 0
    while True:
        data, size, cursor = q.read(cursor)
        if data is None:
            return out
        out.append(struct.unpack('<Q', data[:8])[0])


def _expect_gone(name):
    try:
        q = SlickQueue(name=name, element_size=8)
    except FileNotFoundError:
        return
    q.close()
    raise AssertionError(f"segment {name!r} still exists")


def test_unlink_after_close_removes_segment():
    """unlink() after close() was a silent no-op: close() dropped the SharedMemory
    object unlink() went through, so the documented idiom leaked the segment."""
    name = _unique("lt_unlink_after_close")
    SlickQueue.remove(name)
    q = SlickQueue(name=name, size=16, element_size=8)
    q.close()
    q.unlink()
    _expect_gone(name)


def test_unlink_twice_is_noop():
    """A repeated unlink() must not delete a segment a later run created under the
    same name."""
    name = _unique("lt_unlink_twice")
    SlickQueue.remove(name)
    first = SlickQueue(name=name, size=16, element_size=8)
    first.close()
    first.unlink()

    second = SlickQueue(name=name, size=16, element_size=8)
    try:
        assert second._own
        first.unlink()
        reopened = SlickQueue(name=name, element_size=8)  # still there
        reopened.close()
    finally:
        second.close()
        second.unlink()


def test_close_and_context_manager_keep_segment():
    name = _unique("lt_close_keeps")
    SlickQueue.remove(name)
    keeper = SlickQueue(name=name, size=16, element_size=8)  # holds it on Windows
    try:
        with SlickQueue(name=name, element_size=8) as q:
            idx = q.reserve()
            q[idx][:8] = struct.pack('<Q', 7)
            q.publish(idx)
        q.close()
        reopened = SlickQueue(name=name, element_size=8)
        try:
            assert _values(reopened) == [7]
        finally:
            reopened.close()
    finally:
        keeper.close()
        keeper.unlink()


def test_remove_shm_local_queue_returns_false():
    q = SlickQueue(size=16, element_size=8)
    try:
        assert q.remove_shm() is False
    finally:
        q.close()


def test_remove_shm_keeps_open_mapping_valid():
    name = _unique("lt_remove_shm")
    SlickQueue.remove(name)
    q = SlickQueue(name=name, size=16, element_size=8)
    try:
        idx = q.reserve()
        q[idx][:8] = struct.pack('<Q', 42)
        q.publish(idx)

        assert q.remove_shm() is True
        # The mapping outlives its name
        assert _values(q) == [42]
        idx = q.reserve()
        q[idx][:8] = struct.pack('<Q', 43)
        q.publish(idx)
        assert _values(q) == [42, 43]

        if not IS_WINDOWS:
            # The next creator starts fresh rather than reattaching
            assert q.remove_shm() is False  # already gone
            fresh = SlickQueue(name=name, size=16, element_size=8)
            try:
                assert fresh._own
                assert _values(fresh) == []
            finally:
                fresh.close()
                fresh.unlink()
    finally:
        q.close()
        q.unlink()


def test_remove_by_name():
    name = _unique("lt_remove_static")
    SlickQueue.remove(name)
    q = SlickQueue(name=name, size=16, element_size=8)
    shm_name = q.get_shm_name()
    q.close()
    # get_shm_name()'s value, '/'-prefixed on POSIX, is accepted back
    assert SlickQueue.remove(shm_name) is True
    _expect_gone(name)
    # Windows has no name to remove, so it always reports success
    assert SlickQueue.remove(name) is IS_WINDOWS


def test_creator_exit_keeps_segment():
    """A creator's exit used to unlink the name on POSIX through the resource
    tracker, stranding any peer on an orphaned mapping. On Windows a section dies
    with its last handle, which no one else holds here."""
    if IS_WINDOWS:
        return
    name = _unique("lt_creator_exit")
    SlickQueue.remove(name)
    _run_child(name, 'create', 11)
    q = SlickQueue(name=name, element_size=8)
    try:
        assert _values(q) == [11]
        # A restarted creator reattaches and keeps the contents
        again = SlickQueue(name=name, size=16, element_size=8)
        try:
            assert not again._own
        finally:
            again.close()
    finally:
        q.close()
        q.unlink()


def test_attacher_exit_keeps_segment():
    """Before Python 3.13 attaching registered the name with the attacher's resource
    tracker too, so any peer's exit unlinked the segment on POSIX."""
    name = _unique("lt_attacher_exit")
    SlickQueue.remove(name)
    q = SlickQueue(name=name, size=16, element_size=8)
    try:
        _run_child(name, 'open', 21)
        _run_child(name, 'open', 22)
        reopened = SlickQueue(name=name, element_size=8)
        try:
            assert _values(reopened) == [21, 22]
        finally:
            reopened.close()
    finally:
        q.close()
        q.unlink()


def test_unlink_after_remove_shm_spares_replacement():
    """Regression: remove_shm() left unlink()'s pending name in place, so once the
    old segment was removed and a replacement created under the same name, the old
    queue's unlink() deleted the replacement. Also when remove_shm() finds the name
    already gone, since a replacement can take it just the same."""
    name = _unique("lt_rm_unlink")
    for gone_first in (False, True):
        SlickQueue.remove(name)
        old = SlickQueue(name=name, size=16, element_size=8)
        try:
            if gone_first:
                SlickQueue.remove(name)  # someone else got there first
                assert old.remove_shm() is IS_WINDOWS
            else:
                assert old.remove_shm() is True
            replacement = SlickQueue(name=name, size=16, element_size=8)
            try:
                if not IS_WINDOWS:
                    assert replacement._own
                old.unlink()
                SlickQueue(name=name, element_size=8).close()  # still there
            finally:
                replacement.close()
                replacement.unlink()
        finally:
            old.close()


def test_never_talks_to_resource_tracker():
    """No REGISTER or UNREGISTER for a queue's segment reaches multiprocessing's
    resource tracker - not on create, attach, close or unlink. Before Python 3.13
    the library registered each name and then retracted it at once, which raced
    between processes sharing one tracker and needed a lock that a fork could
    leave held (see the two tests below)."""
    name = _unique("lt_no_tracker")
    SlickQueue.remove(name)
    calls = []
    orig_register, orig_unregister = resource_tracker.register, resource_tracker.unregister
    resource_tracker.register = lambda n, rtype: calls.append(("register", n))
    resource_tracker.unregister = lambda n, rtype: calls.append(("unregister", n))
    try:
        creator = SlickQueue(name=name, size=16, element_size=8)
        opener = SlickQueue(name=name, element_size=8)
        second = SlickQueue(name=name, size=16, element_size=8)  # attaches
        for q in (second, opener, creator):
            q.close()
        creator.unlink()
    finally:
        resource_tracker.register, resource_tracker.unregister = orig_register, orig_unregister
        SlickQueue.remove(name)
    assert calls == [], calls


def test_fork_during_construction_does_not_deadlock():
    """A child forked while another thread is constructing a queue can still
    construct one. Python < 3.13 held a module lock around SharedMemory() for the
    tracker fallback, and a fork inside that window handed the child a copy of
    the lock that nothing would ever release, so its next construction hung."""
    if not hasattr(os, 'fork'):
        return
    name = _unique("lt_fork")
    SlickQueue.remove(name)
    keeper = SlickQueue(name=name, size=16, element_size=8)
    stop = threading.Event()

    def churn():
        while not stop.is_set():
            SlickQueue(name=name, element_size=8).close()

    thread = threading.Thread(target=churn, daemon=True)
    thread.start()
    try:
        with warnings.catch_warnings():
            # Python 3.12+ warns about forking a multi-threaded process
            warnings.simplefilter("ignore", DeprecationWarning)
            for attempt in range(20):
                pid = os.fork()
                if pid == 0:
                    try:
                        SlickQueue(name=name, element_size=8).close()
                    except BaseException:
                        os._exit(1)
                    os._exit(0)
                deadline = time.monotonic() + 5
                while True:
                    done, status = os.waitpid(pid, os.WNOHANG)
                    if done:
                        break
                    if time.monotonic() > deadline:
                        os.kill(pid, signal.SIGKILL)
                        os.waitpid(pid, 0)
                        raise AssertionError(
                            f"forked child {attempt} hung constructing a queue")
                    time.sleep(0.01)
                assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, \
                    f"forked child {attempt} failed (status {status})"
    finally:
        stop.set()
        thread.join()
        keeper.close()
        keeper.unlink()


RELATED_PROCESSES = r"""
import multiprocessing
import sys
sys.path.insert(0, sys.argv[1])
from slick_queue_py import SlickQueue


def churn(name, n):
    for _ in range(n):
        SlickQueue(name=name, element_size=8).close()


if __name__ == '__main__':
    name = sys.argv[2]
    SlickQueue.remove(name)
    q = SlickQueue(name=name, size=16, element_size=8)
    ctx = multiprocessing.get_context('spawn')
    procs = [ctx.Process(target=churn, args=(name, 200)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    q.close()
    q.unlink()
    # Every worker must succeed: max() let a killed one's negative code hide
    # behind any worker that exited 0
    codes = [p.exitcode for p in procs]
    if codes != [0] * len(procs):
        sys.exit(f"worker exit codes: {codes}")
"""


def test_related_processes_do_not_race_in_tracker():
    """multiprocessing children share their parent's resource tracker, so before
    Python 3.13 two of them attaching at once could send REGISTER, REGISTER,
    UNREGISTER, UNREGISTER and make the tracker print a KeyError traceback. The
    tracker writes to the stderr it inherited, so the scenario runs in a fresh
    interpreter whose stderr this test captures. A spawned child re-imports the
    script by path, hence a file rather than -c."""
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "related_processes.py"
        script.write_text(RELATED_PROCESSES)
        result = subprocess.run(
            [sys.executable, str(script), str(ROOT), _unique("lt_related")],
            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"script failed:\n{result.stdout}\n{result.stderr}"
    assert "KeyError" not in result.stderr, result.stderr
    assert "resource_tracker" not in result.stderr, result.stderr


def run_all_tests():
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]

    print("=" * 70)
    print("Segment lifetime tests")
    print("=" * 70)

    passed = 0
    failed = 0
    for name, test_func in tests:
        try:
            print(f"\n[TEST] {name}...", end=" ")
            test_func()
            print("[PASSED]")
            passed += 1
        except Exception as e:
            print("[FAILED]")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)
    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
