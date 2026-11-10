"""
SpscQueue <-> C++ slick::spsc_queue<uint64_t> interop, driven through cpp_spsc_tester.

Build the tester first (see README, "C++/Python Interoperability Tests").
"""
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from slick_queue_py import SlickQueue, SpscQueue
from test_interop import find_cpp_executable
from test_spsc import Segments, consume, produce, push, run_tests, value_of

COUNT = 20000


def _tester(*args):
    return [find_cpp_executable("cpp_spsc_tester"), *map(str, args)]


def _run(*args, timeout=30):
    result = subprocess.run(_tester(*args), capture_output=True, text=True, timeout=timeout)
    return result.returncode, result.stdout.strip()


def _finish(proc, timeout=30):
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
        raise AssertionError(f"cpp_spsc_tester did not finish: {out}")
    return proc.returncode, out.strip()


def test_python_producer_cpp_consumer():
    with Segments() as s:
        name = s.name("i_py2cpp")
        q = s.track(SpscQueue(name=name, size=64, element_size=8))
        proc = subprocess.Popen(_tester("consume", q.get_shm_name(), COUNT),
                                stdout=subprocess.PIPE, text=True)
        try:
            produce(q, 0, COUNT, proc=proc)
        finally:
            rc, out = _finish(proc)
        assert rc == 0 and out == f"OK consumed {COUNT}", out


def test_cpp_producer_python_consumer():
    with Segments() as s:
        name = s.name("i_cpp2py")
        q = s.track(SpscQueue(name=name, size=64, element_size=8))
        proc = subprocess.Popen(_tester("produce", q.get_shm_name(), COUNT),
                                stdout=subprocess.PIPE, text=True)
        try:
            mismatches = consume(q, COUNT)
        finally:
            rc, out = _finish(proc)
        assert rc == 0 and out == f"OK produced {COUNT}", out
        assert mismatches == 0
        assert q.read() is None


def test_python_attaches_to_cpp_created_segment():
    # C++ creates and fills the ring, then blocks until Python drains it, so the segment
    # stays alive (on Windows too) until Python has attached.
    with Segments() as s:
        name = s.name("i_cppnew")
        shm_name = ('/' + name) if sys.platform != 'win32' else name
        proc = subprocess.Popen(_tester("create_produce", shm_name, 16, COUNT),
                                stdout=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    q = s.track(SpscQueue(name=name, element_size=8))
                    break
                except (FileNotFoundError, RuntimeError):
                    # Not created yet, or created but not ready within the wait.
                    assert proc.poll() is None, "C++ creator exited before Python attached"
                    assert time.monotonic() < deadline, "C++ never created the segment"
                    time.sleep(0.01)
            assert q.size == 16 and not q._own
            mismatches = consume(q, COUNT)
        finally:
            rc, out = _finish(proc)
        assert rc == 0 and out == f"OK produced {COUNT}", out
        assert mismatches == 0


def test_read_last_both_ways():
    with Segments() as s:
        name = s.name("i_last")
        q = s.track(SpscQueue(name=name, size=16, element_size=8))
        shm_name = q.get_shm_name()

        rc, out = _run("produce", shm_name, 10)
        assert rc == 0, out
        data, dropped = q.read_last()
        assert (value_of(data), dropped) == (9, 9)

        # q's producer cursor predates the C++ producer, so produce through a fresh
        # attach, which picks up the shared tail.
        producer = s.track(SpscQueue(name=name, element_size=8))
        for i in range(100, 105):
            push(producer, i)
        rc, out = _run("read_last", shm_name)
        assert (rc, out) == (0, "OK value 104 dropped 4"), out

        # The C++ consumer exited holding 104 without release(), so the next consumer to
        # attach gets it again.
        q.close()
        reattached = s.track(SpscQueue(name=name, element_size=8))
        assert value_of(reattached.read()) == 104
        assert reattached.read() is None


def test_cpp_rejects_wrong_queue_type():
    with Segments() as s:
        spsc_name = s.name("i_vs_q")
        queue_name = s.name("i_vs_s")
        spsc = s.track(SpscQueue(name=spsc_name, size=8, element_size=8))
        queue = s.track(SlickQueue(name=queue_name, size=8, element_size=8))

        rc, out = _run("attach_queue", spsc.get_shm_name())
        assert rc == 1 and "holds a slick::spsc_queue" in out, out
        rc, out = _run("attach_spsc", queue.get_shm_name())
        assert rc == 1 and "holds a slick::queue" in out, out
        rc, out = _run("attach_spsc", spsc.get_shm_name())
        assert (rc, out) == (0, "OK attached size 8"), out


if __name__ == '__main__':
    sys.exit(0 if run_tests("SpscQueue C++ interop tests", globals()) else 1)
