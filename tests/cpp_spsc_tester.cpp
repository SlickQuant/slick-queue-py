/**
 * C++ side of the slick::spsc_queue interop tests (test_spsc_interop.py).
 * Elements are uint64_t, so the Python side uses element_size=8.
 *
 *   cpp_spsc_tester consume <name> <count>               attach, read 0..count-1 in order
 *   cpp_spsc_tester produce <name> <count> [first]       attach, push first..first+count-1
 *   cpp_spsc_tester create_produce <name> <size> <count> create, push 0..count-1
 *   cpp_spsc_tester read_last <name>                     attach, print newest value and dropped
 *   cpp_spsc_tester attach_spsc <name>                   attach as slick::spsc_queue
 *   cpp_spsc_tester attach_queue <name>                  attach as slick::queue
 *
 * Prints "OK ..." and exits 0 on success, "ERROR: ..." and exits non-zero otherwise.
 */

#include <slick/queue.hpp>
#include <slick/spsc_queue.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

namespace {

using spsc = slick::spsc_queue<uint64_t>;
using clock_type = std::chrono::steady_clock;
constexpr auto kTimeout = std::chrono::seconds(30);

// Yield while the peer catches up. False once the deadline passes, so a dead peer
// fails the test fast instead of hanging it.
bool wait_for_peer(clock_type::time_point deadline, uint64_t done) {
    if (clock_type::now() > deadline) {
        std::cout << "ERROR: timed out after " << done << " items" << std::endl;
        return false;
    }
    std::this_thread::yield();
    return true;
}

// Batches of 1..3, so a batch regularly wraps around the end of the ring.
int produce(spsc& q, uint64_t first, uint64_t count) {
    const auto deadline = clock_type::now() + kTimeout;
    for (uint64_t i = 0; i < count;) {
        const auto n = static_cast<uint32_t>(std::min<uint64_t>({1 + i % 3, count - i, q.size()}));
        const auto index = q.try_reserve(n);
        if (index == spsc::invalid_index) {
            if (!wait_for_peer(deadline, i)) {
                return 3;
            }
            continue;
        }
        for (uint32_t j = 0; j < n; ++j) {
            *q[index + j] = first + i + j;
        }
        q.publish(index, n);
        i += n;
    }
    std::cout << "OK produced " << count << std::endl;
    return 0;
}

int consume(spsc& q, uint64_t count) {
    const auto deadline = clock_type::now() + kTimeout;
    for (uint64_t n = 0; n < count;) {
        if (const auto* v = q.read()) {
            if (*v != n) {
                std::cout << "ERROR: expected " << n << " but got " << *v << std::endl;
                return 2;
            }
            ++n;
        } else if (!wait_for_peer(deadline, n)) {
            return 3;
        }
    }
    q.release();
    std::cout << "OK consumed " << count << std::endl;
    return 0;
}

uint64_t arg(char** argv, int i) { return std::strtoull(argv[i], nullptr, 10); }

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "ERROR: usage: cpp_spsc_tester <mode> <name> [args]" << std::endl;
        return 64;
    }
    const std::string mode = argv[1];
    const char* name = argv[2];
    try {
        if (mode == "consume" && argc == 4) {
            spsc q(name);
            return consume(q, arg(argv, 3));
        }
        if (mode == "produce" && (argc == 4 || argc == 5)) {
            spsc q(name);
            return produce(q, argc == 5 ? arg(argv, 4) : 0, arg(argv, 3));
        }
        if (mode == "create_produce" && argc == 5) {
            spsc q(static_cast<uint32_t>(arg(argv, 3)), name);
            return produce(q, 0, arg(argv, 4));
        }
        if (mode == "read_last" && argc == 3) {
            // No release() on purpose: the next consumer to attach gets this item again.
            spsc q(name);
            uint64_t dropped = 0;
            const auto* v = q.read_last(dropped);
            if (v) {
                std::cout << "OK value " << *v << " dropped " << dropped << std::endl;
            } else {
                std::cout << "OK empty dropped " << dropped << std::endl;
            }
            return 0;
        }
        if (mode == "attach_spsc" && argc == 3) {
            spsc q(name);
            std::cout << "OK attached size " << q.size() << std::endl;
            return 0;
        }
        if (mode == "attach_queue" && argc == 3) {
            slick::queue<uint64_t> q(name);
            std::cout << "OK attached size " << q.size() << std::endl;
            return 0;
        }
    } catch (const std::exception& e) {
        std::cout << "ERROR: " << e.what() << std::endl;
        return 1;
    }
    std::cout << "ERROR: bad arguments for mode " << mode << std::endl;
    return 64;
}
