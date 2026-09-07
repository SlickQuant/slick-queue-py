/**
 * C++ Multi-Producer - multiple threads write to shared memory queue
 *
 * Usage: cpp_multi_producer <queue_name> <num_threads> <items_per_thread> <element_size>
 */

// // CRITICAL: Force ANSI mode for Python interoperability
// #ifdef _WIN32
// #undef UNICODE
// #undef _UNICODE
// #ifndef _MBCS
// #define _MBCS 1
// #endif
// #endif

#include <limits>
#include <slick/queue.hpp>
#include <iostream>
#include <cstring>
#include <cstdint>
#include <thread>
#include <vector>
#include <chrono>
#include <array>
#include <random>
#include <filesystem>

namespace fs = std::filesystem;

void producer_thread(const char* queue_name, int worker_id, int num_items, int size) {
    try {
        // Open existing queue (created by Python)
        // Use array type to match Python's element_size
        using Element = std::array<uint8_t, 32>;
        slick::queue<Element> queue(size, queue_name);

        while (!fs::exists("ready")) {
            // wait for consumer to be ready
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Add startup delay to allow Python processes to initialize
        // This ensures concurrent execution with Python producers
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        std::random_device rd;
        std::mt19937 engine(rd()); // Mersenne Twister engine is a good choice

        // 2. Define the range (inclusive)
        const double lower_bound = 1;
        const double upper_bound = 3;

        // 3. Define the distribution
        // This produces random numbers in the range [lower_bound, upper_bound)
        std::uniform_real_distribution<double> dist(lower_bound, upper_bound);

        for (int i = 0; i < num_items; i++) {
            // Reserve slot (thread-safe atomic CAS)
            auto idx = queue.reserve();

            // Write data: [worker_id (4 bytes), item_num (4 bytes)]
            Element* slot = queue[idx];
            uint32_t wid = worker_id;
            uint32_t item_num = i;

            std::memcpy(slot->data(), &wid, sizeof(wid));
            std::memcpy(slot->data() + 4, &item_num, sizeof(item_num));

            // Publish (atomic with release semantics)
            queue.publish(idx);

            std::this_thread::sleep_for(std::chrono::milliseconds((int)dist(engine)));
        }

        std::cout << "C++ Thread " << worker_id << " completed: " << num_items << " items\n";

        if (queue.own_buffer()) {
            // Give some time for consumer to finish
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

    } catch (const std::exception& e) {
        std::cerr << "C++ Thread " << worker_id << " error: " << e.what() << "\n";
    }
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <queue_name> <size> <num_threads> <items_per_thread>\n";
        return 1;
    }

    const char* queue_name = argv[1];
    int size = std::atoi(argv[2]);
    int num_threads = std::atoi(argv[3]);
    int items_per_thread = std::atoi(argv[4]);

    std::cout << "C++ Multi-Producer starting...\n";
    std::cout << "Queue: " << queue_name << "\n";
    std::cout << "Queue size: " << size << "\n";
    std::cout << "Threads: " << num_threads << "\n";
    std::cout << "Items per thread: " << items_per_thread << "\n";
    std::cout << "Total items: " << (num_threads * items_per_thread) << "\n";

    auto start = std::chrono::steady_clock::now();

    // Launch producer threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(producer_thread, queue_name, i + 1000, items_per_thread, size);
    }

    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "C++ Multi-Producer completed in " << duration << "ms\n";
    std::cout << "Throughput: " << (num_threads * items_per_thread * 1000 / duration) << " items/sec\n";

    return 0;
}
