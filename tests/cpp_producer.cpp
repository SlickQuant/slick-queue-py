/**
 * C++ Producer - writes data to shared memory queue for Python consumer
 *
 * Usage: cpp_producer <queue_name> <num_items> <element_size>
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
#include <chrono>
#include <thread>
#include <array>
#include <random>

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <queue_name> <num_items> <element_size>\n";
        return 1;
    }

    const char* queue_name = argv[1];
    int size = std::atoi(argv[2]);
    int num_items = std::atoi(argv[3]);
    int element_size = std::atoi(argv[4]);

    std::cout << "C++ Producer starting...\n";
    std::cout << "Queue: " << queue_name << "\n";
    std::cout << "Queue size: " << size << "\n";
    std::cout << "Items: " << num_items << "\n";
    std::cout << "Element size: " << element_size << "\n";

    // Open existing queue (created by Python)
    // Note: C++ SlickQueue constructor opens existing queue when only name is provided
    // Use array type to match Python's element_size
    using Element = std::array<uint8_t, 32>;
    std::cout << "sizeof(Element): " << sizeof(Element) << "\n";

    std::random_device rd;
    std::mt19937 engine(rd()); // Mersenne Twister engine is a good choice

    // 2. Define the range (inclusive)
    const double lower_bound = 1;
    const double upper_bound = 3;

    // 3. Define the distribution
    // This produces random numbers in the range [lower_bound, upper_bound)
    std::uniform_real_distribution<double> dist(lower_bound, upper_bound);

    std::this_thread::sleep_for(std::chrono::milliseconds((int)dist(engine)));
    try {
        slick::queue<Element> queue(queue_name);

        // Produce items
        for (int i = 0; i < num_items; i++) {
            // Reserve slot
            auto idx = queue.reserve();

            // Write data: [worker_id (4 bytes), item_num (4 bytes)]
            Element* slot = queue[idx];
            uint32_t worker_id = 999;  // Special C++ producer ID
            uint32_t item_num = i;

            std::memcpy(slot->data(), &worker_id, sizeof(worker_id));
            std::memcpy(slot->data() + 4, &item_num, sizeof(item_num));

            // Publish
            queue.publish(idx);

            // // Small delay to avoid overwhelming small queues
            // if (num_items > 100 && i % 10 == 0) {
                    // 4. Generate and print a random number
            std::this_thread::sleep_for(std::chrono::milliseconds((int)dist(engine)));
            // }
        }

        std::cout << "C++ Producer completed: " << num_items << " items written\n";
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "C++ Producer error: " << e.what() << "\n";
        return 1;
    }
}
