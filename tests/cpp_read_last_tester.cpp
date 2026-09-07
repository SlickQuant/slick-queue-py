/**
 * C++ test program for read_last() interoperability with Python.
 *
 * Usage modes:
 *   1. Read mode:  cpp_read_last_tester <queue_name> read <element_size>
 *      - Opens queue and calls read_last()
 *      - Prints the data and size
 *
 *   2. Write mode: cpp_read_last_tester <queue_name> write <size> <count> <element_size>
 *      - Creates/opens queue
 *      - Publishes <count> items
 *      - Calls read_last() to verify
 */

#include <slick/queue.hpp>
#include <iostream>
#include <cstring>
#include <cstdint>

struct Element {
    uint32_t value;
    char padding[28];  // Pad to 32 bytes
};

void print_usage() {
    std::cout << "Usage:\n"
              << "  Read mode:  cpp_read_last_tester <queue_name> read <element_size>\n"
              << "  Write mode: cpp_read_last_tester <queue_name> write <size> <count> <element_size>\n";
}

int read_mode(const char* queue_name, uint32_t element_size) {
    try {
        // Open existing queue
        slick::queue<Element> queue(queue_name);

        std::cout << "C++ opened queue: " << queue_name << std::endl;
        std::cout << "  Queue size: " << queue.size() << std::endl;

        // Call read_last()
        auto [data, size] = queue.read_last();

        if (data == nullptr) {
            std::cout << "read_last() returned: nullptr (queue empty or no data)" << std::endl;
            std::cout << "  size: " << size << std::endl;
            return 0;
        }

        std::cout << "read_last() returned:" << std::endl;
        std::cout << "  data->value: " << data->value << std::endl;
        std::cout << "  size: " << size << std::endl;

        // Print first few bytes as hex
        std::cout << "  first 16 bytes: ";
        const uint8_t* bytes = reinterpret_cast<const uint8_t*>(data);
        for (auto i = 0u; i < 16 && i < element_size; i++) {
            printf("%02X ", bytes[i]);
        }
        std::cout << std::endl;

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

int write_mode(const char* queue_name, uint32_t queue_size, uint32_t count, uint32_t element_size) {
    try {
        // Create/open queue
        slick::queue<Element> queue(queue_size, queue_name);

        std::cout << "C++ created/opened queue: " << queue_name << std::endl;
        std::cout << "  Queue size: " << queue.size() << std::endl;
        std::cout << "  Publishing " << count << " items..." << std::endl;

        // Publish items
        for (uint32_t i = 0; i < count; i++) {
            uint64_t idx = queue.reserve();
            Element* elem = queue[idx];
            elem->value = 2000 + i;  // Use offset to distinguish from Python
            queue.publish(idx);

            if (i < 5 || i >= count - 5) {
                std::cout << "    Item " << i << ": value=" << elem->value << std::endl;
            } else if (i == 5) {
                std::cout << "    ..." << std::endl;
            }
        }

        std::cout << "Publishing complete." << std::endl;

        // Call read_last() to verify
        auto [data, size] = queue.read_last();

        if (data == nullptr) {
            std::cerr << "Error: read_last() returned nullptr after publishing!" << std::endl;
            return 1;
        }

        std::cout << "\nC++ read_last() verification:" << std::endl;
        std::cout << "  data->value: " << data->value << std::endl;
        std::cout << "  size: " << size << std::endl;

        uint32_t expected = 2000 + count - 1;
        if (data->value != expected) {
            std::cerr << "Error: Expected value " << expected << ", got " << data->value << std::endl;
            return 1;
        }

        std::cout << "  [OK] Value matches expected" << std::endl;

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    const char* queue_name = argv[1];
    const char* mode = argv[2];

    if (strcmp(mode, "read") == 0) {
        if (argc != 4) {
            std::cerr << "Error: Read mode requires: <queue_name> read <element_size>" << std::endl;
            return 1;
        }
        uint32_t element_size = std::atoi(argv[3]);
        return read_mode(queue_name, element_size);

    } else if (strcmp(mode, "write") == 0) {
        if (argc != 6) {
            std::cerr << "Error: Write mode requires: <queue_name> write <size> <count> <element_size>" << std::endl;
            return 1;
        }
        uint32_t queue_size = std::atoi(argv[3]);
        uint32_t count = std::atoi(argv[4]);
        uint32_t element_size = std::atoi(argv[5]);
        return write_mode(queue_name, queue_size, count, element_size);

    } else {
        std::cerr << "Error: Unknown mode '" << mode << "'" << std::endl;
        print_usage();
        return 1;
    }
}
