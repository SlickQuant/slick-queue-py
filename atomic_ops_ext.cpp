/*
 * Python C++ extension for cross-platform atomic operations
 *
 * As of slick-queue v1.2.0+, reserved_info is a packed uint64_t (not a struct):
 * - Bits 0-15: size (16-bit)
 * - Bits 16-63: index (48-bit)
 *
 * Uses C++11 std::atomic<uint64_t> for lock-free operations
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <atomic>
#include <cstdint>

// Python function: atomic_compare_exchange_64(addr, expected, desired)
// Wraps std::atomic<uint64_t> for cross-language synchronization
// Returns: (success, actual_value)
static PyObject* py_atomic_compare_exchange_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long expected, desired;

    if (!PyArg_ParseTuple(args, "KKK", &addr, &expected, &desired)) {
        return NULL;
    }

    // Cast address to std::atomic<uint64_t> pointer
    // This uses the SAME atomic that C++ uses - ensuring synchronization!
    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);

    // Prepare expected value (will be updated if CAS fails)
    uint64_t expected_val = static_cast<uint64_t>(expected);
    uint64_t desired_val = static_cast<uint64_t>(desired);

    // Perform atomic compare-exchange
    bool success = atomic_ptr->compare_exchange_weak(
        expected_val,
        desired_val,
        std::memory_order_release,  // success order
        std::memory_order_relaxed   // failure order
    );

    // Return (success, actual_value)
    return Py_BuildValue("(iK)", success ? 1 : 0, (unsigned long long)expected_val);
}

// Python function: atomic_exchange_64(addr, value)
// Returns: previous_value
static PyObject* py_atomic_exchange_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long value;

    if (!PyArg_ParseTuple(args, "KK", &addr, &value)) {
        return NULL;
    }

    // Destination must be 8-byte aligned
    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    // Cast address to atomic pointer
    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);

    // Perform atomic exchange with release ordering
    uint64_t previous = atomic_ptr->exchange(value, std::memory_order_release);

    return PyLong_FromUnsignedLongLong(previous);
}

// Python function: atomic_load_64(addr)
// Returns: value
static PyObject* py_atomic_load_64(PyObject* self, PyObject* args) {
    unsigned long long addr;

    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }

    // Destination must be 8-byte aligned
    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    // Cast address to atomic pointer
    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);

    // Perform atomic load with acquire ordering
    uint64_t value = atomic_ptr->load(std::memory_order_acquire);

    return PyLong_FromUnsignedLongLong(value);
}

// Python function: atomic_store_64(addr, value)
// Returns: None
static PyObject* py_atomic_store_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long value;

    if (!PyArg_ParseTuple(args, "KK", &addr, &value)) {
        return NULL;
    }

    // Destination must be 8-byte aligned
    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    // Cast address to atomic pointer
    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);

    // Perform atomic store with release ordering
    atomic_ptr->store(value, std::memory_order_release);

    Py_RETURN_NONE;
}

// Alias for atomic_compare_exchange_64 (for compatibility with Linux/macOS naming)
static PyObject* py_atomic_cas_64(PyObject* self, PyObject* args) {
    return py_atomic_compare_exchange_64(self, args);
}

// Module method definitions
static PyMethodDef AtomicOpsMethods[] = {
    {"atomic_compare_exchange_64", py_atomic_compare_exchange_64, METH_VARARGS,
     "Atomic 64-bit compare-exchange using C++ std::atomic<uint64_t>\n\n"
     "Args:\n"
     "    addr: Memory address\n"
     "    expected: Expected value\n"
     "    desired: Desired value\n\n"
     "Returns:\n"
     "    (success, actual): Tuple of success flag and actual value\n"},

    {"atomic_cas_64", py_atomic_cas_64, METH_VARARGS,
     "Atomic 64-bit compare-and-swap (alias for atomic_compare_exchange_64)\n\n"
     "Args:\n"
     "    addr: Memory address\n"
     "    expected: Expected value\n"
     "    desired: Desired value\n\n"
     "Returns:\n"
     "    (success, actual): Tuple of success flag and actual value\n"},

    {"atomic_exchange_64", py_atomic_exchange_64, METH_VARARGS,
     "Perform 64-bit atomic exchange using C++11 std::atomic.\n\n"
     "Args:\n"
     "    addr: Memory address (must be 8-byte aligned)\n"
     "    value: New value\n\n"
     "Returns:\n"
     "    previous_value: The value before the exchange\n"},

    {"atomic_load_64", py_atomic_load_64, METH_VARARGS,
     "Perform 64-bit atomic load using C++11 std::atomic.\n\n"
     "Args:\n"
     "    addr: Memory address (must be 8-byte aligned)\n\n"
     "Returns:\n"
     "    value: The loaded value\n"},

    {"atomic_store_64", py_atomic_store_64, METH_VARARGS,
     "Perform 64-bit atomic store using C++11 std::atomic.\n\n"
     "Args:\n"
     "    addr: Memory address (must be 8-byte aligned)\n"
     "    value: Value to store\n\n"
     "Returns:\n"
     "    None\n"},

    {NULL, NULL, 0, NULL}
};

// Module definition
static struct PyModuleDef atomic_ops_ext_module = {
    PyModuleDef_HEAD_INIT,
    "atomic_ops_ext",
    "Cross-platform atomic operations extension using C++11 std::atomic",
    -1,
    AtomicOpsMethods
};

// Module initialization
PyMODINIT_FUNC PyInit_atomic_ops_ext(void) {
    return PyModule_Create(&atomic_ops_ext_module);
}
