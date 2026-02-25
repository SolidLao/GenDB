#include <cstdio>
#include <cstring>
#include <cstdint>

union double_bits { double d; uint64_t u; };

void atomic_add_v1(double* addr, double val) {
    uint64_t old_bits, new_bits;
    double tmp;
    do {
        old_bits = __atomic_load_n((uint64_t*)addr, __ATOMIC_RELAXED);
        memcpy(&tmp, &old_bits, 8);
        tmp += val;
        memcpy(&new_bits, &tmp, 8);
    } while (!__atomic_compare_exchange_n((uint64_t*)addr, &old_bits, &new_bits, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
}

void atomic_add_v2(double* addr, double val) {
    union { double d; uint64_t u; } old_u, new_u;
    do {
        old_u.u = __atomic_load_n((uint64_t*)addr, __ATOMIC_RELAXED);
        new_u.d = old_u.d + val;
    } while (!__atomic_compare_exchange_n((uint64_t*)addr, &old_u.u, new_u.u, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
}

int main() {
    double x = 0.0;
    atomic_add_v1(&x, 1.5);
    atomic_add_v1(&x, 2.3);
    printf("v1: x = %.6f (expected 3.8)\n", x);
    
    x = 0.0;
    atomic_add_v2(&x, 1.5);
    atomic_add_v2(&x, 2.3);
    printf("v2: x = %.6f (expected 3.8)\n", x);
    return 0;
}
