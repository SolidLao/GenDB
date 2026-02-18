#pragma once
#include <chrono>
#include <cstdio>

namespace gendb {

#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point start;
    double* out_ms;  // optional: store result for programmatic access

    PhaseTimer(const char* n, double* out = nullptr)
        : name(n), start(std::chrono::high_resolution_clock::now()), out_ms(out) {}

    ~PhaseTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::printf("[TIMING] %s: %.2f ms\n", name, ms);
        if (out_ms) *out_ms = ms;
    }
};
#define GENDB_PHASE(name) gendb::PhaseTimer _phase_##__LINE__(name)
#define GENDB_PHASE_MS(name, var) double var = 0; gendb::PhaseTimer _phase_##__LINE__(name, &var)
#else
#define GENDB_PHASE(name)
#define GENDB_PHASE_MS(name, var) double var = 0
#endif

} // namespace gendb
