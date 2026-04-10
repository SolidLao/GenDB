// mqo_profile.hpp — runtime instrumentation for MQO-mode generated code.
//
// This header is included by the Batch Code Generator's output (shared/*.hpp,
// mqo_main.cpp, and queries/qN.cpp). It provides:
//   - MQO_TIME_BEGIN(name) / MQO_TIME_END(name) macros to bracket shared
//     components, per-query tails, and dispatcher phases.
//   - A global in-process profile registry that aggregates per-region timings
//     across the run and can be flushed to a JSON file at process exit.
//
// The profile JSON written by mqo::profile::flush() is consumed by the
// Strategy ε optimizer (src/gendb/orchestrator/mqo-optimizer.mjs) to decide
// which layer (shared component / tail / dispatcher) contains the bottleneck.
//
// Design notes:
//   - Thread-safe: uses std::mutex to guard the registry. Overhead is
//     negligible for coarse-grained regions (entire shared components).
//   - Header-only to keep the generated artifact single-binary.
//   - Also emits [TIMING] lines for backward compatibility with the existing
//     parseTimingLines helper in orchestrator/measure.mjs.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace mqo {
namespace profile {

struct RegionStats {
    std::string name;
    std::string kind;       // "shared" | "tail" | "dispatcher" | "phase"
    double total_ms = 0.0;
    uint64_t call_count = 0;
    double min_ms = 1e18;
    double max_ms = 0.0;
};

class Registry {
 public:
    static Registry& instance() {
        static Registry r;
        return r;
    }

    void record(const std::string& name, const std::string& kind, double ms) {
        std::lock_guard<std::mutex> lock(mu_);
        auto& s = regions_[name];
        if (s.name.empty()) {
            s.name = name;
            s.kind = kind;
        }
        s.total_ms += ms;
        s.call_count += 1;
        if (ms < s.min_ms) s.min_ms = ms;
        if (ms > s.max_ms) s.max_ms = ms;
    }

    // Flush to JSON. Safe to call multiple times; overwrites the file.
    void flush(const std::string& path) const {
        std::lock_guard<std::mutex> lock(mu_);
        std::ofstream os(path);
        if (!os) return;
        os << "{\n  \"version\": \"1.0\",\n  \"regions\": {\n";
        bool first = true;
        for (const auto& kv : regions_) {
            if (!first) os << ",\n";
            first = false;
            const auto& s = kv.second;
            os << "    \"" << escape(s.name) << "\": {";
            os << "\"kind\": \"" << escape(s.kind) << "\",";
            os << "\"total_ms\": " << s.total_ms << ",";
            os << "\"call_count\": " << s.call_count << ",";
            os << "\"min_ms\": " << (s.call_count ? s.min_ms : 0.0) << ",";
            os << "\"max_ms\": " << s.max_ms;
            os << "}";
        }
        os << "\n  }\n}\n";
    }

    // Erase all state (used by tests / repeated measurement).
    void reset() {
        std::lock_guard<std::mutex> lock(mu_);
        regions_.clear();
    }

 private:
    Registry() = default;
    mutable std::mutex mu_;
    std::map<std::string, RegionStats> regions_;

    static std::string escape(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (char c : s) {
            if (c == '"' || c == '\\') out += '\\';
            out += c;
        }
        return out;
    }
};

struct ScopedTimer {
    const char* name;
    const char* kind;
    std::chrono::steady_clock::time_point start;

    ScopedTimer(const char* n, const char* k)
        : name(n), kind(k), start(std::chrono::steady_clock::now()) {}

    ~ScopedTimer() {
        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        Registry::instance().record(name, kind, ms);
        // Emit [TIMING] line for legacy parseTimingLines helper.
        std::printf("[TIMING] %s: %.2f ms\n", name, ms);
    }
};

inline void flush(const std::string& path) {
    Registry::instance().flush(path);
}

inline void record(const char* name, const char* kind, double ms) {
    Registry::instance().record(name, kind, ms);
}

}  // namespace profile
}  // namespace mqo

// ---------------------------------------------------------------------------
// Public macros
// ---------------------------------------------------------------------------

#define MQO_CONCAT_INNER(a, b) a##b
#define MQO_CONCAT(a, b) MQO_CONCAT_INNER(a, b)

// Bracket a region with a scoped timer. Use the matching END only if you want
// to capture the name symmetrically; ScopedTimer RAII handles end automatically.
#define MQO_TIME_SHARED(name)     mqo::profile::ScopedTimer MQO_CONCAT(_mqo_t_, __LINE__)(name, "shared")
#define MQO_TIME_TAIL(name)       mqo::profile::ScopedTimer MQO_CONCAT(_mqo_t_, __LINE__)(name, "tail")
#define MQO_TIME_DISPATCHER(name) mqo::profile::ScopedTimer MQO_CONCAT(_mqo_t_, __LINE__)(name, "dispatcher")
#define MQO_TIME_PHASE(name)      mqo::profile::ScopedTimer MQO_CONCAT(_mqo_t_, __LINE__)(name, "phase")

// Begin/end pair — useful when the region doesn't map to a C++ scope.
#define MQO_TIME_BEGIN(name) \
    auto MQO_CONCAT(_mqo_t0_, __LINE__) = std::chrono::steady_clock::now(); \
    const char* MQO_CONCAT(_mqo_n_, __LINE__) = name

#define MQO_TIME_END(kind) \
    do { \
        auto _mqo_end = std::chrono::steady_clock::now(); \
        double _mqo_ms = std::chrono::duration<double, std::milli>(_mqo_end - MQO_CONCAT(_mqo_t0_, __LINE__)).count(); \
        mqo::profile::record(MQO_CONCAT(_mqo_n_, __LINE__), kind, _mqo_ms); \
        std::printf("[TIMING] %s: %.2f ms\n", MQO_CONCAT(_mqo_n_, __LINE__), _mqo_ms); \
    } while (0)

// Flush the profile to disk. Call once at the end of main() or inside an
// atexit handler. The path should typically be "<iter_dir>/profile.json".
#define MQO_PROFILE_FLUSH(path) mqo::profile::flush(path)
