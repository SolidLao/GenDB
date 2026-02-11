# Compiled Queries

## What It Is
Compiled queries generate specialized machine code (or C++ templates) for each query at runtime, eliminating interpretation overhead, virtual function calls, and enabling aggressive compiler optimizations.

## When To Use
- Long-running analytical queries where compilation cost is amortized
- Queries with complex expressions or tight loops
- When CPU efficiency is critical (scan-heavy workloads)
- Queries executed multiple times (prepared statements)
- When 10-100ms compilation latency is acceptable

## Key Implementation Ideas
- **Template-based compilation:** Use C++ templates with lambda predicates/projections so the compiler inlines and optimizes away all abstractions at compile time
- **LLVM JIT compilation:** Generate LLVM IR for query pipelines at runtime, then compile to native machine code for maximum performance
- **Push-based compiled pipelines:** Replace pull-based Volcano iterator model with push-based callbacks, eliminating per-tuple virtual function calls
- **Data-centric code generation:** Generate code that directly references columnar arrays (e.g., col1[i], col2[i]) instead of generic tuple abstractions
- **Adaptive compilation:** Interpret queries for the first few executions, then JIT-compile after a threshold to avoid paying compilation cost for one-shot queries
- **Expression compilation:** Generate specialized native code for expression evaluation trees, turning interpreted expression walks into straight-line arithmetic
- **Whole-pipeline compilation:** Compile the entire pipeline (scan+filter+project+aggregate) into a single function to maximize instruction cache locality
- **Compilation caching:** Cache compiled query functions keyed by query structure so repeated queries skip recompilation
- **Tiered optimization levels:** Use fast compilation (-O0/-O1) for first execution and recompile with aggressive optimization (-O3) for hot queries

## Performance Characteristics
- **Speedup:** 2-10x vs interpreted execution for CPU-bound queries
- **Compilation overhead:** 10-100ms (LLVM), 1-5ms (template instantiation)
- **Best for:** Queries running >100ms where compilation cost is <5% of execution time

## Pitfalls
- Short queries (<10ms): compilation overhead dominates execution time
- Code cache management needed to evict compiled code and avoid memory bloat
- LLVM adds a heavy dependency; template-based approach is more portable
