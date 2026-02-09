# Compiled Queries

## What It Is
Compiled queries generate specialized machine code (or C++ templates) for each query at runtime, eliminating interpretation overhead, virtual function calls, and enabling aggressive compiler optimizations.

## When To Use
- Long-running analytical queries (compilation cost amortized)
- Queries with complex expressions or tight loops
- When CPU efficiency is critical (scan-heavy workloads)
- Queries executed multiple times (prepared statements)
- When you can afford 10-100ms compilation latency

## Key Implementation Ideas

**Template-Based Compilation (No JIT Required):**
```cpp
// Generate specialized C++ code, compile with existing compiler
template<typename FilterFunc, typename ProjectFunc>
void compiled_pipeline(Table& table, FilterFunc filter, ProjectFunc project) {
    for (size_t i = 0; i < table.row_count; i++) {
        auto tuple = table.get_tuple(i);
        if (filter(tuple)) {  // Inlined at compile time
            emit(project(tuple));  // Inlined at compile time
        }
    }
}

// Usage: compiler optimizes away all abstractions
auto query = compiled_pipeline(
    table,
    [](Tuple& t) { return t.col1 > 100; },
    [](Tuple& t) { return t.col1 + t.col2; }
);
```

**LLVM-Based JIT Compilation:**
```cpp
class QueryCompiler {
    llvm::LLVMContext context_;
    llvm::Module* module_;

    llvm::Function* compile_scan_filter(PhysicalPlan& plan) {
        auto* func = create_function("scan_filter");
        auto* bb = llvm::BasicBlock::Create(context_, "entry", func);
        llvm::IRBuilder<> builder(bb);

        // Generate tight loop
        auto* loop_var = builder.CreateAlloca(builder.getInt64Ty());
        // ... generate IR for scan, filter, project

        return func;
    }

    void* get_function_pointer(llvm::Function* func) {
        llvm::ExecutionEngine* engine = create_engine();
        return engine->getPointerToFunction(func);
    }
};
```

**Push-Based vs Pull-Based:**
```cpp
// PULL-BASED: Operators pull tuples from children (Volcano-style)
class Operator {
    virtual Tuple next() = 0;  // Virtual call per tuple (slow)
};

// PUSH-BASED: Parent pushes tuples to children (compiled style)
void compiled_push_pipeline() {
    scan([&](Tuple& t) {
        if (filter(t)) {
            project(t, [&](Tuple& result) {
                output.emit(result);
            });
        }
    });
}
```

**Data-Centric Compilation:**
```cpp
// Generate code that operates on columnar data structures
std::string generate_vectorized_code(Query& query) {
    return R"(
        void execute(int64_t* col1, int64_t* col2, int64_t* output, size_t n) {
            size_t out_idx = 0;
            for (size_t i = 0; i < n; i++) {
                if (col1[i] > 100) {  // Filter
                    output[out_idx++] = col1[i] + col2[i];  // Project
                }
            }
        }
    )";
}
```

**Adaptive Compilation:**
```cpp
class AdaptiveExecutor {
    static constexpr int COMPILE_THRESHOLD = 5;

    void execute(Query& query) {
        query.execution_count++;

        if (query.execution_count < COMPILE_THRESHOLD) {
            // Interpret for first few executions
            interpret(query);
        } else if (!query.is_compiled) {
            // Compile after threshold
            query.compiled_func = compile(query);
            query.is_compiled = true;
        }

        if (query.is_compiled) {
            query.compiled_func();
        }
    }
};
```

**Expression Compilation:**
```cpp
// Generate specialized code for expressions
class ExpressionCompiler {
    using ExprFunc = int64_t(*)(Tuple&);

    ExprFunc compile_expression(Expression& expr) {
        std::string code = "int64_t eval(Tuple& t) { return ";
        code += generate_expr_code(expr);
        code += "; }";

        // Compile and load
        return compile_and_load(code);
    }

    std::string generate_expr_code(Expression& expr) {
        if (expr.type == COLUMN_REF) {
            return "t.get_col(" + std::to_string(expr.col_idx) + ")";
        } else if (expr.type == BINARY_OP) {
            return "(" + generate_expr_code(expr.left) + " " +
                   expr.op + " " + generate_expr_code(expr.right) + ")";
        }
        // ... handle other expression types
    }
};
```

## Performance Characteristics
- **Speedup:** 2-10x vs interpreted execution for CPU-bound queries
- **Compilation overhead:** 10-100ms per query (LLVM), 1-5ms (template instantiation)
- **Memory overhead:** Generated code size 10-100 KB per query
- **Cache behavior:** Tight loops improve instruction cache hit rate
- **Best for:** Queries running >100ms where compilation cost <5% of execution

## Real-World Examples

**HyPer/Umbra:** Full LLVM compilation, entire query as single C function
```cpp
// Generated code looks like hand-written C
void query_execute(Table& t, Output& out) {
    for (size_t i = 0; i < t.size; i++) {
        if (t.col1[i] > 100) {
            out.append(t.col1[i] + t.col2[i]);
        }
    }
}
```

**PostgreSQL with JIT:** LLVM-based JIT for expression evaluation and tuple deforming

**DuckDB:** Mix of interpretation (complex operators) and template specialization (expressions)

**Spark:** WholeStageCodegen generates Java bytecode for fused pipelines

**ClickHouse:** Primarily interpreted, but uses C++ templates for type-specialized operators

## Pitfalls
- **Short queries:** Compilation overhead dominates for queries <10ms
- **Cold start:** First execution pays compilation cost; bad for interactive workloads
- **Code cache management:** Need to evict compiled code to avoid memory bloat
- **Debugging:** Generated code is hard to debug; need good error messages
- **Complex queries:** Compilation time can exceed execution time for very complex queries
- **Portability:** LLVM adds heavy dependency; template-based approach more portable
- **Optimization time:** Aggressive LLVM optimization (-O3) takes longer but generates faster code
- **ABI compatibility:** Function pointer calls require stable ABI between compiled code and runtime
- **Expression complexity:** Deeply nested expressions can cause compilation failures or slowdowns
