# Pipeline Breakers

## What It Is
Pipeline breakers are operators that require full materialization of input data before producing output (e.g., hash joins, sorts, window functions), breaking the streaming data flow and requiring intermediate storage.

## When To Use
Understanding pipeline breakers is critical for:
- Memory budget planning and spill-to-disk decisions
- Query plan optimization and operator ordering
- Parallelization boundaries and work distribution
- Cache-conscious algorithm selection

## Key Implementation Ideas

**Identifying Pipeline Breakers:**
```cpp
class Operator {
    virtual bool is_pipeline_breaker() const = 0;
    virtual size_t estimated_materialization_size() const = 0;
};

// Pipeline breakers
class HashJoinBuild : public Operator {
    bool is_pipeline_breaker() const override { return true; }
    // Must materialize entire build side into hash table
};

class Sort : public Operator {
    bool is_pipeline_breaker() const override { return true; }
    // Must see all tuples before producing first output
};

// Streaming operators
class Filter : public Operator {
    bool is_pipeline_breaker() const override { return false; }
    // Can produce output immediately
};
```

**Pipeline Segmentation:**
```cpp
struct Pipeline {
    vector<Operator*> operators;
    Operator* source;          // Scan or materialization point
    Operator* sink;            // Materialization point or output
    bool requires_finalize;    // Hash build, sort, etc.
};

vector<Pipeline> split_into_pipelines(PhysicalPlan& plan) {
    vector<Pipeline> pipelines;
    Pipeline current;

    for (auto& op : plan.operators) {
        if (op->is_pipeline_breaker()) {
            current.sink = op;
            pipelines.push_back(current);
            current = Pipeline{.source = op};  // New pipeline
        } else {
            current.operators.push_back(op);
        }
    }

    return pipelines;
}
```

**Hash Table Materialization:**
```cpp
class HashJoinBuild {
    unordered_map<KeyType, vector<Tuple>> hash_table_;

    void Build(DataChunk& input) {
        // Materialize into hash table
        for (size_t i = 0; i < input.size; i++) {
            auto key = extract_key(input, i);
            auto tuple = extract_tuple(input, i);
            hash_table_[key].push_back(tuple);
        }
    }

    void Probe(DataChunk& probe_chunk, DataChunk& output) {
        // Streaming probe phase (not a pipeline breaker)
        for (size_t i = 0; i < probe_chunk.size; i++) {
            auto key = extract_key(probe_chunk, i);
            for (auto& build_tuple : hash_table_[key]) {
                output.append(join(probe_chunk[i], build_tuple));
            }
        }
    }
};
```

**Sort Materialization:**
```cpp
class Sort {
    vector<Tuple> buffer_;

    void Sink(DataChunk& input) {
        // Pipeline breaker: accumulate all input
        buffer_.insert(buffer_.end(), input.begin(), input.end());
    }

    void Finalize() {
        // Sort after seeing all input
        std::sort(buffer_.begin(), buffer_.end(), comparator_);
    }

    void Source(DataChunk& output) {
        // Stream sorted output in next pipeline
        copy_next_chunk(buffer_, output);
    }
};
```

**Memory Management at Boundaries:**
```cpp
class MaterializationBuffer {
    static constexpr size_t MAX_MEMORY = 1ULL << 30;  // 1 GB
    vector<DataChunk> chunks_;
    size_t total_size_ = 0;

    void Add(DataChunk& chunk) {
        if (total_size_ + chunk.memory_size() > MAX_MEMORY) {
            spill_to_disk();
        }
        chunks_.push_back(chunk);
        total_size_ += chunk.memory_size();
    }

    void spill_to_disk() {
        // Write to temporary file
        auto file = open_temp_file();
        for (auto& chunk : chunks_) {
            file.write(chunk);
        }
        chunks_.clear();
        total_size_ = 0;
    }
};
```

**Pipeline Scheduling:**
```cpp
class PipelineScheduler {
    void execute(vector<Pipeline>& pipelines) {
        for (auto& pipeline : pipelines) {
            if (pipeline.requires_finalize) {
                // Build phase (e.g., hash table)
                while (pipeline.source->has_more()) {
                    DataChunk chunk;
                    pipeline.source->GetChunk(chunk);
                    pipeline.sink->Sink(chunk);
                }
                pipeline.sink->Finalize();
            } else {
                // Streaming pipeline
                while (pipeline.source->has_more()) {
                    DataChunk chunk;
                    pipeline.source->GetChunk(chunk);
                    // Process through all operators
                    for (auto& op : pipeline.operators) {
                        op->Execute(chunk);
                    }
                }
            }
        }
    }
};
```

**Avoiding Unnecessary Materialization:**
```cpp
// BAD: Unnecessary materialization
vector<Tuple> sorted = sort(scan());
vector<Tuple> limited = take_top_k(sorted, 100);

// GOOD: Top-K heap (no full sort)
class TopK {
    priority_queue<Tuple> heap_;
    size_t k_;

    void Process(DataChunk& input) {
        for (auto& tuple : input) {
            if (heap_.size() < k_) {
                heap_.push(tuple);
            } else if (tuple < heap_.top()) {
                heap_.pop();
                heap_.push(tuple);
            }
        }
    }
};
```

## Performance Characteristics
- **Memory overhead:** Pipeline breakers can require 2-100x more memory than streaming
- **Cache behavior:** Materialization point flushes cache; subsequent pipeline starts cold
- **Parallelism:** Natural parallelization boundaries (build vs probe, sort vs merge)
- **Latency:** First output delayed until entire input consumed
- **Disk I/O:** Large materializations trigger spilling, adding 10-100x slowdown

## Real-World Examples

**DuckDB:** Clear pipeline boundaries, explicit build/probe phases
```cpp
// Build pipeline: scan → project → hash_build
// Probe pipeline: scan → hash_probe → output
```

**PostgreSQL:** Tuple-at-a-time with explicit materialization for sorts/hashes

**Spark:** Stage boundaries at shuffles (equivalent to pipeline breakers)

**ClickHouse:** Block-based processing, minimal materialization except for joins/aggregations

## Pitfalls
- **Memory explosion:** Multiple hash joins in sequence consume memory multiplicatively
- **Poor ordering:** Building hash table on large side wastes memory; should build on small side
- **Unnecessary sorts:** Don't sort if downstream operator doesn't need order
- **No spilling:** Systems without spill-to-disk fail on large joins; need grace hash join or external sort
- **Cache thrashing:** Very large hash tables (>L3 cache) suffer random access penalties
- **Lost parallelism:** Pipeline breakers serialize execution if not carefully managed
- **Over-materialization:** Materializing intermediate results that could be streamed
- **Blocking aggregations:** Hash aggregations with high cardinality require materialization; consider two-phase aggregation
