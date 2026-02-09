# Join Ordering

## What It Is
Join ordering determines the sequence of binary joins to evaluate a multi-way join query. Optimal ordering minimizes intermediate result sizes, reduces total work, and enables predicate pushdown. This is a combinatorial optimization problem with exponential search space.

## When To Use
- Queries with 3+ tables being joined
- Star schemas (fact table + dimension tables): dimension filters reduce fact table early
- Snowflake schemas with long join chains
- Complex queries where naive left-to-right ordering produces huge intermediates
- OLAP workloads with selective filters on dimension tables

## Key Implementation Ideas

### Cardinality-Based Greedy Heuristic
```cpp
// Simple greedy: always join the two relations with smallest combined cardinality
std::vector<Relation*> GreedyJoinOrder(std::vector<Relation*> relations) {
    std::vector<Relation*> order;

    while (relations.size() > 1) {
        // Find pair with smallest intermediate size
        size_t best_i = 0, best_j = 1;
        size_t min_cost = EstimateJoinSize(relations[0], relations[1]);

        for (size_t i = 0; i < relations.size(); i++) {
            for (size_t j = i + 1; j < relations.size(); j++) {
                size_t cost = EstimateJoinSize(relations[i], relations[j]);
                if (cost < min_cost) {
                    min_cost = cost;
                    best_i = i;
                    best_j = j;
                }
            }
        }

        // Join best pair, replace with intermediate
        auto intermediate = CreateJoin(relations[best_i], relations[best_j]);
        relations.erase(relations.begin() + best_j);
        relations[best_i] = intermediate;
    }

    return order;
}
```

### Dynamic Programming for Optimal Ordering
```cpp
// Exact algorithm for small number of relations (n <= 12)
struct JoinPlan {
    double cost;
    std::vector<int> order;
};

std::unordered_map<std::set<int>, JoinPlan> dp_table;

JoinPlan OptimalJoinOrder(std::set<int> relations) {
    if (relations.size() == 1) {
        return {0.0, {*relations.begin()}};
    }

    if (dp_table.count(relations)) {
        return dp_table[relations];
    }

    JoinPlan best_plan = {INFINITY, {}};

    // Try all ways to split relations into two subsets
    for (auto split : EnumerateBipartitions(relations)) {
        auto left_plan = OptimalJoinOrder(split.first);
        auto right_plan = OptimalJoinOrder(split.second);

        double join_cost = EstimateJoinCost(split.first, split.second);
        double total_cost = left_plan.cost + right_plan.cost + join_cost;

        if (total_cost < best_plan.cost) {
            best_plan.cost = total_cost;
            best_plan.order = MergePlans(left_plan, right_plan);
        }
    }

    dp_table[relations] = best_plan;
    return best_plan;
}
```

### Filter Pushdown Before Joins
```cpp
// Apply selective filters before joining to reduce input sizes
void PushdownFilters(QueryPlan& plan) {
    for (auto& filter : plan.filters) {
        // Identify which relation(s) the filter applies to
        auto applicable_rels = IdentifyRelations(filter);

        if (applicable_rels.size() == 1) {
            // Push filter down to base relation scan
            auto rel = applicable_rels[0];
            rel->AddFilter(filter);

            // Update cardinality estimate
            rel->UpdateCardinality(EstimateSelectivity(filter));
        }
    }
}
```

### Left-Deep vs Bushy Trees
```cpp
// Left-deep tree: fully pipelined, no materialization
//     Join
//    /    \
//  Join    R3
//  /  \
// R1  R2

// Bushy tree: allows parallelism, may require materialization
//       Join
//      /    \
//   Join    Join
//   /  \    /  \
//  R1  R2  R3  R4

// Heuristic: Use bushy trees for independent subtrees
bool ShouldUseBushyTree(std::vector<Relation*> relations) {
    // Check if query has independent join groups
    auto join_graph = BuildJoinGraph(relations);
    return HasDisconnectedComponents(join_graph);
}
```

### Selectivity Estimation
```cpp
// Estimate intermediate result size using selectivity
size_t EstimateJoinSize(Relation* left, Relation* right, JoinCondition cond) {
    // Base cardinality
    size_t left_card = left->GetCardinality();
    size_t right_card = right->GetCardinality();

    // Foreign key join: result size = right cardinality
    if (IsForeignKeyJoin(left, right, cond)) {
        return right_card;
    }

    // General case: use column statistics
    double selectivity = 1.0 / std::max(
        left->GetDistinctCount(cond.left_col),
        right->GetDistinctCount(cond.right_col)
    );

    return static_cast<size_t>(left_card * right_card * selectivity);
}
```

## Performance Characteristics
- Optimal ordering can reduce query time by 10-1000x for complex queries
- Dynamic programming: O(3^n) time, practical for n <= 12 relations
- Greedy heuristic: O(n^3) time, within 2-5x of optimal in practice
- Star schema with pushed filters: 100x+ speedup by filtering dimensions first
- Poor ordering: Intermediate results explode, exceed memory, spill to disk

## Real-World Examples
- **PostgreSQL**: Uses dynamic programming for <= 12 relations, GEQO genetic algorithm for larger queries
- **DuckDB**: Hyper-style graph-based optimizer with filter pushdown, left-deep bias for pipelining
- **SQL Server**: Greedy heuristic with cardinality estimates, parallel bushy trees for large queries
- **Oracle**: Cost-based optimizer with histograms, considers left-deep and bushy plans

## Pitfalls
- **Stale statistics**: Outdated cardinality estimates lead to catastrophic join ordering
- **Ignoring correlation**: Assumes independence between predicates, can underestimate selectivity by 10-100x
- **No feedback loop**: Execute bad plan repeatedly without learning, implement adaptive re-optimization
- **Over-optimization**: Spending seconds optimizing millisecond query, limit optimizer time budget
- **Forgetting cross products**: Queries without join conditions can produce trillion-row intermediates, detect and warn
- **Bushy tree overhead**: Materializing intermediates for parallelism may cost more than pipelining
