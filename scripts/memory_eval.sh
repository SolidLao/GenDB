#!/usr/bin/env bash
#
# Memory System v3 Evaluation Script
#
# Four experiments to measure memory system effectiveness:
#
#   Exp 1 — Baseline TPC-H (no memory):
#     Run all 22 TPC-H queries with no memory system.
#     Establishes baseline cost, timing, and iteration counts.
#
#   Exp 2 — Baseline SEC-EDGAR (no memory):
#     Run all 6 SEC-EDGAR queries with no memory system.
#     Establishes baseline for cross-benchmark comparison.
#
#   Exp 3 — TPC-H with memory (train 5Q → infer 22Q):
#     Phase A: Run 5 training queries (Q1,Q3,Q6,Q9,Q18) to build memory + skills.
#     Phase B: Run all 22 queries with the learned memory.
#     Measures same-schema, query-level knowledge transfer.
#
#   Exp 4 — SEC-EDGAR with TPC-H memory (cross-benchmark transfer):
#     Reuse the memory built in Exp 3 (after all 22 TPC-H queries) to run SEC-EDGAR.
#     Measures whether knowledge transfers across completely different schemas.
#
# Prerequisites:
#   - TPC-H SF10 data: benchmarks/tpc-h/data/sf10/
#   - SEC-EDGAR SF3 data: benchmarks/sec-edgar/data/sf3/
#   - Node.js dependencies installed (npm install)
#   - ANTHROPIC_API_KEY set
#
# Usage:
#   bash scripts/memory_eval.sh                    # Run all 4 experiments
#   bash scripts/memory_eval.sh --exp 1            # Run only experiment 1
#   bash scripts/memory_eval.sh --exp 3            # Run only experiment 3
#   bash scripts/memory_eval.sh --exp 3 --phase b  # Run only Exp 3 Phase B (assumes Phase A done)
#   bash scripts/memory_eval.sh --provider codex --model gpt-5.4
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ORCH="$REPO_DIR/src/gendb/orchestrator.mjs"

# --- Configuration ---
TPCH_SF=10
EDGAR_SF=3
TPCH_QUERIES="$REPO_DIR/benchmarks/tpc-h/queries.sql"
TPCH_TRAINING_QUERIES="$REPO_DIR/benchmarks/tpc-h/queries_training.sql"
EDGAR_QUERIES="$REPO_DIR/benchmarks/sec-edgar/queries.sql"
SKILLS_DIR="$REPO_DIR/.claude/skills"

# Eval output root — all experiments live here
EVAL_DIR="$REPO_DIR/output/memory_eval"
LOG_DIR="$EVAL_DIR/logs"

# --- Parse arguments ---
RUN_EXPS=()  # empty = all
PHASE_FILTER=""  # "" = all phases, "a" or "b" = specific phase
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --exp)        RUN_EXPS+=("$2"); shift 2 ;;
    --phase)      PHASE_FILTER="$2"; shift 2 ;;
    --provider)   EXTRA_ARGS="$EXTRA_ARGS --agent-provider $2"; shift 2 ;;
    --model)      EXTRA_ARGS="$EXTRA_ARGS --model $2"; shift 2 ;;
    --tpch-sf)    TPCH_SF=$2; shift 2 ;;
    --edgar-sf)   EDGAR_SF=$2; shift 2 ;;
    *)            EXTRA_ARGS="$EXTRA_ARGS $1"; shift ;;
  esac
done

# Default: run all experiments
if [ ${#RUN_EXPS[@]} -eq 0 ]; then
  RUN_EXPS=(1 2 3 4)
fi

should_run_exp() { for e in "${RUN_EXPS[@]}"; do [[ "$e" == "$1" ]] && return 0; done; return 1; }

mkdir -p "$LOG_DIR"

# --- Helper functions ---
timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(timestamp)] $*"; }

count_skills() {
  find "$SKILLS_DIR" -maxdepth 2 -name "SKILL.md" 2>/dev/null | wc -l
}

print_skills_summary() {
  log "Skills inventory:"
  local skills
  skills=$(find "$SKILLS_DIR" -maxdepth 2 -name "SKILL.md" 2>/dev/null | while read f; do basename "$(dirname "$f")"; done | sort)
  local count
  count=$(echo "$skills" | grep -c . 2>/dev/null || echo 0)
  if [ "$count" -gt 0 ]; then
    log "  ($count skills): $(echo $skills | tr '\n' ' ')"
  else
    log "  (empty)"
  fi
}

print_memory_summary() {
  local mem_dir="$1"
  if [ -f "$mem_dir/graph/index.json" ]; then
    log "Memory graph: $(node -e "
      const idx = JSON.parse(require('fs').readFileSync('$mem_dir/graph/index.json','utf-8'));
      const s = idx.stats?.by_layer || {};
      console.log(idx.stats.total + ' nodes — ' + Object.entries(s).map(([k,v]) => k+':'+v).join(', '));
    " 2>/dev/null || echo 'empty')"
  else
    log "Memory graph: empty"
  fi
}

# Clean skills directory (remove all generated skills, keep base directory)
clean_skills() {
  log "Cleaning skills directory..."
  if [ -d "$SKILLS_DIR" ]; then
    # Remove all skill subdirectories but keep the .claude/skills/ dir itself
    find "$SKILLS_DIR" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
  fi
}

# Ensure the default workload directory does not contain stale data from another experiment.
# The orchestrator auto-creates output/<benchmark>-sf<N>/ — we need it clean for each experiment.
# Strategy: before each run, move any existing workload dir out of the way.
# After the run, move the workload dir to the experiment-specific location.
prepare_workload_dir() {
  local benchmark="$1"
  local sf="$2"
  local workload_dir="$REPO_DIR/output/${benchmark}-sf${sf}"

  if [ -d "$workload_dir" ]; then
    local backup="${workload_dir}.pre-eval-$(date '+%Y%m%dT%H%M%S')"
    log "Moving existing workload dir: $workload_dir → $backup"
    mv "$workload_dir" "$backup"
  fi
}

# After a run, move the workload dir to the experiment output
capture_workload_dir() {
  local benchmark="$1"
  local sf="$2"
  local exp_name="$3"
  local workload_dir="$REPO_DIR/output/${benchmark}-sf${sf}"
  local dest="$EVAL_DIR/$exp_name/workload"

  if [ -d "$workload_dir" ]; then
    mkdir -p "$EVAL_DIR/$exp_name"
    mv "$workload_dir" "$dest"
    log "Captured workload → $dest"
  fi
}

# Restore a previously captured workload dir so the orchestrator reuses its storage
restore_workload_dir() {
  local benchmark="$1"
  local sf="$2"
  local exp_name="$3"
  local workload_dir="$REPO_DIR/output/${benchmark}-sf${sf}"
  local src="$EVAL_DIR/$exp_name/workload"

  if [ -d "$src" ]; then
    # Move any existing out of the way
    if [ -d "$workload_dir" ]; then
      mv "$workload_dir" "${workload_dir}.tmp-$(date '+%s')"
    fi
    cp -a "$src" "$workload_dir"
    log "Restored workload from $exp_name → $workload_dir"
  fi
}

# Extract key metrics from a run's telemetry and run.json
print_run_results() {
  local workload_dir="$1"
  local label="$2"

  local run_dir="$workload_dir/runs/latest"
  if [ ! -d "$run_dir" ]; then
    log "  [$label] No run found"
    return
  fi

  node -e '
    const fs = require("fs");
    const runDir = process.argv[1];
    const label = process.argv[2];

    const run = JSON.parse(fs.readFileSync(runDir + "/run.json", "utf-8"));
    const tel = JSON.parse(fs.readFileSync(runDir + "/telemetry.json", "utf-8"));

    const pipelines = run.phase2?.pipelines || {};
    const queries = Object.keys(pipelines);
    let totalTiming = 0, totalIters = 0, passCount = 0;

    for (const [qid, p] of Object.entries(pipelines)) {
      if (p.status === "completed") passCount++;
      totalIters += p.iterations || 0;
      const iterMatch = p.bestCppPath?.match(/iter_(\d+)/);
      const iter = iterMatch ? iterMatch[1] : "0";
      try {
        const execPath = runDir + "/queries/" + qid + "/iter_" + iter + "/execution_results.json";
        const exec = JSON.parse(fs.readFileSync(execPath, "utf-8"));
        if (exec.timing_ms) totalTiming += exec.timing_ms;
      } catch {}
    }

    let tiers = { exact: 0, structural: 0, novel: 0, none: 0 };
    try {
      const mem = JSON.parse(fs.readFileSync(runDir + "/memory_report.json", "utf-8"));
      for (const q of Object.values(mem.queries || {})) {
        tiers[q.tier || "none"]++;
      }
    } catch { tiers.none = queries.length; }

    console.log("  [" + label + "] " + queries.length + "Q: " + passCount + "/" + queries.length + " pass, " +
      totalTiming.toFixed(1) + "ms total, " +
      (totalTiming / queries.length).toFixed(1) + "ms avg, " +
      totalIters + " iters, $" + tel.total_cost_usd.toFixed(2) + " cost, " +
      (tel.total_wall_clock_ms / 1000 / 60).toFixed(1) + "min wall");
    if (tiers.exact + tiers.structural > 0) {
      console.log("  [" + label + "] Memory tiers: " + tiers.exact + " exact, " + tiers.structural + " structural, " + tiers.novel + " novel");
    }
  ' "$run_dir" "$label" 2>/dev/null || log "  [$label] Could not parse results"
}

# ============================================================
#  EXPERIMENT 1: Baseline TPC-H (no memory)
# ============================================================
if should_run_exp 1; then
  log ""
  log "============================================================"
  log "  EXPERIMENT 1: Baseline TPC-H — 22 queries, NO memory"
  log "============================================================"

  prepare_workload_dir "tpc-h" "$TPCH_SF"
  clean_skills

  EXP1_LOG="$LOG_DIR/exp1_tpch_baseline_$(date '+%Y%m%dT%H%M%S').log"
  log "Log: $EXP1_LOG"

  EXP1_START=$(date +%s)

  node "$ORCH" \
    --benchmark tpc-h \
    --sf "$TPCH_SF" \
    --queries "$TPCH_QUERIES" \
    --no-memory \
    $EXTRA_ARGS \
    2>&1 | tee "$EXP1_LOG"

  EXP1_END=$(date +%s)
  log "Experiment 1 finished in $(( (EXP1_END - EXP1_START) / 60 ))m"

  capture_workload_dir "tpc-h" "$TPCH_SF" "exp1_tpch_baseline"
  print_run_results "$EVAL_DIR/exp1_tpch_baseline/workload" "Exp1"
  log ""
fi

# ============================================================
#  EXPERIMENT 2: Baseline SEC-EDGAR (no memory)
# ============================================================
if should_run_exp 2; then
  log ""
  log "============================================================"
  log "  EXPERIMENT 2: Baseline SEC-EDGAR — 6 queries, NO memory"
  log "============================================================"

  prepare_workload_dir "sec-edgar" "$EDGAR_SF"
  clean_skills

  EXP2_LOG="$LOG_DIR/exp2_edgar_baseline_$(date '+%Y%m%dT%H%M%S').log"
  log "Log: $EXP2_LOG"

  EXP2_START=$(date +%s)

  node "$ORCH" \
    --benchmark sec-edgar \
    --sf "$EDGAR_SF" \
    --queries "$EDGAR_QUERIES" \
    --no-memory \
    $EXTRA_ARGS \
    2>&1 | tee "$EXP2_LOG"

  EXP2_END=$(date +%s)
  log "Experiment 2 finished in $(( (EXP2_END - EXP2_START) / 60 ))m"

  capture_workload_dir "sec-edgar" "$EDGAR_SF" "exp2_edgar_baseline"
  print_run_results "$EVAL_DIR/exp2_edgar_baseline/workload" "Exp2"
  log ""
fi

# ============================================================
#  EXPERIMENT 3: TPC-H with memory (train 5Q → infer 22Q)
# ============================================================
if should_run_exp 3; then
  EXP3_MEM="$EVAL_DIR/exp3_memory"

  # --- Phase A: Training (5 queries → build memory) ---
  if [ -z "$PHASE_FILTER" ] || [ "$PHASE_FILTER" = "a" ]; then
    log ""
    log "============================================================"
    log "  EXPERIMENT 3A: TPC-H Training — 5 queries, build memory"
    log "  Queries: Q1, Q3, Q6, Q9, Q18"
    log "============================================================"

    prepare_workload_dir "tpc-h" "$TPCH_SF"
    clean_skills
    rm -rf "$EXP3_MEM"

    EXP3A_LOG="$LOG_DIR/exp3a_tpch_train_$(date '+%Y%m%dT%H%M%S').log"
    log "Memory dir: $EXP3_MEM"
    log "Log: $EXP3A_LOG"

    EXP3A_START=$(date +%s)

    node "$ORCH" \
      --benchmark tpc-h \
      --sf "$TPCH_SF" \
      --queries "$TPCH_TRAINING_QUERIES" \
      --memory-dir "$EXP3_MEM" \
      $EXTRA_ARGS \
      2>&1 | tee "$EXP3A_LOG"

    EXP3A_END=$(date +%s)
    log "Experiment 3A finished in $(( (EXP3A_END - EXP3A_START) / 60 ))m"

    SKILLS_AFTER=$(count_skills)
    log "Skills created: $SKILLS_AFTER"
    print_skills_summary
    print_memory_summary "$EXP3_MEM"

    # Save the training workload dir (storage will be reused for inference)
    capture_workload_dir "tpc-h" "$TPCH_SF" "exp3a_tpch_train"
    print_run_results "$EVAL_DIR/exp3a_tpch_train/workload" "Exp3A"
    log ""
  fi

  # --- Phase B: Inference (all 22 queries with memory from Phase A) ---
  if [ -z "$PHASE_FILTER" ] || [ "$PHASE_FILTER" = "b" ]; then
    log ""
    log "============================================================"
    log "  EXPERIMENT 3B: TPC-H Inference — 22 queries, WITH memory"
    log "  Memory from: Exp 3A (5 training queries)"
    log "============================================================"

    # Restore the training workload so storage is reused (no re-ingestion)
    restore_workload_dir "tpc-h" "$TPCH_SF" "exp3a_tpch_train"

    if [ ! -d "$EXP3_MEM" ]; then
      log "ERROR: Memory dir $EXP3_MEM not found. Run Exp 3A first."
      exit 1
    fi

    log "Memory dir: $EXP3_MEM"
    print_memory_summary "$EXP3_MEM"
    log "Skills: $(count_skills)"

    EXP3B_LOG="$LOG_DIR/exp3b_tpch_infer_$(date '+%Y%m%dT%H%M%S').log"
    log "Log: $EXP3B_LOG"

    EXP3B_START=$(date +%s)

    node "$ORCH" \
      --benchmark tpc-h \
      --sf "$TPCH_SF" \
      --queries "$TPCH_QUERIES" \
      --memory-dir "$EXP3_MEM" \
      $EXTRA_ARGS \
      2>&1 | tee "$EXP3B_LOG"

    EXP3B_END=$(date +%s)
    log "Experiment 3B finished in $(( (EXP3B_END - EXP3B_START) / 60 ))m"

    SKILLS_AFTER=$(count_skills)
    log "Skills after inference: $SKILLS_AFTER"
    print_skills_summary
    print_memory_summary "$EXP3_MEM"

    # Capture the full inference workload (includes training + inference runs)
    capture_workload_dir "tpc-h" "$TPCH_SF" "exp3b_tpch_infer"
    print_run_results "$EVAL_DIR/exp3b_tpch_infer/workload" "Exp3B"
    log ""
  fi
fi

# ============================================================
#  EXPERIMENT 4: SEC-EDGAR with TPC-H memory (cross-benchmark)
# ============================================================
if should_run_exp 4; then
  log ""
  log "============================================================"
  log "  EXPERIMENT 4: SEC-EDGAR — 6 queries, WITH TPC-H memory"
  log "  Memory from: Exp 3 (22 TPC-H queries)"
  log "============================================================"

  EXP3_MEM="$EVAL_DIR/exp3_memory"

  if [ ! -d "$EXP3_MEM" ]; then
    log "ERROR: Memory dir $EXP3_MEM not found. Run Exp 3 first."
    exit 1
  fi

  prepare_workload_dir "sec-edgar" "$EDGAR_SF"
  # Keep skills from Exp 3 — they're part of the memory system

  log "Memory dir: $EXP3_MEM"
  print_memory_summary "$EXP3_MEM"
  log "Skills: $(count_skills)"

  EXP4_LOG="$LOG_DIR/exp4_edgar_transfer_$(date '+%Y%m%dT%H%M%S').log"
  log "Log: $EXP4_LOG"

  EXP4_START=$(date +%s)

  node "$ORCH" \
    --benchmark sec-edgar \
    --sf "$EDGAR_SF" \
    --queries "$EDGAR_QUERIES" \
    --memory-dir "$EXP3_MEM" \
    $EXTRA_ARGS \
    2>&1 | tee "$EXP4_LOG"

  EXP4_END=$(date +%s)
  log "Experiment 4 finished in $(( (EXP4_END - EXP4_START) / 60 ))m"

  SKILLS_AFTER=$(count_skills)
  log "Skills after cross-benchmark: $SKILLS_AFTER"
  print_skills_summary
  print_memory_summary "$EXP3_MEM"

  capture_workload_dir "sec-edgar" "$EDGAR_SF" "exp4_edgar_transfer"
  print_run_results "$EVAL_DIR/exp4_edgar_transfer/workload" "Exp4"
  log ""
fi

# ============================================================
#  FINAL COMPARISON SUMMARY
# ============================================================
log ""
log "============================================================"
log "  EVALUATION SUMMARY"
log "============================================================"
log ""

# Print results for all completed experiments
for exp_dir in "$EVAL_DIR"/exp*/; do
  if [ -d "$exp_dir/workload" ]; then
    exp_name=$(basename "$exp_dir")
    print_run_results "$exp_dir/workload" "$exp_name"
  fi
done

log ""
log "Key comparisons:"
log "  Exp1 vs Exp3B → Same-schema transfer: does TPC-H memory help TPC-H queries?"
log "  Exp2 vs Exp4  → Cross-schema transfer: does TPC-H memory help SEC-EDGAR queries?"
log "  Exp3A (5Q training cost) + Exp3B (22Q inference cost) vs Exp1 (22Q baseline cost)"
log ""
log "Artifacts:"
log "  Eval dir:  $EVAL_DIR/"
log "  Logs:      $LOG_DIR/"
log "  Memory:    $EVAL_DIR/exp3_memory/"
log "  Skills:    $SKILLS_DIR/"
log ""
