/**
 * Data extraction for GenDB demo VIZ generation.
 * Reads raw files and computes only truly deterministic values (benchmark numbers,
 * generation stats, speedup). Everything semantic is left to the LLM.
 */
import { readFileSync, existsSync, readdirSync } from 'fs';
import { join } from 'path';

function readJSON(p) {
  try { return JSON.parse(readFileSync(p, 'utf-8')); } catch { return null; }
}
function readText(p) {
  try { return readFileSync(p, 'utf-8'); } catch { return ''; }
}

const QUERY_TITLES = {
  tpch: {
    Q1: 'Pricing Summary Report', Q3: 'Shipping Priority',
    Q6: 'Forecasting Revenue Change', Q9: 'Product Type Profit Measure',
    Q18: 'Large Volume Customer'
  },
  secedgar: {
    Q1: 'Filing Statement Summary', Q2: 'Maximum Value Lookup',
    Q3: 'Top Companies by Value', Q4: 'Industry Sector Analysis',
    Q6: 'Income Statement Detail', Q24: 'Orphaned Financial Tags'
  }
};

const BENCHMARK_SYSTEMS = ['GenDB', 'Umbra', 'DuckDB', 'MonetDB', 'ClickHouse', 'PostgreSQL'];

/**
 * Extract all raw data + compute deterministic numbers for a single query.
 */
export function extractQueryContext({ benchmark, queryId, outputDir, runDir, benchmarkDir, sfDir, workloadAnalysis, storageDesign }) {
  const qDir = join(outputDir, 'queries', queryId);
  const runQDir = join(runDir, 'queries', queryId);

  // --- Raw files ---
  const sql = readText(join(qDir, 'template.sql')).trim();
  const paramsData = readJSON(join(qDir, 'params.json'));
  const params = paramsData?.parameters || [];
  const plan = readJSON(join(runQDir, 'iter_0', 'plan.json'));
  const optHistory = readJSON(join(runQDir, 'optimization_history.json'));
  const iterations = optHistory?.iterations || [];

  // --- Best iteration ---
  const bestIter = iterations.reduce(
    (best, it) => it.validation === 'pass' && (!best || it.timing_ms < best.timing_ms) ? it : best,
    null
  );
  const firstIter = iterations[0] || null;

  // --- C++ line count ---
  let linesOfCode = 0;
  const bestIterNum = bestIter ? bestIter.iteration : 0;
  const bestIterDir = join(runQDir, `iter_${bestIterNum}`);
  if (existsSync(bestIterDir)) {
    const cppFiles = readdirSync(bestIterDir).filter(f => f.endsWith('.cpp'));
    if (cppFiles.length > 0) {
      const code = readText(join(bestIterDir, cppFiles[0]));
      linesOfCode = code ? code.split('\n').length : 0;
    }
  }

  // --- Telemetry (generation cost & time) ---
  const telemetry = readJSON(join(runDir, 'telemetry.json'));
  const qTel = telemetry?.phases?.[`query_${queryId}`];
  let genTimeSec = 0, genCostUsd = 0;
  if (qTel) {
    genTimeSec = Math.round(qTel.total_ms / 1000);
    genCostUsd = Object.values(qTel.agents || {}).reduce((s, a) => s + (a.cost_usd || 0), 0);
  }

  // --- Benchmark comparison (deterministic math) ---
  const benchResults = readJSON(join(benchmarkDir, 'results', sfDir, 'metrics', 'benchmark_results.json'));
  const gendbMs = bestIter ? Math.round(bestIter.timing_ms) : null;
  const queryBenchmark = [];
  for (const sys of BENCHMARK_SYSTEMS) {
    if (sys === 'GenDB' && gendbMs != null) {
      queryBenchmark.push({ name: 'GenDB', ms: gendbMs, highlight: true });
    } else {
      const sd = benchResults?.[sys]?.[queryId];
      if (sd) queryBenchmark.push({ name: sys, ms: Math.round(sd.average_ms) });
    }
  }
  queryBenchmark.sort((a, b) => a.ms - b.ms);

  const gendbEntry = queryBenchmark.find(e => e.name === 'GenDB');
  const speedups = gendbEntry
    ? queryBenchmark.filter(e => e.name !== 'GenDB').map(e => ({
        vs: e.name, factor: +(e.ms / gendbEntry.ms).toFixed(1)
      }))
    : [];

  // --- Self-optimization speedup (deterministic math) ---
  let speedup;
  if (firstIter && bestIter && firstIter.timing_ms !== bestIter.timing_ms) {
    speedup = {
      from: Math.round(firstIter.timing_ms),
      to: Math.round(bestIter.timing_ms),
      factor: +(firstIter.timing_ms / bestIter.timing_ms).toFixed(1),
      iters: iterations.length
    };
  } else if (gendbMs != null) {
    speedup = { from: gendbMs, to: gendbMs, factor: 1.0, iters: iterations.length };
  } else {
    speedup = null;
  }

  return {
    benchmark, queryId,
    title: QUERY_TITLES[benchmark]?.[queryId] || queryId,

    // Raw source data — LLM reads these to understand the query
    sql, params, plan,
    optimizationHistory: iterations,
    workloadAnalysis,   // full workload analysis (all tables, joins, filters)
    storageDesign,      // full storage design (all encodings, indexes)
    hardware: workloadAnalysis.hardware,

    // Pre-computed deterministic values — LLM must copy these verbatim
    queryBenchmark, speedups, speedup,
    generation: {
      timeSec: genTimeSec,
      costUsd: +genCostUsd.toFixed(2),
      iterations: iterations.length,
      linesOfCode
    }
  };
}
