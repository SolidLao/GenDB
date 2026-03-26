#!/usr/bin/env node
/**
 * Extract GenDB run data into JS bundles for the demo app.
 * Usage: node src/demo/extract-data.mjs
 */
import { readFileSync, writeFileSync, readdirSync, existsSync, statSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';

const ROOT = dirname(dirname(dirname(import.meta.url.replace('file://', ''))));

function readJSON(path) {
  try { return JSON.parse(readFileSync(path, 'utf-8')); } catch { return null; }
}
function readText(path) {
  try { return readFileSync(path, 'utf-8'); } catch { return null; }
}

function findRunDirs(outputDir) {
  const runsDir = join(outputDir, 'runs');
  if (!existsSync(runsDir)) return [];
  return readdirSync(runsDir)
    .filter(d => d.match(/^\d{4}-/) && statSync(join(runsDir, d)).isDirectory())
    .sort()
    .map(d => join(runsDir, d));
}

function extractQueryFromRuns(runDirs, queryId) {
  // Find which run directory contains this query
  for (const runDir of runDirs) {
    const qDir = join(runDir, 'queries', queryId);
    if (!existsSync(qDir)) continue;

    const optimizationHistory = readJSON(join(qDir, 'optimization_history.json'));
    const memoryMatch = readJSON(join(qDir, 'memory_match.json'));

    // Extract per-iteration data
    const iterations = [];
    const iterDirs = readdirSync(qDir)
      .filter(d => d.startsWith('iter_') && statSync(join(qDir, d)).isDirectory())
      .sort((a, b) => parseInt(a.split('_')[1]) - parseInt(b.split('_')[1]));

    for (const iterDir of iterDirs) {
      const iterPath = join(qDir, iterDir);
      const plan = readJSON(join(iterPath, 'plan.json'));
      const cppFiles = readdirSync(iterPath).filter(f => f.endsWith('.cpp'));
      const code = cppFiles.length > 0 ? readText(join(iterPath, cppFiles[0])) : null;
      const execResults = readJSON(join(iterPath, 'execution_results.json'));
      iterations.push({
        iteration: parseInt(iterDir.split('_')[1]),
        plan,
        code,
        executionResults: execResults
      });
    }

    return { optimizationHistory, memoryMatch, iterations };
  }
  return null;
}

function extractBenchmark(config) {
  const { name, outputDir, benchmarkDir, sfDir } = config;
  console.log(`\nExtracting ${name}...`);

  // Pipeline-level data
  const workloadAnalysis = readJSON(join(outputDir, 'workload_analysis.json'));
  const storageDesign = readJSON(join(outputDir, 'storage_design.json'));
  const schema = readText(join(benchmarkDir, 'schema.sql'));
  const queriesSQL = readText(join(benchmarkDir, 'queries.sql'));

  // Find all run directories
  const runDirs = findRunDirs(outputDir);
  console.log(`  Found ${runDirs.length} run(s)`);

  // Per-query data from top-level queries/ dir + runs/
  const queriesDir = join(outputDir, 'queries');
  const queries = {};

  if (existsSync(queriesDir)) {
    const queryIds = readdirSync(queriesDir)
      .filter(d => statSync(join(queriesDir, d)).isDirectory())
      .sort((a, b) => {
        const na = parseInt(a.replace(/\D/g, '')), nb = parseInt(b.replace(/\D/g, ''));
        return na - nb;
      });

    for (const qId of queryIds) {
      const qDir = join(queriesDir, qId);
      const template = readText(join(qDir, 'template.sql'));
      const params = readJSON(join(qDir, 'params.json'));
      const guide = readText(join(qDir, 'guide.md'));

      // Best version code
      const bestDir = join(qDir, 'best');
      let bestCode = null, bestMeta = null;
      if (existsSync(bestDir)) {
        const cppFiles = readdirSync(bestDir).filter(f => f.endsWith('.cpp'));
        if (cppFiles.length > 0) bestCode = readText(join(bestDir, cppFiles[0]));
        bestMeta = readJSON(join(bestDir, 'meta.json'));
      }

      // Run-level data (iterations, plans, code per iteration)
      const runData = extractQueryFromRuns(runDirs, qId);

      queries[qId] = {
        template,
        params,
        guide,
        bestCode,
        bestMeta,
        ...(runData || {})
      };

      const iterCount = runData?.iterations?.length || 0;
      console.log(`  ${qId}: ${iterCount} iteration(s), plan: ${runData?.iterations?.[0]?.plan ? 'yes' : 'no'}`);
    }
  }

  // Benchmark results
  const metricsDir = join(benchmarkDir, 'results', sfDir, 'metrics');
  const benchmarkResults = readJSON(join(metricsDir, 'benchmark_results.json'));
  const iterationHistory = readJSON(join(metricsDir, 'gendb_iteration_history.json'));

  return {
    name,
    workloadAnalysis,
    storageDesign,
    schema,
    queriesSQL,
    queries,
    benchmarkResults,
    iterationHistory
  };
}

// Configuration for both benchmarks
const configs = [
  {
    name: 'TPC-H SF10',
    varName: 'TPCH_DATA',
    outputDir: join(ROOT, 'output/tpc-h-sf10-3.19'),
    benchmarkDir: join(ROOT, 'benchmarks/tpc-h'),
    sfDir: 'sf10',
    outputFile: join(ROOT, 'docs/demo/js/data/tpc-h-sf10.js')
  },
  {
    name: 'SEC-EDGAR SF3',
    varName: 'SECEDGAR_DATA',
    outputDir: join(ROOT, 'output/sec-edgar-sf3-3.19'),
    benchmarkDir: join(ROOT, 'benchmarks/sec-edgar'),
    sfDir: 'sf3',
    outputFile: join(ROOT, 'docs/demo/js/data/sec-edgar-sf3.js')
  }
];

for (const config of configs) {
  const data = extractBenchmark(config);
  mkdirSync(dirname(config.outputFile), { recursive: true });
  const js = `// Auto-generated by src/demo/extract-data.mjs — do not edit manually\nconst ${config.varName} = ${JSON.stringify(data, null, 2)};\n`;
  writeFileSync(config.outputFile, js);
  const sizeMB = (js.length / 1024 / 1024).toFixed(1);
  console.log(`  Wrote ${config.outputFile} (${sizeMB} MB)`);
}

console.log('\nDone!');
