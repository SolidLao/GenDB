#!/usr/bin/env node
/**
 * GenDB Demo — VIZ Data Generator
 *
 * Generates visualization data objects for the demo app by extracting data
 * from GenDB output files and using an LLM to produce the creative parts.
 *
 * Usage:
 *   node src/demo/generate-viz.mjs [--model MODEL] [--benchmark BENCH] [--query QID] [--dry-run]
 *
 * Examples:
 *   node src/demo/generate-viz.mjs                          # all queries
 *   node src/demo/generate-viz.mjs --benchmark tpch         # TPC-H only
 *   node src/demo/generate-viz.mjs --query Q1               # single query (across benchmarks)
 *   node src/demo/generate-viz.mjs --benchmark secedgar --query Q6
 */

import { query } from '@anthropic-ai/claude-agent-sdk';
import { extractQueryContext } from './viz-agent/extract.mjs';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, '..', '..');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const BENCHMARKS = {
  tpch: {
    outputDir: join(ROOT, 'output/tpc-h-sf10-3.19'),
    runTimestamp: '2026-03-19T22-20-41',
    benchmarkDir: join(ROOT, 'benchmarks/tpc-h'),
    sfDir: 'sf10',
    queries: ['Q1', 'Q3', 'Q6', 'Q9', 'Q18']
  },
  secedgar: {
    outputDir: join(ROOT, 'output/sec-edgar-sf3-3.19'),
    runTimestamp: '2026-03-20T20-15-08',
    benchmarkDir: join(ROOT, 'benchmarks/sec-edgar'),
    sfDir: 'sf3',
    queries: ['Q1', 'Q2', 'Q3', 'Q4', 'Q6', 'Q24']
  }
};

const DEFAULT_MODEL = 'claude-sonnet-4-6';
const MAX_CONCURRENT = 5;  // parallel LLM calls

// Skip Q3 TPC-H — hand-crafted Q3_VIZ already exists
const SKIP = new Set(['tpch:Q3']);

// ---------------------------------------------------------------------------
// Parse CLI args
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { model: DEFAULT_MODEL, benchmark: null, query: null, dryRun: false, verbose: false };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--model' && args[i + 1]) opts.model = args[++i];
    else if (args[i] === '--benchmark' && args[i + 1]) opts.benchmark = args[++i];
    else if (args[i] === '--query' && args[i + 1]) opts.query = args[++i];
    else if (args[i] === '--dry-run') opts.dryRun = true;
    else if (args[i] === '--verbose' || args[i] === '-v') opts.verbose = true;
  }
  return opts;
}

// ---------------------------------------------------------------------------
// Read workload/storage data (shared per benchmark)
// ---------------------------------------------------------------------------

function readJSON(p) {
  try { return JSON.parse(readFileSync(p, 'utf-8')); } catch { return null; }
}

// ---------------------------------------------------------------------------
// Build user prompt from extracted context
// ---------------------------------------------------------------------------

function buildUserPrompt(ctx) {
  const lines = [];
  lines.push(`Generate the VIZ object for **${ctx.benchmark.toUpperCase()} ${ctx.queryId}**: "${ctx.title}"\n`);

  // Pre-computed values (MUST be used exactly)
  lines.push(`## Pre-computed Values (USE EXACTLY — copy these verbatim into your output)`);
  lines.push('```json');
  lines.push(JSON.stringify({
    benchmark: {
      queryMs: ctx.queryBenchmark,
      gendbSpeedups: ctx.speedups,
      generation: ctx.generation
    },
    speedup: ctx.speedup
  }, null, 2));
  lines.push('```\n');

  // SQL template
  lines.push(`## SQL Template`);
  lines.push('```sql');
  lines.push(ctx.sql);
  lines.push('```\n');

  // Parameters
  lines.push(`## Query Parameters`);
  if (ctx.params.length === 0) {
    lines.push(`No parameters (static query).\n`);
  } else {
    lines.push('```json');
    lines.push(JSON.stringify(ctx.params, null, 2));
    lines.push('```\n');
  }

  // Full workload analysis (let the LLM read and understand all tables, joins, filters)
  lines.push(`## Workload Analysis (full)`);
  lines.push('```json');
  lines.push(JSON.stringify(ctx.workloadAnalysis, null, 2));
  lines.push('```\n');

  // Full storage design (let the LLM read encodings, indexes, shared dicts)
  lines.push(`## Storage Design (full)`);
  lines.push('```json');
  lines.push(JSON.stringify(ctx.storageDesign, null, 2));
  lines.push('```\n');

  // Execution plan
  if (ctx.plan) {
    lines.push(`## Execution Plan (plan.json)`);
    lines.push('```json');
    lines.push(JSON.stringify(ctx.plan, null, 2));
    lines.push('```\n');
  }

  // Optimization history
  lines.push(`## Optimization History`);
  for (const it of ctx.optimizationHistory) {
    lines.push(`### Iteration ${it.iteration} — ${Math.round(it.timing_ms)}ms (${it.validation})`);
    lines.push(`Strategy: ${it.strategy}`);
    lines.push(`Operation timings: ${JSON.stringify(it.operation_timings)}`);
    lines.push('');
  }

  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Call LLM and parse response
// ---------------------------------------------------------------------------

async function generateVizForQuery(systemPrompt, ctx, model, verbose) {
  const userPrompt = buildUserPrompt(ctx);
  const key = `${ctx.benchmark}:${ctx.queryId}`;

  if (verbose) {
    console.log(`\n--- User prompt for ${key} (${userPrompt.length} chars) ---`);
    console.log(userPrompt.slice(0, 500) + '...\n');
  }

  const startTime = Date.now();
  console.log(`  [${key}] Calling ${model}...`);

  let resultText = '';
  try {
    const agentQuery = query({
      prompt: userPrompt,
      options: {
        systemPrompt,
        allowedTools: [],  // no tools needed — pure generation
        model: model || undefined,
        cwd: ROOT,
        permissionMode: 'bypassPermissions',
        allowDangerouslySkipPermissions: true,
        maxOutputTokens: 16000,
      },
    });

    for await (const message of agentQuery) {
      if (message.type === 'assistant') {
        for (const block of message.message.content) {
          if (block.type === 'text' && block.text) {
            resultText += block.text;
          }
        }
      }
      if (message.type === 'result') {
        if (message.subtype === 'success') {
          resultText = message.result || resultText;
        } else {
          const err = message.errors?.join('; ') || message.subtype;
          console.error(`  [${key}] Agent error: ${err}`);
          return null;
        }
      }
    }
  } catch (err) {
    console.error(`  [${key}] ERROR: ${err.message}`);
    return null;
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  [${key}] Done in ${elapsed}s`);

  // Extract JSON from code block in the result text
  const match = resultText.match(/```(?:json|javascript|js)?\s*\n([\s\S]*?)```/);
  if (!match) {
    console.error(`  [${key}] ERROR: No code block found in response`);
    if (verbose) console.error(resultText.slice(0, 500));
    return null;
  }

  let jsonStr = match[1].trim();
  // Strip var declaration if present
  jsonStr = jsonStr.replace(/^var\s+\w+\s*=\s*/, '').replace(/;\s*$/, '');

  let vizObj;
  try {
    vizObj = JSON.parse(jsonStr);
  } catch (e) {
    console.error(`  [${key}] ERROR: JSON parse failed: ${e.message}`);
    if (verbose) console.error(jsonStr.slice(0, 500));
    return null;
  }

  // Validate required fields
  const required = ['version', 'queryId', 'title', 'canvas', 'agents', 'benchmark'];
  for (const field of required) {
    if (!(field in vizObj)) {
      console.error(`  [${key}] WARNING: Missing required field '${field}'`);
    }
  }

  return { key, vizObj };
}

// ---------------------------------------------------------------------------
// Concurrency limiter
// ---------------------------------------------------------------------------

async function runWithConcurrency(tasks, maxConcurrent) {
  const results = [];
  let idx = 0;

  async function worker() {
    while (idx < tasks.length) {
      const i = idx++;
      results[i] = await tasks[i]();
    }
  }

  const workers = Array.from({ length: Math.min(maxConcurrent, tasks.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs();
  console.log(`GenDB Demo VIZ Generator`);
  console.log(`  Model: ${opts.model}`);
  console.log(`  Benchmark: ${opts.benchmark || 'all'}`);
  console.log(`  Query: ${opts.query || 'all'}`);
  console.log(`  Dry run: ${opts.dryRun}\n`);

  // Read system prompt
  const systemPrompt = readFileSync(join(__dirname, 'viz-agent', 'prompt.md'), 'utf-8');

  // Build task list
  const tasks = [];
  for (const [benchKey, config] of Object.entries(BENCHMARKS)) {
    if (opts.benchmark && opts.benchmark !== benchKey) continue;

    const workloadAnalysis = readJSON(join(config.outputDir, 'workload_analysis.json'));
    const storageDesign = readJSON(join(config.outputDir, 'storage_design.json'));
    const runDir = join(config.outputDir, 'runs', config.runTimestamp);

    if (!workloadAnalysis || !storageDesign) {
      console.error(`  Skipping ${benchKey}: missing workload_analysis.json or storage_design.json`);
      continue;
    }

    for (const queryId of config.queries) {
      if (opts.query && opts.query !== queryId) continue;
      const key = `${benchKey}:${queryId}`;
      if (SKIP.has(key)) {
        console.log(`  Skipping ${key} (hand-crafted)`);
        continue;
      }

      const ctx = extractQueryContext({
        benchmark: benchKey, queryId,
        outputDir: config.outputDir,
        runDir,
        benchmarkDir: config.benchmarkDir,
        sfDir: config.sfDir,
        workloadAnalysis,
        storageDesign
      });

      tasks.push({ key, ctx });
    }
  }

  console.log(`\n  ${tasks.length} queries to generate\n`);

  if (opts.dryRun) {
    for (const { key, ctx } of tasks) {
      const prompt = buildUserPrompt(ctx);
      console.log(`  [${key}] "${ctx.title}"`);
      console.log(`    Benchmark: ${ctx.queryBenchmark.map(b => `${b.name}=${b.ms}ms`).join(', ')}`);
      console.log(`    Speedup: ${ctx.speedup ? `${ctx.speedup.from}ms → ${ctx.speedup.to}ms (${ctx.speedup.factor}x, ${ctx.speedup.iters} iters)` : 'none'}`);
      console.log(`    Generation: ${ctx.generation.timeSec}s, $${ctx.generation.costUsd}, ${ctx.generation.linesOfCode} LOC`);
      console.log(`    Prompt size: ${(prompt.length / 1024).toFixed(1)} KB\n`);
    }
    console.log('Dry run complete. Use without --dry-run to generate.');
    return;
  }

  // Run LLM calls with concurrency limit
  const llmTasks = tasks.map(({ key, ctx }) =>
    () => generateVizForQuery(systemPrompt, ctx, opts.model, opts.verbose)
  );
  const results = await runWithConcurrency(llmTasks, MAX_CONCURRENT);

  // Collect successful results
  const vizEntries = {};
  let successCount = 0, failCount = 0;
  for (const result of results) {
    if (result && result.vizObj) {
      vizEntries[result.key] = result.vizObj;
      successCount++;
    } else {
      failCount++;
    }
  }

  console.log(`\n  Results: ${successCount} success, ${failCount} failed\n`);

  if (successCount === 0) {
    console.error('No VIZ objects generated. Exiting.');
    process.exit(1);
  }

  // Write output file
  const outputPath = join(ROOT, 'docs/demo/js/data/generated-viz.js');
  mkdirSync(dirname(outputPath), { recursive: true });

  // Build JS file with var declaration
  const entries = Object.entries(vizEntries)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, obj]) => `  ${JSON.stringify(key)}: ${JSON.stringify(obj, null, 2).split('\n').map((l, i) => i === 0 ? l : '  ' + l).join('\n')}`)
    .join(',\n');

  const output = `// Auto-generated by src/demo/generate-viz.mjs — do not edit manually\nvar GENERATED_VIZ = {\n${entries}\n};\n`;
  writeFileSync(outputPath, output);

  const sizeMB = (output.length / 1024 / 1024).toFixed(1);
  console.log(`  Wrote ${outputPath} (${sizeMB} MB, ${Object.keys(vizEntries).length} queries)`);
  console.log('\nDone!');
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
