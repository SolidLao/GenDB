import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';
import { readFile, mkdir } from 'fs/promises';
import { existsSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT = resolve(__dirname, '../..');
const PORT = process.env.PORT || 3001;

const app = express();
app.use(cors());
app.use(express.json());

// File uploads
const uploadDir = join(ROOT, 'tmp', 'uploads');
await mkdir(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

// Static serving
app.use('/demo', express.static(join(ROOT, 'docs', 'demo')));
app.use('/docs', express.static(join(ROOT, 'docs')));
app.use('/', express.static(join(ROOT, 'docs', 'demo')));

// Single concurrent generation guard
let activeRun = null;

app.get('/api/status', (_req, res) => {
  res.json({ online: true, version: '1.0' });
});

app.post('/api/generate', (req, res) => {
  if (activeRun) {
    return res.status(409).json({ error: 'A generation is already in progress', activeRunId: activeRun.runId });
  }

  const { mode, benchmark, sf, query, provider, model, dataDir, schemaPath, queryPath } = req.body;
  if (!mode || !['existing', 'custom'].includes(mode)) {
    return res.status(400).json({ error: 'Invalid mode. Use "existing" or "custom".' });
  }

  const runId = Date.now().toString();
  const outDir = join(ROOT, 'runs', runId);

  // Build command args
  const useMultiAgent = mode === 'existing' && !query;
  const script = useMultiAgent ? 'src/gendb/orchestrator.mjs' : 'src/gendb/single.mjs';
  const args = [script];

  if (mode === 'existing') {
    if (benchmark) args.push('--benchmark', benchmark);
    if (sf) args.push('--sf', String(sf));
    if (query) args.push('--query', query);
  } else {
    if (dataDir) args.push('--data-dir', dataDir);
    if (schemaPath) args.push('--schema', schemaPath);
    if (queryPath) args.push('--query', queryPath);
  }

  if (provider) args.push('--provider', provider);
  if (model) args.push('--model', model);
  args.push('--output-dir', outDir);

  const child = spawn('node', args, { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });

  let log = '';
  child.stdout.on('data', (d) => { log += d.toString(); });
  child.stderr.on('data', (d) => { log += d.toString(); });

  activeRun = { runId, outDir, child, startedAt: Date.now(), log: () => log };

  child.on('close', (code) => {
    if (activeRun?.runId === runId) activeRun = null;
    console.log(`Run ${runId} finished with code ${code}`);
  });

  res.json({ runId });
});

app.get('/api/status/:runId', async (req, res) => {
  const { runId } = req.params;
  const outDir = join(ROOT, 'runs', runId);
  const runJsonPath = join(outDir, 'run.json');

  if (!existsSync(outDir)) {
    return res.status(404).json({ error: 'Run not found' });
  }

  try {
    if (existsSync(runJsonPath)) {
      const data = JSON.parse(await readFile(runJsonPath, 'utf-8'));
      const isActive = activeRun?.runId === runId;
      return res.json({ runId, active: isActive, ...data });
    }
    // run.json not yet written — still initializing
    return res.json({ runId, active: activeRun?.runId === runId, phase: 'initializing', progress: 0 });
  } catch (err) {
    res.status(500).json({ error: 'Failed to read run status', details: err.message });
  }
});

app.get('/api/results/:runId', async (req, res) => {
  const { runId } = req.params;
  const outDir = join(ROOT, 'runs', runId);

  if (!existsSync(outDir)) {
    return res.status(404).json({ error: 'Run not found' });
  }

  const results = {};
  const tryRead = async (key, filename) => {
    const p = join(outDir, filename);
    if (existsSync(p)) {
      try { results[key] = JSON.parse(await readFile(p, 'utf-8')); } catch { /* skip */ }
    }
  };

  await Promise.all([
    tryRead('workloadAnalysis', 'workload_analysis.json'),
    tryRead('storageDesign', 'storage_design.json'),
    tryRead('run', 'run.json'),
  ]);

  // Collect per-query results
  const queriesDir = join(outDir, 'queries');
  if (existsSync(queriesDir)) {
    const { readdirSync } = await import('fs');
    const queryDirs = readdirSync(queriesDir, { withFileTypes: true })
      .filter(d => d.isDirectory())
      .map(d => d.name);

    results.queries = {};
    for (const qd of queryDirs) {
      const qPath = join(queriesDir, qd);
      const queryResult = {};
      await Promise.all([
        (async () => {
          const planPath = join(qPath, 'plan.json');
          if (existsSync(planPath)) queryResult.plan = JSON.parse(await readFile(planPath, 'utf-8'));
        })(),
        (async () => {
          const codePath = join(qPath, 'query.cpp');
          if (existsSync(codePath)) queryResult.code = await readFile(codePath, 'utf-8');
        })(),
        (async () => {
          const metricsPath = join(qPath, 'metrics.json');
          if (existsSync(metricsPath)) queryResult.metrics = JSON.parse(await readFile(metricsPath, 'utf-8'));
        })(),
      ]);
      results.queries[qd] = queryResult;
    }
  }

  res.json({ runId, results });
});

app.post('/api/upload', upload.array('files', 20), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }

  const files = req.files.map(f => ({
    originalName: f.originalname,
    path: f.path,
    size: f.size,
    mimetype: f.mimetype,
  }));

  res.json({ files });
});

app.listen(PORT, () => {
  console.log(`GenDB demo server running on http://localhost:${PORT}`);
  console.log(`  Demo UI:  http://localhost:${PORT}/demo/`);
  console.log(`  API:      http://localhost:${PORT}/api/status`);
});
