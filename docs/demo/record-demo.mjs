#!/usr/bin/env node

// not working very well: mismatch in audio and animations

/* ============================================================
   GenDB Demo — Video Recording Script

   Strategy: Let the browser's tutorial play audio naturally.
   Intercept Audio.play() to log exact timestamps.
   After recording, place audio files at those timestamps.
   → Perfect sync by construction.

   Usage: node docs/demo/record-demo.mjs
   Output: docs/demo/demo-video.mp4
   ============================================================ */

import puppeteer from 'puppeteer';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEMO_DIR = __dirname;
const AUDIO_DIR = path.join(DEMO_DIR, 'audio');
const OUTPUT_DIR = DEMO_DIR;

const durations = JSON.parse(fs.readFileSync(path.join(AUDIO_DIR, 'durations.json'), 'utf8'));

/* ════════════════════════════════════════════════════════════════
   Static file server
   ════════════════════════════════════════════════════════════════ */
const MIME = {
  '.html':'text/html','.css':'text/css','.js':'application/javascript',
  '.json':'application/json','.png':'image/png','.jpg':'image/jpeg',
  '.svg':'image/svg+xml','.mp3':'audio/mpeg','.woff2':'font/woff2','.ico':'image/x-icon',
};
function startServer(port) {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      let fp = path.join(DEMO_DIR, decodeURIComponent(req.url.split('?')[0]));
      if (fp.endsWith('/')) fp += 'index.html';
      if (!fs.existsSync(fp)) { res.writeHead(404); res.end(); return; }
      if (fs.statSync(fp).isDirectory()) fp = path.join(fp, 'index.html');
      res.writeHead(200, { 'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream' });
      fs.createReadStream(fp).pipe(res);
    });
    srv.listen(port, () => resolve(srv));
  });
}

function delay(ms) { return new Promise(r => setTimeout(r, Math.max(0, ms))); }

/* ════════════════════════════════════════════════════════════════
   Main
   ════════════════════════════════════════════════════════════════ */
async function main() {
  const PORT = 8321;
  console.log('Starting server...');
  const server = await startServer(PORT);

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--force-color-profile=srgb',
      '--disable-lcd-text',
      '--autoplay-policy=no-user-gesture-required',
    ],
  });

  const page = await browser.newPage();
  await page.setViewport({ width: 1920, height: 1080, deviceScaleFactor: 1 });

  // Hook Audio.play() to capture timestamps — suppress actual playback
  await page.evaluateOnNewDocument(() => {
    window.__audioLog = [];
    window.__t0 = 0;

    const _origPlay = HTMLMediaElement.prototype.play;
    HTMLMediaElement.prototype.play = function () {
      if (window.__t0 > 0 && this.src && this.volume > 0) {
        // Extract filename from URL (e.g., "audio/intro-logo.mp3")
        const url = new URL(this.src, location.href);
        window.__audioLog.push({
          file: url.pathname.split('/').slice(-2).join('/'),  // "audio/xxx.mp3"
          offset: Date.now() - window.__t0,
        });
      }
      // Suppress actual playback (headless has no audio device)
      return Promise.resolve();
    };
  });

  console.log('Loading demo...');
  await page.goto(`http://localhost:${PORT}/`, { waitUntil: 'networkidle0', timeout: 30000 });
  await page.waitForSelector('#tut-btn-watch', { timeout: 10000 });

  // ─── Start recording ────────────────────────────────────────
  const videoPath = path.join(OUTPUT_DIR, '_video.webm');
  const recorder = await page.screencast({ path: videoPath });

  // Mark t0 in browser (synced with recording start)
  await page.evaluate(() => { window.__t0 = Date.now(); });
  const nodeT0 = Date.now();

  console.log('Recording...\n');

  // Helper: get audio key from duration map for wait calculations
  function audioDur(key) { return durations[key] || 5000; }

  // ─── Landing ─────────────────────────────────────────────────
  await delay(1200);
  console.log(`  [${sec()}] Click Guided Walkthrough`);
  await jsClick(page, '#tut-btn-watch');

  // First intro scene appears after 500ms fade
  await delay(500);

  // ─── Intro scenes ───────────────────────────────────────────
  // Scene 0 (logo) is already showing + audio playing
  console.log(`  [${sec()}] intro-logo`);
  await delay(audioDur('intro-logo') + 200);

  const introNext = [
    'intro-problem', 'intro-insight', 'intro-pipeline',
    'intro-results', 'intro-transition',
  ];

  for (const key of introNext) {
    console.log(`  [${sec()}] Click Next → ${key}`);
    await jsClick(page, '.tut-intro-next');
    // Scene transition 350ms + audio plays
    await delay(350 + audioDur(key) + 200);
  }

  // ─── Click "Start Tour" ─────────────────────────────────────
  console.log(`  [${sec()}] Click Start Tour`);
  await jsClick(page, '.tut-intro-next');
  // 800ms transition → first tour step appears + audio
  await delay(800);

  // ─── Tour steps ─────────────────────────────────────────────
  // First step (control-panel) already showing
  console.log(`  [${sec()}] tour: control-panel`);
  await delay(audioDur('tour-control-panel') + 200);

  // Remaining tour steps — click Next, wait for audio to finish
  const tourFlow = [
    { key: 'tour-browse-tab' },
    { key: 'tour-new-query',           settle: 250 },  // click-tab: dim delay
    { key: 'tour-new-query-providers' },
    { key: 'tour-custom-data',         settle: 250 },
    { key: 'tour-custom-data-pane' },
    { key: 'tour-back-to-browse',      settle: 250 },
    { key: 'tour-view-pipeline' },
    // After this Next click: scroll-to-viz auto-advances → pipeline-bar
    { key: 'tour-pipeline-bar',        preWait: 1200 },  // scroll-to-viz processing
    { key: 'tour-agent-sql',           settle: 250 },
    { key: 'tour-canvas-sql' },
    { key: 'tour-agent-wa',            settle: 250 },
    { key: 'tour-canvas-wa-tables' },
    { key: 'tour-canvas-wa-hw' },
    { key: 'tour-agent-sd',            settle: 250 },
    { key: 'tour-canvas-sd' },
    { key: 'tour-agent-qp',            settle: 250 },
    { key: 'tour-canvas-qp' },
    { key: 'tour-agent-cg',            settle: 250 },
    { key: 'tour-canvas-cg' },
    { key: 'tour-agent-opt',           settle: 250 },
    { key: 'tour-canvas-opt' },
    { key: 'tour-canvas-opt-code' },
    { key: 'tour-benchmark' },
    { key: 'tour-finish' },
  ];

  for (const step of tourFlow) {
    console.log(`  [${sec()}] Click Next → ${step.key}`);
    await jsClick(page, '.tut-tooltip-next');

    // Pre-wait for auto-advance steps (scroll-to-viz)
    if (step.preWait) await delay(step.preWait);

    // Settle time for background-change steps
    const settle = step.settle || 50;
    await delay(settle + audioDur(step.key) + 200);
  }

  // Hold on finish screen
  await delay(500);

  // ─── Stop recording ─────────────────────────────────────────
  const totalSec = (Date.now() - nodeT0) / 1000;
  console.log(`\nStopping recording... (${totalSec.toFixed(1)}s)`);
  await recorder.stop();

  // Retrieve audio timestamps from browser
  const audioLog = await page.evaluate(() => window.__audioLog);
  console.log(`Captured ${audioLog.length} audio events\n`);

  await browser.close();
  server.close();

  // Print audio log
  for (const entry of audioLog) {
    console.log(`  ${(entry.offset/1000).toFixed(1).padStart(6)}s  ${entry.file}`);
  }
  console.log();

  // ─── Build audio track from captured timestamps ─────────────
  console.log('Building audio track...');
  const audioPath = path.join(OUTPUT_DIR, '_audio.wav');
  buildAudioTrack(audioLog, totalSec, audioPath);

  // ─── Merge video + audio ────────────────────────────────────
  console.log('Encoding final MP4...');
  const finalPath = path.join(OUTPUT_DIR, 'demo-video.mp4');
  mergeVideoAudio(videoPath, audioPath, finalPath);

  // Cleanup
  try { fs.unlinkSync(videoPath); } catch(e) {}
  try { fs.unlinkSync(audioPath); } catch(e) {}

  const stat = fs.statSync(finalPath);
  console.log(`\n✅ ${finalPath}`);
  console.log(`   ${(stat.size/1024/1024).toFixed(1)} MB, ${totalSec.toFixed(0)}s (${(totalSec/60).toFixed(1)} min)`);

  function sec() { return ((Date.now() - nodeT0) / 1000).toFixed(1); }
}

async function jsClick(page, selector) {
  await page.evaluate(sel => {
    const el = document.querySelector(sel);
    if (el) el.click();
  }, selector);
}

/* ════════════════════════════════════════════════════════════════
   Build audio track — place each MP3 at its captured offset
   ════════════════════════════════════════════════════════════════ */
function buildAudioTrack(audioLog, totalSec, outputPath) {
  const listPath = path.join(OUTPUT_DIR, '_ac.txt');
  const tmpFiles = [];
  const entries = [];
  let pos = 0; // current position in ms

  // Sort by offset (should already be sorted)
  audioLog.sort((a, b) => a.offset - b.offset);

  for (const entry of audioLog) {
    const filePath = path.join(DEMO_DIR, entry.file);
    if (!fs.existsSync(filePath)) {
      console.warn(`  Missing: ${filePath}`);
      continue;
    }

    // Silence gap before this segment
    const gap = entry.offset - pos;
    if (gap > 20) {
      const sf = path.join(OUTPUT_DIR, `_s${tmpFiles.length}.wav`);
      execSync(`ffmpeg -y -f lavfi -i anullsrc=r=44100:cl=stereo -t ${(gap/1000).toFixed(4)} "${sf}"`, { stdio: 'pipe' });
      entries.push(`file '${sf}'`);
      tmpFiles.push(sf);
    }

    // Convert MP3 to WAV
    const wf = path.join(OUTPUT_DIR, `_a${tmpFiles.length}.wav`);
    execSync(`ffmpeg -y -i "${filePath}" -ar 44100 -ac 2 "${wf}"`, { stdio: 'pipe' });
    entries.push(`file '${wf}'`);
    tmpFiles.push(wf);

    // Get actual duration from ffprobe
    const durStr = execSync(`ffprobe -v quiet -show_entries format=duration -of csv=p=0 "${filePath}"`, { encoding: 'utf8' }).trim();
    pos = entry.offset + Math.round(parseFloat(durStr) * 1000);
  }

  // Trailing silence
  const totalMs = totalSec * 1000;
  if (totalMs - pos > 20) {
    const sf = path.join(OUTPUT_DIR, `_s${tmpFiles.length}.wav`);
    execSync(`ffmpeg -y -f lavfi -i anullsrc=r=44100:cl=stereo -t ${((totalMs - pos)/1000).toFixed(4)} "${sf}"`, { stdio: 'pipe' });
    entries.push(`file '${sf}'`);
    tmpFiles.push(sf);
  }

  fs.writeFileSync(listPath, entries.join('\n'));
  execSync(`ffmpeg -y -f concat -safe 0 -i "${listPath}" -c:a pcm_s16le "${outputPath}"`, { stdio: 'pipe' });

  for (const f of tmpFiles) try { fs.unlinkSync(f); } catch(e) {}
  try { fs.unlinkSync(listPath); } catch(e) {}
  console.log(`  Done (${audioLog.length} segments placed)\n`);
}

/* ════════════════════════════════════════════════════════════════
   Merge video + audio → MP4
   ════════════════════════════════════════════════════════════════ */
function mergeVideoAudio(videoPath, audioPath, outputPath) {
  const cmd = [
    'ffmpeg -y',
    `-i "${videoPath}"`,
    `-i "${audioPath}"`,
    '-c:v libx264 -preset veryfast -crf 18',
    '-maxrate 4000k -bufsize 8000k',
    '-pix_fmt yuv420p',
    '-color_primaries bt709 -color_trc bt709 -colorspace bt709',
    '-c:a aac -b:a 128k',
    '-map 0:v:0 -map 1:a:0',
    '-shortest',
    '-movflags +faststart',
    `"${outputPath}"`,
  ].join(' ');

  execSync(cmd, { stdio: 'pipe', maxBuffer: 200 * 1024 * 1024 });

  const sizeMB = fs.statSync(outputPath).size / 1024 / 1024;
  if (sizeMB > 50) {
    console.log(`  ${sizeMB.toFixed(1)} MB > 50 MB, re-encoding...`);
    const tmp = outputPath + '.tmp.mp4';
    fs.renameSync(outputPath, tmp);
    execSync([
      'ffmpeg -y', `-i "${tmp}"`,
      '-c:v libx264 -preset veryfast -crf 28 -maxrate 1500k -bufsize 3000k',
      '-pix_fmt yuv420p -c:a aac -b:a 96k -movflags +faststart',
      `"${outputPath}"`,
    ].join(' '), { stdio: 'pipe' });
    fs.unlinkSync(tmp);
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
