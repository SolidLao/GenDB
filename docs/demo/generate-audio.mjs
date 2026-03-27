/**
 * GenDB Demo — Audio Narration Generator
 * Uses OpenAI gpt-4o-mini-tts to generate MP3 narration segments.
 *
 * Usage:
 *   export OPENAI_API_KEY="sk-..."
 *   node docs/demo/generate-audio.mjs
 *
 * Outputs MP3 files + durations.json for sync.
 */

import OpenAI from 'openai';
import { writeFileSync, mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const AUDIO_DIR = join(__dirname, 'audio');
mkdirSync(AUDIO_DIR, { recursive: true });

const openai = new OpenAI();

const VOICE = 'echo';
const MODEL = 'gpt-4o-mini-tts';
const SPEED = 1.08;  // 1.0 = normal, 1.08 = slightly brisk

const INSTRUCTIONS = `Affect/personality: An enthusiastic but composed tech demo presenter.

Tone: Professional yet warm — like presenting an exciting research project at a conference demo session. Confident, clear, and genuinely interested in the technology.

Pronunciation: Clear, articulate. Technical terms (TPC-H, SIMD, mmap, bitmap, O(1)) should be pronounced naturally and confidently. Numbers should be spoken clearly.

Pacing: Speak at a brisk, energetic pace — slightly faster than normal conversational speed. Keep momentum. Do NOT drag or slow down unnecessarily. Short, purposeful pauses between sentences only — no long pauses.

Emotion: Genuinely excited about the technology but not over-the-top. Convey the sense of "look what this can do" without being salesy. Warm and inviting.`;

/**
 * All segments — text MUST match the subtitle text in tutorial-script.js exactly.
 * The audio file names must match the audioFiles mapping in tutorial-script.js.
 */
const segments = [
  // ── Part 1: Intro scenes (must match tutorial-script.js subtitle text exactly) ──
  { name: 'intro-logo', text: 'Welcome to GenDB.' },
  { name: 'intro-problem', text: 'Traditional databases run every query through the same general-purpose engine \u2014 regardless of your data or hardware.' },
  { name: 'intro-insight', text: 'GenDB uses AI agents to generate instance-optimized query processing code, tailored to your specific data and hardware.' },
  { name: 'intro-pipeline', text: 'GenDB uses a multi-agent workflow \u2014 profiling your workload, designing storage, planning execution, generating code, and iteratively optimizing it.' },
  { name: 'intro-results', text: 'On the TPC-H benchmark, GenDB\u2019s generated code outperforms every established database system we tested \u2014 fully automatic, zero manual tuning.' },
  { name: 'intro-transition', text: 'Let\u2019s see how it works.' },

  // ── Part 2: Tour steps (must match tutorial-script.js subtitle text exactly) ──
  { name: 'tour-control-panel', text: 'This is your control panel \u2014 browse pre-computed results, write your own queries, or even bring your own data.' },
  { name: 'tour-browse-tab', text: 'We\u2019ve pre-computed results for TPC-H and SEC-EDGAR. Pick any query to instantly see the full pipeline.' },
  { name: 'tour-new-query', text: 'You can write any SQL you want, and GenDB generates optimized code for it in real time.' },
  { name: 'tour-new-query-providers', text: 'You choose which LLM powers the generation \u2014 Claude or OpenAI. You can even compare how different models optimize the same query.' },
  { name: 'tour-custom-data', text: 'Or bring your own data \u2014 GenDB generates optimized code for any dataset.' },
  { name: 'tour-custom-data-pane', text: 'Just provide your database schema, drop in CSV files, and write a query. GenDB takes care of storage design, execution planning, and code generation.' },
  { name: 'tour-back-to-browse', text: 'Let\u2019s see the pipeline on TPC-H Q3 \u2014 a three-table join with aggregation and top-K.' },
  { name: 'tour-view-pipeline', text: 'Click View Pipeline to dive in.' },
  { name: 'tour-pipeline-bar', text: 'Here is the multi-agent pipeline. Each step builds on the previous, and you can click any to see exactly what it contributes.' },
  { name: 'tour-agent-sql', text: 'The SQL Input step extracts a parameterized template from your query. Different filter values or date ranges can reuse the same generated code.' },
  { name: 'tour-canvas-sql', text: 'See the color-coded values? Those are the extracted parameters \u2014 swap them out for different workloads without regenerating.' },
  { name: 'tour-agent-wa', text: 'Now the Workload Analyzer kicks in \u2014 it profiles your data and hardware to spot bottlenecks and opportunities before any code is written.' },
  { name: 'tour-canvas-wa-tables', text: 'The customer segment filter keeps just 20% of rows, from 1.5 million down to 300 thousand. That kind of insight is gold for optimization.' },
  { name: 'tour-canvas-wa-hw', text: 'The hardware \u2014 64 cores enable morsel-driven parallelism, HDD favors sequential I/O, 44-megabyte L3 cache fits the customer bitmap, and AVX-512 unlocks SIMD aggregation.' },
  { name: 'tour-agent-sd', text: 'Next up, the Storage Designer \u2014 it transforms raw text files into binary columnar storage with memory-mapped I/O, picking the ideal encoding and index for each column.' },
  { name: 'tour-canvas-sd', text: 'Market segment has only 5 values, so it gets dictionary-encoded into a single byte. The grouped index on lineitem maps each order key to a contiguous range, turning a 60-million-row scan into O(1) lookup.' },
  { name: 'tour-agent-qp', text: 'Now the Query Planner takes everything we\u2019ve learned and designs the actual execution strategy \u2014 join algorithms, scan methods, parallelism, all of it.' },
  { name: 'tour-canvas-qp', text: 'Watch how it flows for Q3 \u2014 first a bitmap from 300 thousand qualifying customers, then a semi-join filter on orders, an index probe into lineitem, and finally the top 10 by revenue. 76 million input rows reduced to just 10.' },
  { name: 'tour-agent-cg', text: 'This is where it all comes together \u2014 the Code Generator produces a complete, standalone C++ program. Not interpreted SQL, but native compiled code.' },
  { name: 'tour-canvas-cg', text: 'For Q3, that\u2019s just 289 lines. And see those line-range badges on the operator tree? They link each operator directly to its code section, so you can trace exactly how the plan maps to code.' },
  { name: 'tour-agent-opt', text: 'And finally, the Optimizer \u2014 this is where the magic happens. It profiles the code, finds bottlenecks, and rewrites them, iterating until performance converges.' },
  { name: 'tour-canvas-opt', text: 'Here\u2019s the story: the first version ran in 593 milliseconds \u2014 91% of that was wasted zero-initializing a 1 gigabyte array for just 3.5 million entries. The Optimizer spotted it, swapped in compact structures, added demand-paged I/O, parallelized the bitmap \u2014 and four iterations later, 27 milliseconds. A 22x speedup.' },
  { name: 'tour-canvas-opt-code', text: 'Browse the actual C++ code for each iteration. Use the dropdown to compare how the Optimizer rewrites the code at every step.' },

  { name: 'tour-benchmark', text: 'And the results speak for themselves \u2014 GenDB\u2019s generated code beats every established database system we tested. Fully automatic, zero manual tuning.' },
  { name: 'tour-finish', text: 'That\u2019s GenDB. Click any agent to dig deeper, switch queries, or try generating code on your own data. Enjoy exploring!' },
];

// Get MP3 duration using ffprobe if available, else estimate from file size
function getDurationSec(filePath) {
  try {
    const out = execSync(`ffprobe -v error -show_entries format=duration -of csv=p=0 "${filePath}"`, { encoding: 'utf8' });
    return parseFloat(out.trim());
  } catch {
    return null;
  }
}

console.log(`Generating ${segments.length} audio segments with OpenAI TTS (${MODEL}, voice: ${VOICE})...\n`);

const durations = {};

for (const segment of segments) {
  try {
    const response = await openai.audio.speech.create({
      model: MODEL,
      voice: VOICE,
      input: segment.text,
      instructions: INSTRUCTIONS,
      speed: SPEED,
      response_format: 'mp3',
    });

    const buffer = Buffer.from(await response.arrayBuffer());
    const outPath = join(AUDIO_DIR, `${segment.name}.mp3`);
    writeFileSync(outPath, buffer);

    // Try to get actual duration
    const dur = getDurationSec(outPath);
    if (dur) {
      durations[segment.name] = Math.round(dur * 1000); // ms
      console.log(`  \u2713 ${segment.name}.mp3 (${(buffer.length / 1024).toFixed(1)} KB, ${dur.toFixed(1)}s)`);
    } else {
      // Estimate: ~150 words/min at 0.95x speed → ~10 chars/sec
      const estMs = Math.round((segment.text.length / 10) * 1000) + 500;
      durations[segment.name] = estMs;
      console.log(`  \u2713 ${segment.name}.mp3 (${(buffer.length / 1024).toFixed(1)} KB, ~${(estMs/1000).toFixed(1)}s est)`);
    }
  } catch (err) {
    console.error(`  \u2717 ${segment.name}: ${err.message}`);
  }
}

// Write durations JSON for the tutorial to use
const durPath = join(AUDIO_DIR, 'durations.json');
writeFileSync(durPath, JSON.stringify(durations, null, 2));
console.log(`\nDurations saved to ${durPath}`);
console.log('Done! Audio files saved to docs/demo/audio/');
