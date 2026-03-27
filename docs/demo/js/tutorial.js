/* ============================================================
   GenDB Demo — Tutorial Controller
   Cinematic intro + guided tour with spotlight, audio & subtitles.
   ============================================================ */

var Tutorial = (function () {
  'use strict';

  var overlay, introEl, subtitleEl, skipBtn, audioBtn;
  var spotlightSvg, spotlightRect, highlightRing, tooltipEl;
  var currentAudio = null;
  var aborted = false;
  var tourIdx = -1;
  var advancing = false;
  var audioEnabled = getAudioPref();

  /* ════════════════════════════════════════════════════════
     Helpers
     ════════════════════════════════════════════════════════ */
  function el(tag, cls, html) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html !== undefined) e.innerHTML = html;
    return e;
  }

  function wait(ms) {
    return new Promise(function (resolve) {
      if (ms <= 0 || aborted) { resolve(); return; }
      var t = setTimeout(resolve, ms);
      var check = setInterval(function () {
        if (aborted) { clearTimeout(t); clearInterval(check); resolve(); }
      }, 100);
    });
  }

  function showSubtitle(text) {
    if (!subtitleEl) return;
    subtitleEl.textContent = text || '';
    subtitleEl.classList.toggle('visible', !!text);
  }

  function hideSubtitle() { showSubtitle(''); }

  /* ════════════════════════════════════════════════════════
     Audio
     ════════════════════════════════════════════════════════ */
  var audioUnlocked = false;

  // Mobile browsers block audio.play() until a user gesture triggers it.
  // This preloads and starts (then immediately pauses) a real audio file
  // inside the user gesture handler, which unlocks the audio context.
  function unlockAudio() {
    if (audioUnlocked) return;
    // Play any available audio file to unlock the context within this gesture
    var firstSrc = TUTORIAL_SCRIPT.audioFiles['intro-logo'];
    if (!firstSrc) return;
    var a = new Audio(firstSrc);
    a.volume = 0;
    a.play().then(function () {
      a.pause();
      a.currentTime = 0;
      audioUnlocked = true;
    }).catch(function () {});
  }

  function getAudioPref() {
    try { return localStorage.getItem('gendb-tutorial-audio') !== 'false'; }
    catch (e) { return true; }
  }
  function setAudioPref(on) {
    audioEnabled = on;
    try { localStorage.setItem('gendb-tutorial-audio', on ? 'true' : 'false'); }
    catch (e) {}
    updateAudioBtn();
  }
  function toggleAudio() {
    unlockAudio(); // user gesture — unlock mobile audio
    setAudioPref(!audioEnabled);
    if (!audioEnabled) stopAudio();
  }
  function updateAudioBtn() {
    if (!audioBtn) return;
    audioBtn.innerHTML = audioEnabled
      ? '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/></svg>'
      : '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><line x1="23" y1="9" x2="17" y2="15"/><line x1="17" y1="9" x2="23" y2="15"/></svg>';
    audioBtn.title = audioEnabled ? 'Mute narration' : 'Enable narration';
  }

  // Plays audio and returns a Promise that resolves when audio finishes.
  // Resolves immediately if audio is disabled or unavailable.
  function playAudio(key) {
    if (currentAudio) { currentAudio.pause(); currentAudio = null; }
    if (!audioEnabled) return Promise.resolve(0);
    var src = TUTORIAL_SCRIPT.audioFiles[key];
    if (!src) return Promise.resolve(0);
    var audio = new Audio(src);
    audio.volume = 0.85;
    currentAudio = audio;
    return new Promise(function (resolve) {
      audio.addEventListener('ended', function () { resolve(audio.duration * 1000); });
      audio.addEventListener('error', function () { resolve(0); });
      audio.play().catch(function () { resolve(0); });
      // Safety: if audio somehow stalls, resolve after 30s
      setTimeout(function () { resolve((audio.duration || 0) * 1000); }, 30000);
    });
  }

  // Play audio without waiting (for tour steps where we don't want to block)
  function playAudioFire(key) {
    if (currentAudio) { currentAudio.pause(); currentAudio = null; }
    if (!audioEnabled) return;
    var src = TUTORIAL_SCRIPT.audioFiles[key];
    if (!src) return;
    var audio = new Audio(src);
    audio.volume = 0.85;
    audio.play().catch(function () {});
    currentAudio = audio;
  }

  function stopAudio() {
    if (currentAudio) { currentAudio.pause(); currentAudio = null; }
  }

  /* ════════════════════════════════════════════════════════
     START — Entry Point
     ════════════════════════════════════════════════════════ */
  function start() {
    aborted = false;
    advancing = false;
    buildOverlay();
    overlay.classList.add('active');
    overlay.style.background = '';  // clear inline preload background
    document.body.style.overflow = 'hidden';
    showLanding();
  }

  function buildOverlay() {
    overlay = document.getElementById('tutorial-overlay');
    if (!overlay) {
      overlay = el('div'); overlay.id = 'tutorial-overlay';
      document.body.insertBefore(overlay, document.body.firstChild);
    }
    overlay.innerHTML = '';

    // Top-right controls
    var controls = el('div', 'tut-controls');

    audioBtn = el('button', 'tut-audio-btn');
    audioBtn.addEventListener('click', toggleAudio);
    updateAudioBtn();
    controls.appendChild(audioBtn);

    skipBtn = el('button', 'tut-skip', 'Skip');
    skipBtn.addEventListener('click', skipAll);
    controls.appendChild(skipBtn);

    overlay.appendChild(controls);

    subtitleEl = el('div', 'tut-subtitle');
    overlay.appendChild(subtitleEl);
  }

  /* ════════════════════════════════════════════════════════
     LANDING — Watch or Skip choice
     ════════════════════════════════════════════════════════ */
  function showLanding() {
    var old = overlay.querySelector('.tut-intro');
    if (old) old.remove();

    introEl = el('div', 'tut-intro');
    var landing = el('div', 'tut-landing');
    landing.innerHTML =
      '<div class="tut-landing-logo"><span class="gen">Gen</span>DB</div>' +
      '<div class="tut-landing-tagline">LLM-Powered Generative Query Engine</div>' +
      '<div class="tut-landing-buttons">' +
        '<button class="tut-btn tut-btn-primary" id="tut-btn-watch">Guided Walkthrough</button>' +
        '<button class="tut-btn tut-btn-secondary" id="tut-btn-skip-intro">Skip to Interface</button>' +
      '</div>';
    introEl.appendChild(landing);
    overlay.appendChild(introEl);

    document.getElementById('tut-btn-watch').addEventListener('click', function () {
      unlockAudio(); // unlock mobile audio on first user gesture
      landing.style.opacity = '0';
      landing.style.transition = 'opacity 0.4s ease';
      setTimeout(function () { landing.remove(); runIntroScenes(); }, 500);
    });
    document.getElementById('tut-btn-skip-intro').addEventListener('click', function () {
      unlockAudio(); // unlock mobile audio on first user gesture
      skipIntro();
    });
  }

  /* ════════════════════════════════════════════════════════
     PART 1: Cinematic Intro Scenes (user-paced with Next)
     ════════════════════════════════════════════════════════ */
  var introNextBtn = null;
  var introSceneIdx = -1;
  var introScenes = null;

  function runIntroScenes() {
    if (aborted) return;
    introScenes = TUTORIAL_SCRIPT.introScenes;
    introSceneIdx = -1;

    // Create a persistent "Next" button for intro scenes
    introNextBtn = el('button', 'tut-btn tut-btn-primary tut-intro-next', 'Next \u2192');
    introNextBtn.addEventListener('click', function () {
      unlockAudio();
      advanceIntroScene();
    });
    overlay.appendChild(introNextBtn);

    // Show first scene immediately
    advanceIntroScene();
  }

  async function advanceIntroScene() {
    if (aborted) return;
    introSceneIdx++;

    // Done with all scenes?
    if (introSceneIdx >= introScenes.length) {
      hideSubtitle();
      if (introNextBtn) { introNextBtn.remove(); introNextBtn = null; }
      transitionToTour();
      return;
    }

    var scene = introScenes[introSceneIdx];
    var isLast = introSceneIdx === introScenes.length - 1;

    // Update next button text
    if (introNextBtn) {
      introNextBtn.textContent = isLast ? 'Start Tour \u2192' : 'Next \u2192';
    }

    // Fade out previous scene
    var prev = introEl.querySelector('.tut-scene');
    if (prev) { prev.classList.remove('active'); await wait(350); prev.remove(); }

    // Build new scene
    var s = el('div', 'tut-scene');
    switch (scene.id) {
      case 'logo': buildSceneLogo(s); break;
      case 'problem': buildSceneProblem(s); break;
      case 'insight': buildSceneInsight(s); break;
      case 'pipeline': buildScenePipeline(s); break;
      case 'results': buildSceneResults(s); break;
      case 'transition': buildSceneTransition(s); break;
    }
    introEl.appendChild(s);
    requestAnimationFrame(function () {
      requestAnimationFrame(function () { s.classList.add('active'); });
    });

    // Play audio + subtitle (user just tapped, so audio is allowed)
    showSubtitle(scene.subtitle);
    playAudioFire('intro-' + scene.id);

    // Run scene-specific animations
    if (scene.id === 'pipeline') animatePipeline(s, scene.duration);
    else if (scene.id === 'results') animateResults(s, scene.duration);
  }

  /* ── Scene Builders ── */

  function buildSceneLogo(s) {
    s.innerHTML = '<div class="tut-logo-glow"></div>' +
      '<div class="tut-logo-big"><span class="gen">Gen</span>DB</div>';
  }

  function buildSceneProblem(s) {
    s.innerHTML =
      '<div class="tut-problem-visual">' +
        '<div class="tut-db-icon">' +
          '<svg viewBox="0 0 80 100" fill="none" xmlns="http://www.w3.org/2000/svg">' +
            '<ellipse cx="40" cy="20" rx="34" ry="14" stroke="rgba(99,102,241,0.4)" stroke-width="2"/>' +
            '<path d="M6 20v60c0 7.7 15.2 14 34 14s34-6.3 34-14V20" stroke="rgba(99,102,241,0.4)" stroke-width="2"/>' +
            '<ellipse cx="40" cy="50" rx="34" ry="14" stroke="rgba(99,102,241,0.15)" stroke-width="1"/>' +
          '</svg>' +
        '</div>' +
        '<div class="tut-problem-text">' +
          '<h2>One Engine for All Queries</h2>' +
          '<p>Traditional databases use general-purpose query engines. The same code processes every query, regardless of your data distribution or hardware.</p>' +
        '</div>' +
      '</div>';
  }

  function buildSceneInsight(s) {
    s.innerHTML =
      '<div class="tut-insight">' +
        '<div class="tut-code-morph">' +
          '<div class="tut-code-block sql-block">' +
            '<span class="kw">SELECT</span> l_orderkey,<br>' +
            '  <span class="fn">SUM</span>(l_extendedprice)<br>' +
            '<span class="kw">FROM</span> lineitem, orders<br>' +
            '<span class="kw">WHERE</span> o_orderdate &lt; <span class="str">\'1995-03-15\'</span><br>' +
            '<span class="kw">GROUP BY</span> l_orderkey' +
          '</div>' +
          '<div class="tut-arrow-morph">\u279C</div>' +
          '<div class="tut-code-block cpp-block">' +
            '<span class="fn">mmap</span>(data, PROT_READ);<br>' +
            '<span class="kw">#pragma omp parallel</span><br>' +
            'bitmap_join(cust, ord);<br>' +
            'simd_aggregate(result);<br>' +
            'top_k_sort(output, 10);' +
          '</div>' +
        '</div>' +
        '<div class="tut-insight-headline">What if AI <em>generated instance-optimized code</em> for each query?</div>' +
      '</div>';
  }

  function buildScenePipeline(s) {
    var html = '<div class="tut-pipeline">';
    TUTORIAL_SCRIPT.pipelineAgents.forEach(function (a, i) {
      if (i > 0) html += '<div class="tut-pipe-arrow" data-idx="' + i + '">\u2192</div>';
      html +=
        '<div class="tut-agent" data-idx="' + i + '">' +
          '<div class="tut-agent-circle" style="background:' + a.color + '">' + a.shortName + '</div>' +
          '<div class="tut-agent-name">' + a.name + '</div>' +
          '<div class="tut-agent-role">' + a.role + '</div>' +
        '</div>';
    });
    html += '</div>';
    s.innerHTML = html;
  }

  async function animatePipeline(s, totalDuration) {
    var agents = s.querySelectorAll('.tut-agent');
    var arrows = s.querySelectorAll('.tut-pipe-arrow');
    var stagger = 900;
    for (var i = 0; i < agents.length; i++) {
      if (aborted) return;
      agents[i].classList.add('show');
      if (i > 0 && arrows[i - 1]) arrows[i - 1].classList.add('show');
      await wait(stagger);
    }
    var elapsed = agents.length * stagger;
    if (totalDuration > elapsed) await wait(totalDuration - elapsed);
  }

  function buildSceneResults(s) {
    var bars = TUTORIAL_SCRIPT.benchmarkBars;
    // Linear scale capped at ClickHouse; PostgreSQL gets a taller 'broken' bar
    var maxBarH = 140;
    var ref = 2407; // ClickHouse ms = full normal height
    var minH = 22;
    var html =
      '<div class="tut-results">' +
        '<div class="tut-results-title">TPC-H SF = 10</div>' +
        '<div class="tut-results-subtitle">Full workload, identical hardware</div>' +
        '<div class="tut-stats-row">' +
          '<div class="tut-stat"><div class="tut-stat-number c-emerald" data-target="3.2" data-suffix="\u00D7">0</div><div class="tut-stat-label">faster than DuckDB</div></div>' +
          '<div class="tut-stat"><div class="tut-stat-number c-indigo" data-target="13" data-suffix="\u00D7">0</div><div class="tut-stat-label">faster than ClickHouse</div></div>' +
          '<div class="tut-stat"><div class="tut-stat-number c-amber" data-target="462" data-suffix="\u00D7">0</div><div class="tut-stat-label">faster than PostgreSQL</div></div>' +
        '</div>' +
        '<div class="tut-results-bar">';
    bars.forEach(function (b) {
      var h;
      if (b.ms <= ref) {
        h = Math.max(minH, Math.round((b.ms / ref) * maxBarH));
      } else {
        h = maxBarH + 20; // PostgreSQL: oversized with break marker
      }
      var isBreak = b.ms > ref;
      var msLabel = b.ms >= 1000 ? (b.ms / 1000).toFixed(1) + 's' : b.ms + 'ms';
      html +=
        '<div class="tut-bar-col">' +
          '<div class="tut-bar' + (isBreak ? ' tut-bar-break' : '') + '" data-height="' + h + '" style="background:' + b.color +
            (b.highlight ? ';box-shadow:0 2px 16px ' + b.color + '60' : '') + '">' +
            '<div class="tut-bar-ms">' + msLabel + '</div>' +
          '</div>' +
          '<div class="tut-bar-label">' + b.name + '</div>' +
        '</div>';
    });
    html +=
        '</div>' +
        '<div class="tut-auto-note">Automatically generated, zero manual tuning</div>' +
      '</div>';
    s.innerHTML = html;
  }

  async function animateResults(s, totalDuration) {
    var nums = s.querySelectorAll('.tut-stat-number');
    nums.forEach(function (numEl) {
      var target = parseFloat(numEl.dataset.target);
      var suffix = numEl.dataset.suffix || '';
      countUp(numEl, 0, target, 1400, suffix);
    });
    await wait(400);
    var bars = s.querySelectorAll('.tut-bar');
    bars.forEach(function (bar, i) {
      setTimeout(function () {
        bar.style.height = bar.dataset.height + 'px';
      }, i * 100);
    });
    await wait(totalDuration - 400);
  }

  function countUp(numEl, from, to, duration, suffix) {
    var start = performance.now();
    var isFloat = to !== Math.floor(to);
    function tick(now) {
      var t = Math.min((now - start) / duration, 1);
      t = 1 - Math.pow(1 - t, 3);
      var val = from + (to - from) * t;
      numEl.textContent = (isFloat ? val.toFixed(1) : Math.round(val)) + suffix;
      if (t < 1) requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);
  }

  function buildSceneTransition(s) {
    s.innerHTML = '<div class="tut-transition-text">Let\u2019s see how it works\u2026</div>';
  }

  /* ════════════════════════════════════════════════════════
     Transition: Intro → Tour
     ════════════════════════════════════════════════════════ */
  function transitionToTour() {
    stopAudio();
    hideSubtitle();
    if (introNextBtn) { introNextBtn.remove(); introNextBtn = null; }
    if (introEl) {
      introEl.classList.add('fade-out');
      setTimeout(function () {
        if (introEl) introEl.remove();
        introEl = null;
        document.body.style.overflow = '';
        startTour();
      }, 800);
    }
  }

  function skipIntro() {
    stopAudio();
    hideSubtitle();
    if (introNextBtn) { introNextBtn.remove(); introNextBtn = null; }
    if (introEl) introEl.remove();
    introEl = null;
    document.body.style.overflow = '';
    startTour();
  }

  /* ════════════════════════════════════════════════════════
     PART 2: Guided Tour
     ════════════════════════════════════════════════════════ */
  function startTour() {
    tourIdx = -1;
    advancing = false;
    // Clear viz so the tour starts with a clean slate — it will reload when View Pipeline is clicked
    var vizEl = document.getElementById('viz-content');
    if (vizEl) { vizEl.innerHTML = ''; vizEl.style.display = 'none'; }
    buildSpotlight();
    nextTourStep();
  }

  function buildSpotlight() {
    spotlightSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    spotlightSvg.setAttribute('class', 'tut-spotlight-svg');
    spotlightSvg.innerHTML =
      '<defs><mask id="tut-mask">' +
        '<rect width="100%" height="100%" fill="white"/>' +
        '<rect id="tut-cutout" rx="12" ry="12" fill="black" x="0" y="0" width="0" height="0"' +
        ' style="transition: all 0.4s cubic-bezier(0.16,1,0.3,1)"/>' +
      '</mask></defs>' +
      '<rect width="100%" height="100%" fill="rgba(0,0,0,0.45)" mask="url(#tut-mask)"/>';
    overlay.appendChild(spotlightSvg);
    spotlightRect = spotlightSvg.querySelector('#tut-cutout');

    highlightRing = el('div', 'tut-highlight-ring');
    highlightRing.style.display = 'none';
    overlay.appendChild(highlightRing);

    tooltipEl = el('div', 'tut-tooltip');
    tooltipEl.innerHTML =
      '<div class="tut-tooltip-step"></div>' +
      '<div class="tut-tooltip-text"></div>' +
      '<div class="tut-tooltip-actions">' +
        '<button class="tut-tooltip-skip">Skip tour</button>' +
        '<button class="tut-tooltip-next">Next</button>' +
      '</div>';
    overlay.appendChild(tooltipEl);

    tooltipEl.querySelector('.tut-tooltip-next').addEventListener('click', function () { unlockAudio(); nextTourStep(); });
    tooltipEl.querySelector('.tut-tooltip-skip').addEventListener('click', skipAll);

    document.addEventListener('keydown', handleTourKey);
  }

  function handleTourKey(e) {
    if (e.key === 'Enter' || e.key === ' ' || e.key === 'ArrowRight') {
      e.preventDefault();
      nextTourStep();
    } else if (e.key === 'Escape') {
      skipAll();
    }
  }

  // Does this step cause significant background changes?
  function stepCausesChange(step) {
    return step.action === 'click-tab' || step.action === 'click-agent' ||
           step.scrollTo === 'pipeline' || step.action === 'click-and-scroll';
  }

  async function nextTourStep() {
    if (advancing || aborted) return;
    advancing = true;

    tourIdx++;
    var steps = TUTORIAL_SCRIPT.tourSteps;
    if (tourIdx >= steps.length) { advancing = false; finishTour(); return; }

    var step = steps[tourIdx];

    // "click-and-scroll" is a silent step — dim, fire, auto-advance
    if (step.action === 'click-and-scroll') {
      hideSpotlight();
      if (tooltipEl) tooltipEl.classList.remove('visible');
      var ct = step.target ? document.querySelector(step.target) : null;
      if (ct) ct.click();
      await wait(step.delay || 600);
      advancing = false;
      nextTourStep();
      return;
    }

    // For steps that cause background changes: dim first, then settle, then reveal
    var willChange = stepCausesChange(step);

    // 1) Hide tooltip + close spotlight (fade to full dim)
    if (tooltipEl) tooltipEl.classList.remove('visible');
    if (willChange) {
      hideSpotlight();          // shrinks cutout → screen goes fully dimmed
      highlightRing.style.display = 'none';
      await wait(250);          // wait for the dim transition (CSS 0.4s, but 250ms is enough visually)
    }

    if (aborted) { advancing = false; return; }

    // 2) Show subtitle & audio
    showSubtitle(step.subtitle);
    playAudioFire(step.id);

    if (step.action === 'finish') {
      hideSpotlight();
      showFinishTooltip(step, steps.length);
      advancing = false;
      return;
    }

    var target = step.target ? document.querySelector(step.target) : null;
    if (!target) { advancing = false; nextTourStep(); return; }

    // 3) Perform click actions while screen is dimmed — user doesn't see the jarring change
    if (step.action === 'click-tab') {
      target.click();
      await wait(200);
    } else if (step.action === 'click-agent') {
      target.click();
      await wait(350);
    }

    // 4) Scroll while dimmed
    if (step.scrollTo === 'pipeline') {
      var pipeBar = document.querySelector('.pipeline-bar');
      if (pipeBar) {
        window.scrollTo({ top: pipeBar.getBoundingClientRect().top + window.pageYOffset - 80, behavior: 'smooth' });
        await wait(350);
      }
    } else if (step.scrollTo === true) {
      target = document.querySelector(step.target);
      if (target) {
        var r = target.getBoundingClientRect();
        if (r.top < 80 || r.bottom > window.innerHeight - 80) {
          window.scrollTo({ top: r.top + window.pageYOffset - 100, behavior: 'smooth' });
          await wait(350);
        }
      }
    }

    if (aborted) { advancing = false; return; }

    // 5) Re-query target after scroll/click settle
    target = step.target ? document.querySelector(step.target) : null;
    if (!target) { advancing = false; nextTourStep(); return; }

    // 6) Open spotlight at new position (CSS transition animates the reveal)
    positionSpotlight(target);

    // 7) Position and show tooltip
    positionTooltip(target, step, steps.length);
    if (tooltipEl) tooltipEl.classList.add('visible');

    advancing = false;
  }

  function positionSpotlight(target) {
    var rect = target.getBoundingClientRect();
    var pad = 6;
    var x = rect.left - pad;
    var y = rect.top - pad;
    var w = rect.width + pad * 2;
    var h = rect.height + pad * 2;

    // Move SVG cutout (CSS transition animates it)
    spotlightRect.setAttribute('x', x);
    spotlightRect.setAttribute('y', y);
    spotlightRect.setAttribute('width', w);
    spotlightRect.setAttribute('height', h);

    // Hide ring during cutout transition, show after it arrives
    highlightRing.style.display = 'none';
    setTimeout(function () {
      if (!highlightRing || aborted) return;
      highlightRing.style.left = x + 'px';
      highlightRing.style.top = y + 'px';
      highlightRing.style.width = w + 'px';
      highlightRing.style.height = h + 'px';
      highlightRing.style.display = 'block';
    }, 420); // slightly after the 0.4s SVG cutout transition
  }

  function hideSpotlight() {
    if (spotlightRect) {
      spotlightRect.setAttribute('width', 0);
      spotlightRect.setAttribute('height', 0);
    }
    if (highlightRing) highlightRing.style.display = 'none';
  }

  function positionTooltip(target, step, totalSteps) {
    var rect = target.getBoundingClientRect();
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var gap = 16;

    // Disable position transitions during repositioning
    tooltipEl.classList.add('no-transition');

    // Reset classes
    tooltipEl.className = 'tut-tooltip no-transition';

    // Measure tooltip
    tooltipEl.querySelector('.tut-tooltip-step').textContent =
      'Step ' + (tourIdx + 1) + ' of ' + totalSteps;
    tooltipEl.querySelector('.tut-tooltip-text').textContent = step.tooltip;
    var nextBtn = tooltipEl.querySelector('.tut-tooltip-next');
    nextBtn.textContent = tourIdx === totalSteps - 2 ? 'Finish' : 'Next';

    tooltipEl.style.visibility = 'hidden';
    tooltipEl.style.display = 'block';
    tooltipEl.style.top = '0'; tooltipEl.style.left = '0';
    var tw = tooltipEl.offsetWidth;
    var th = tooltipEl.offsetHeight;
    tooltipEl.style.visibility = '';

    // Smart placement: try preferred, fall back to where there's space
    var preferred = step.position || 'bottom';
    var placements = [preferred, 'bottom', 'top', 'right', 'left'];
    // Remove duplicates
    var seen = {};
    placements = placements.filter(function (p) { if (seen[p]) return false; seen[p] = true; return true; });

    var top, left, arrowClass, arrowX;
    var placed = false;

    for (var i = 0; i < placements.length; i++) {
      var pos = placements[i];
      var t, l;
      switch (pos) {
        case 'bottom':
          t = rect.bottom + gap;
          l = rect.left + rect.width / 2 - tw / 2;
          if (t + th < vh - 20 && l > 10 && l + tw < vw - 10) {
            top = t; left = l; arrowClass = 'arrow-top';
            arrowX = Math.max(20, Math.min(tw - 20, rect.left + rect.width / 2 - l)) + 'px';
            placed = true;
          }
          break;
        case 'top':
          t = rect.top - th - gap;
          l = rect.left + rect.width / 2 - tw / 2;
          if (t > 20 && l > 10 && l + tw < vw - 10) {
            top = t; left = l; arrowClass = 'arrow-bottom';
            arrowX = Math.max(20, Math.min(tw - 20, rect.left + rect.width / 2 - l)) + 'px';
            placed = true;
          }
          break;
        case 'right':
          t = rect.top + rect.height / 2 - th / 2;
          l = rect.right + gap;
          if (l + tw < vw - 20 && t > 20 && t + th < vh - 20) {
            top = t; left = l; arrowClass = 'arrow-left';
            placed = true;
          }
          break;
        case 'left':
          t = rect.top + rect.height / 2 - th / 2;
          l = rect.left - tw - gap;
          if (l > 20 && t > 20 && t + th < vh - 20) {
            top = t; left = l; arrowClass = 'arrow-right';
            placed = true;
          }
          break;
      }
      if (placed) break;
    }

    // Final fallback: force bottom, clamp
    if (!placed) {
      top = rect.bottom + gap;
      left = rect.left + rect.width / 2 - tw / 2;
      arrowClass = 'arrow-top';
      arrowX = '50%';
    }

    // Clamp to viewport
    if (left < 12) left = 12;
    if (left + tw > vw - 12) left = vw - tw - 12;
    if (top < 12) top = 12;
    if (top + th > vh - 12) top = vh - th - 12;

    tooltipEl.style.top = top + 'px';
    tooltipEl.style.left = left + 'px';
    tooltipEl.classList.add(arrowClass);
    if (arrowX) tooltipEl.style.setProperty('--arrow-x', arrowX);

    // Re-enable transitions after positioning
    requestAnimationFrame(function () {
      tooltipEl.classList.remove('no-transition');
    });
  }

  function showFinishTooltip(step, totalSteps) {
    tooltipEl.className = 'tut-tooltip';
    tooltipEl.querySelector('.tut-tooltip-step').textContent = '';
    tooltipEl.querySelector('.tut-tooltip-text').textContent = step.tooltip;
    tooltipEl.querySelector('.tut-tooltip-next').textContent = 'Start Exploring';

    tooltipEl.style.visibility = 'hidden';
    tooltipEl.style.display = 'block';
    var tw = tooltipEl.offsetWidth;
    var th = tooltipEl.offsetHeight;
    tooltipEl.style.visibility = '';

    tooltipEl.style.left = (window.innerWidth / 2 - tw / 2) + 'px';
    tooltipEl.style.top = (window.innerHeight / 2 - th / 2) + 'px';

    setTimeout(function () { tooltipEl.classList.add('visible'); }, 100);
  }

  /* ════════════════════════════════════════════════════════
     FINISH / SKIP
     ════════════════════════════════════════════════════════ */
  function finishTour() { cleanup(); markSeen(); }

  function skipAll() {
    aborted = true;
    stopAudio(); hideSubtitle();
    cleanup(); markSeen();
  }

  function cleanup() {
    document.removeEventListener('keydown', handleTourKey);
    stopAudio(); hideSubtitle();
    document.body.style.overflow = '';
    if (overlay) {
      overlay.classList.remove('active');
      overlay.innerHTML = '';
    }
    introEl = null; introNextBtn = null; introScenes = null; introSceneIdx = -1;
    spotlightSvg = null; spotlightRect = null;
    highlightRing = null; tooltipEl = null; skipBtn = null;
    audioBtn = null; subtitleEl = null; tourIdx = -1; advancing = false;
  }

  function markSeen() {
    try { localStorage.setItem('gendb-tutorial-seen', 'true'); } catch (e) {}
  }

  function hasBeenSeen() {
    try { return localStorage.getItem('gendb-tutorial-seen') === 'true'; } catch (e) { return false; }
  }

  function reset() {
    try { localStorage.removeItem('gendb-tutorial-seen'); } catch (e) {}
  }

  /* ════════════════════════════════════════════════════════
     PUBLIC API
     ════════════════════════════════════════════════════════ */
  return {
    start: start,
    skipAll: skipAll,
    hasBeenSeen: hasBeenSeen,
    reset: reset
  };
})();
