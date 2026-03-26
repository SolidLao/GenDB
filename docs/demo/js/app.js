/* ============================================================
   GenDB Demo — app.js
   Main application module: state, routing, initialization.
   VLDB demonstration for multi-agent LLM query code generation.
   ============================================================ */

var App = (function () {
  'use strict';

  /* ---------- State ---------- */

  var state = {
    benchmark: 'tpch',     // 'tpch' | 'secedgar'
    queryId: 'Q3',         // current query
    stageInView: 0,        // 0-6 for the 7 stages
    observers: []          // IntersectionObservers
  };

  var STAGES = [
    { num: 0, id: 'stage-sql',          label: 'SQL Query',        icon: '1' },
    { num: 1, id: 'stage-workload',     label: 'Workload Analysis', icon: '2' },
    { num: 2, id: 'stage-storage',      label: 'Storage Design',   icon: '3' },
    { num: 3, id: 'stage-plan',         label: 'Execution Plan',   icon: '4' },
    { num: 4, id: 'stage-code',         label: 'Code Generation',  icon: '5' },
    { num: 5, id: 'stage-optimization', label: 'Optimization',     icon: '6' },
    { num: 6, id: 'stage-performance',  label: 'Performance',      icon: '7' }
  ];

  /* ---------- Data Access ---------- */

  function getDataForBenchmark(key) {
    if (key === 'tpch' && typeof TPCH_DATA !== 'undefined') return TPCH_DATA;
    if (key === 'secedgar' && typeof SECEDGAR_DATA !== 'undefined') return SECEDGAR_DATA;
    return null;
  }

  function getVizData(benchmarkKey, queryId) {
    // Check for hand-crafted viz-data (Q3 has a hand-crafted version)
    if (benchmarkKey === 'tpch' && queryId === 'Q3' && typeof Q3_VIZ !== 'undefined') return Q3_VIZ;
    // Check auto-generated viz-data registry
    if (typeof GENERATED_VIZ !== 'undefined') {
      var key = benchmarkKey + ':' + queryId;
      if (GENERATED_VIZ[key]) return GENERATED_VIZ[key];
    }
    return null;
  }

  function getAvailableQueries(key) {
    var data = getDataForBenchmark(key);
    if (!data || !data.queries) return [];
    var all = Object.keys(data.queries).sort(function (a, b) {
      var na = parseInt(a.replace(/\D/g, ''), 10);
      var nb = parseInt(b.replace(/\D/g, ''), 10);
      return na - nb;
    });
    // Only show queries that have VIZ data
    return all.filter(function (qId) { return getVizData(key, qId) !== null; });
  }

  /* ---------- Initialization ---------- */

  function initApp() {
    populateBenchmarkDropdown();
    populateQueryDropdown();
    setupDropdownHandlers();
    setupControlTabs();
    setupBrowseButton();
    setupPipelineNav();
    setupScrollObserver();
    loadQuery(state.benchmark, state.queryId);

    // Set up reveal animations
    Charts.animateOnScroll(document.querySelectorAll('.reveal'));

    // Check for server mode (non-blocking)
    ServerMode.initServerMode();

    // Tutorial: always show on page load — users can skip via the landing screen
    if (typeof Tutorial !== 'undefined') {
      setTimeout(function () { Tutorial.start(); }, 300);
    }

    // Help button: replay tutorial
    var helpBtn = document.getElementById('tut-help-btn');
    if (helpBtn) {
      helpBtn.addEventListener('click', function () {
        if (typeof Tutorial !== 'undefined') {
          Tutorial.reset();
          Tutorial.start();
        }
      });
    }
  }

  /* ---------- Control Panel Tabs ---------- */

  function setupControlTabs() {
    var tabs = document.querySelectorAll('.control-tab');
    tabs.forEach(function (tab) {
      tab.addEventListener('click', function () {
        var paneName = tab.dataset.pane;
        // Deactivate all tabs and panes
        document.querySelectorAll('.control-tab').forEach(function (t) { t.classList.remove('active'); });
        document.querySelectorAll('.control-pane').forEach(function (p) { p.classList.remove('active'); });
        // Activate clicked
        tab.classList.add('active');
        var pane = document.getElementById('pane-' + paneName);
        if (pane) pane.classList.add('active');
      });
    });
  }

  /* ---------- Browse Button ---------- */

  function setupBrowseButton() {
    var btn = document.getElementById('btn-browse');
    if (btn) {
      btn.addEventListener('click', function () {
        // In viz mode, scroll to viz content; in legacy mode, scroll to first stage
        var vizEl = document.getElementById('viz-content');
        var target = (vizEl && vizEl.style.display !== 'none') ? vizEl : document.getElementById('stage-sql');
        if (target) {
          var headerH = 64;
          var y = target.getBoundingClientRect().top + window.pageYOffset - headerH;
          window.scrollTo({ top: y, behavior: 'smooth' });
        }
      });
    }
  }

  /* ---------- Dropdowns ---------- */

  function populateBenchmarkDropdown() {
    var select = document.getElementById('benchmark-select');
    if (!select) return;
    select.innerHTML = '';

    var benchmarks = [
      { key: 'tpch', name: 'TPC-H SF10' },
      { key: 'secedgar', name: 'SEC-EDGAR SF3' }
    ];

    benchmarks.forEach(function (b) {
      var data = getDataForBenchmark(b.key);
      if (!data) return; // skip if data not loaded
      var opt = document.createElement('option');
      opt.value = b.key;
      opt.textContent = data.name || b.name;
      if (b.key === state.benchmark) opt.selected = true;
      select.appendChild(opt);
    });
  }

  function populateQueryDropdown() {
    var select = document.getElementById('query-select');
    if (!select) return;
    select.innerHTML = '';

    var queries = getAvailableQueries(state.benchmark);
    queries.forEach(function (qId) {
      var opt = document.createElement('option');
      opt.value = qId;
      opt.textContent = qId;
      if (qId === state.queryId) opt.selected = true;
      select.appendChild(opt);
    });

    // If current query not available, default to first
    if (queries.length && queries.indexOf(state.queryId) === -1) {
      state.queryId = queries[0];
      select.value = state.queryId;
    }
  }

  function setupDropdownHandlers() {
    var benchSelect = document.getElementById('benchmark-select');
    var querySelect = document.getElementById('query-select');

    if (benchSelect) {
      benchSelect.addEventListener('change', function () {
        state.benchmark = benchSelect.value;
        populateQueryDropdown();
        // Default to Q3 if available, else first
        var queries = getAvailableQueries(state.benchmark);
        state.queryId = queries.indexOf('Q3') >= 0 ? 'Q3' : (queries[0] || 'Q1');
        if (querySelect) querySelect.value = state.queryId;
        loadQuery(state.benchmark, state.queryId);
      });
    }

    if (querySelect) {
      querySelect.addEventListener('change', function () {
        state.queryId = querySelect.value;
        loadQuery(state.benchmark, state.queryId);
      });
    }
  }

  /* ---------- Pipeline Navigation ---------- */

  function setupPipelineNav() {
    var nav = document.getElementById('pipeline-nav');
    if (!nav) return;

    nav.innerHTML = '';
    STAGES.forEach(function (stage) {
      var item = document.createElement('div');
      item.className = 'step';
      item.dataset.stage = stage.num;
      item.innerHTML = '<span class="step-dot"></span>' +
                        '<span class="step-label">' + stage.label + '</span>';
      item.addEventListener('click', function () {
        scrollToStage(stage.num);
      });
      item.style.cursor = 'pointer';
      nav.appendChild(item);

      if (stage.num < STAGES.length - 1) {
        var conn = document.createElement('span');
        conn.className = 'step-line';
        nav.appendChild(conn);
      }
    });
  }

  function updatePipelineActive(stageNum) {
    state.stageInView = stageNum;
    var steps = document.querySelectorAll('.pipeline-nav .step');
    var lines = document.querySelectorAll('.pipeline-nav .step-line');
    steps.forEach(function (step) {
      var sn = parseInt(step.dataset.stage, 10);
      step.classList.remove('active', 'completed');
      if (sn === stageNum) step.classList.add('active');
      else if (sn < stageNum) step.classList.add('completed');
    });
    lines.forEach(function (line, i) {
      line.classList.toggle('completed', i < stageNum);
    });
  }

  function scrollToStage(stageNum) {
    var stage = STAGES[stageNum];
    if (!stage) return;
    var el = document.getElementById(stage.id);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  }

  /* ---------- Scroll Observer ---------- */

  function setupScrollObserver() {
    // Clean up old observers
    state.observers.forEach(function (obs) { obs.disconnect(); });
    state.observers = [];

    if (!('IntersectionObserver' in window)) return;

    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          var stageId = entry.target.id;
          var stageObj = STAGES.find(function (s) { return s.id === stageId; });
          if (stageObj) {
            updatePipelineActive(stageObj.num);
          }
        }
      });
    }, {
      rootMargin: '-20% 0px -60% 0px',
      threshold: 0
    });

    STAGES.forEach(function (stage) {
      var el = document.getElementById(stage.id);
      if (el) observer.observe(el);
    });

    state.observers.push(observer);
  }

  /* ---------- Load Query ---------- */

  function loadQuery(benchmarkKey, queryId) {
    var data = getDataForBenchmark(benchmarkKey);
    if (!data) {
      console.warn('No data available for benchmark:', benchmarkKey);
      return;
    }

    var queryData = data.queries[queryId];
    if (!queryData) {
      console.warn('Query not found:', queryId);
      return;
    }

    var vizData = getVizData(benchmarkKey, queryId);
    var vizContainer = document.getElementById('viz-content');
    var legacyContainer = document.getElementById('legacy-stages');
    var pipelineNav = document.getElementById('pipeline-nav');

    if (vizData && vizContainer) {
      // Viz mode: render into unified container, hide header pipeline nav
      vizContainer.style.display = 'block';
      if (legacyContainer) legacyContainer.style.display = 'none';
      if (pipelineNav) pipelineNav.style.display = 'none';
      try {
        VizRenderer.render(vizContainer, vizData);
      } catch (err) {
        console.error('VizRenderer error:', err);
        vizContainer.innerHTML = '<div style="padding:80px 24px;color:red;">Viz render error: ' + err.message + '</div>';
      }
    } else {
      // Legacy mode: render into 7 stage sections, show header pipeline nav
      if (vizContainer) vizContainer.style.display = 'none';
      if (legacyContainer) legacyContainer.style.display = '';
      if (pipelineNav) pipelineNav.style.display = '';

      var containers = {
        sql:          document.getElementById('stage-sql-content'),
        workload:     document.getElementById('stage-workload-content'),
        storage:      document.getElementById('stage-storage-content'),
        plan:         document.getElementById('stage-plan-content'),
        code:         document.getElementById('stage-code-content'),
        optimization: document.getElementById('stage-optimization-content'),
        performance:  document.getElementById('stage-performance-content')
      };

      if (containers.sql) {
        Pipeline.renderSQLStage(containers.sql, queryData, data.queriesSQL);
      }
      if (containers.workload) {
        Pipeline.renderWorkloadStage(containers.workload, data.workloadAnalysis, queryData);
      }
      if (containers.storage) {
        Pipeline.renderStorageStage(containers.storage, data.storageDesign, queryData);
      }
      if (containers.plan) {
        Pipeline.renderPlanStage(containers.plan, queryData);
      }
      if (containers.code) {
        Pipeline.renderCodeStage(containers.code, queryData);
      }
      if (containers.optimization) {
        Pipeline.renderOptimizationStage(containers.optimization, queryData);
      }
      if (containers.performance) {
        Pipeline.renderPerformanceStage(containers.performance, data, queryId);
      }
    }

    // Reset scroll observer for new content
    setupScrollObserver();
    updatePipelineActive(0);
  }

  /* ---------- Public API ---------- */

  return {
    initApp: initApp,
    loadQuery: loadQuery,
    getDataForBenchmark: getDataForBenchmark,
    scrollToStage: scrollToStage,
    getState: function () { return state; }
  };
})();

/* ---------- Boot ---------- */
document.addEventListener('DOMContentLoaded', function () {
  App.initApp();
});
