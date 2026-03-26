/* ============================================================
   GenDB Demo — server-mode.js
   Detects whether a GenDB server is running. When online,
   enables the Generate buttons. When offline, shows notices
   and keeps buttons disabled.
   ============================================================ */

var ServerMode = (function () {
  'use strict';

  var serverAvailable = false;
  var activeRunId = null;
  var pollTimer = null;

  /* ---------- initServerMode ---------- */

  function initServerMode() {
    var statusEl = document.getElementById('server-status');

    // Try to detect server
    fetch('/api/status', { method: 'GET' })
      .then(function (res) {
        if (!res.ok) throw new Error('Not available');
        return res.json();
      })
      .then(function () {
        serverAvailable = true;
        if (statusEl) {
          statusEl.classList.remove('offline');
          statusEl.classList.add('online');
          statusEl.querySelector('.status-text').textContent = 'Server Online';
        }
        // Enable generate buttons and hide notices
        setGenerateButtons(true);
        hideServerNotices();
      })
      .catch(function () {
        serverAvailable = false;
        // Show server notices on generation tabs
        showServerNotices();
        setGenerateButtons(false);
      });

    // Set up provider/model switching
    setupProviderModelSync();
    // Set up generate button handlers
    setupGenerateHandlers();
    // Set up file upload interaction
    setupFileUpload();
  }

  /* ---------- Server notices ---------- */

  function showServerNotices() {
    var notices = document.querySelectorAll('.server-notice');
    notices.forEach(function (n) { n.classList.remove('hidden'); });
  }

  function hideServerNotices() {
    var notices = document.querySelectorAll('.server-notice');
    notices.forEach(function (n) { n.classList.add('hidden'); });
  }

  function setGenerateButtons(enabled) {
    var btns = document.querySelectorAll('.btn-generate');
    btns.forEach(function (btn) {
      if (enabled) {
        btn.disabled = false;
        btn.title = '';
      } else {
        btn.disabled = true;
        btn.title = 'Start a GenDB server to enable live generation';
      }
    });
  }

  /* ---------- Provider/Model sync ---------- */

  var MODEL_OPTIONS = {
    claude: [
      { value: 'claude-opus-4-6', label: 'Claude Opus 4.6' },
      { value: 'claude-sonnet-4-6', label: 'Claude Sonnet 4.6' }
    ],
    openai: [
      { value: 'gpt-5.4', label: 'GPT-5.4' },
      { value: 'gpt-5.3-codex', label: 'GPT-5.3-Codex' }
    ]
  };

  function setupProviderModelSync() {
    setupProviderSelect('gen-provider', 'gen-model');
    setupProviderSelect('custom-provider', 'custom-model');
  }

  function setupProviderSelect(providerId, modelId) {
    var providerEl = document.getElementById(providerId);
    var modelEl = document.getElementById(modelId);
    if (!providerEl || !modelEl) return;

    providerEl.addEventListener('change', function () {
      var provider = providerEl.value;
      var models = MODEL_OPTIONS[provider] || [];
      modelEl.innerHTML = '';
      models.forEach(function (m) {
        var opt = document.createElement('option');
        opt.value = m.value;
        opt.textContent = m.label;
        modelEl.appendChild(opt);
      });
    });
  }

  /* ---------- Generate handlers ---------- */

  function setupGenerateHandlers() {
    var btnExisting = document.getElementById('btn-generate-existing');
    var btnCustom = document.getElementById('btn-generate-custom');

    if (btnExisting) {
      btnExisting.addEventListener('click', function () {
        if (!serverAvailable) return;
        var benchmark = document.getElementById('gen-benchmark').value;
        var query = document.getElementById('new-query-sql').value;
        var provider = document.getElementById('gen-provider').value;
        var model = document.getElementById('gen-model').value;
        var apiKey = (document.getElementById('gen-api-key').value || '').trim();
        if (!query.trim()) {
          alert('Please enter a SQL query.');
          return;
        }
        var config = {
          mode: 'existing',
          benchmark: benchmark.split('-')[0],
          sf: benchmark.split('-')[1] || '10',
          query: query,
          provider: provider,
          model: model
        };
        if (apiKey) config.apiKey = apiKey;
        startGeneration(config);
      });
    }

    if (btnCustom) {
      btnCustom.addEventListener('click', function () {
        if (!serverAvailable) return;
        var schema = document.getElementById('custom-schema').value;
        var sql = document.getElementById('custom-sql').value;
        var provider = document.getElementById('custom-provider').value;
        var model = document.getElementById('custom-model').value;
        var apiKey = (document.getElementById('custom-api-key').value || '').trim();
        if (!schema.trim() || !sql.trim()) {
          alert('Please provide both a schema and a SQL query.');
          return;
        }
        var config = {
          mode: 'custom',
          schema: schema,
          sql: sql,
          provider: provider,
          model: model
        };
        if (apiKey) config.apiKey = apiKey;
        startGeneration(config);
      });
    }
  }

  /* ---------- File upload ---------- */

  function setupFileUpload() {
    var dropzone = document.getElementById('csv-upload');
    var fileInput = document.getElementById('csv-files');
    if (!dropzone || !fileInput) return;

    dropzone.addEventListener('click', function () { fileInput.click(); });

    dropzone.addEventListener('dragover', function (e) {
      e.preventDefault();
      dropzone.classList.add('dragover');
    });
    dropzone.addEventListener('dragleave', function () {
      dropzone.classList.remove('dragover');
    });
    dropzone.addEventListener('drop', function (e) {
      e.preventDefault();
      dropzone.classList.remove('dragover');
      if (e.dataTransfer.files.length) {
        fileInput.files = e.dataTransfer.files;
        updateFileList(e.dataTransfer.files);
      }
    });
    fileInput.addEventListener('change', function () {
      updateFileList(fileInput.files);
    });
  }

  function updateFileList(files) {
    var dropzone = document.getElementById('csv-upload');
    if (!dropzone) return;
    var existing = dropzone.querySelector('.file-list');
    if (existing) existing.remove();

    if (!files.length) return;
    var list = document.createElement('div');
    list.className = 'file-list';
    for (var i = 0; i < files.length; i++) {
      var item = document.createElement('div');
      item.className = 'file-item';
      item.textContent = files[i].name + ' (' + (files[i].size / 1024).toFixed(1) + ' KB)';
      list.appendChild(item);
    }
    dropzone.appendChild(list);
  }

  /* ---------- Generation & Polling ---------- */

  function startGeneration(config) {
    var progressArea = document.getElementById('progress-area');
    var progressFill = document.getElementById('progress-fill');
    var progressStatus = document.getElementById('progress-status');

    if (progressArea) progressArea.classList.remove('hidden');
    if (progressFill) progressFill.style.width = '0%';
    if (progressStatus) progressStatus.textContent = 'Starting generation...';

    // Disable buttons during generation
    setGenerateButtons(false);

    fetch('/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config)
    })
    .then(function (res) {
      if (res.status === 409) {
        if (progressStatus) progressStatus.textContent = 'A generation is already in progress. Please wait.';
        setGenerateButtons(true);
        throw new Error('busy');
      }
      if (!res.ok) throw new Error('Request failed: ' + res.status);
      return res.json();
    })
    .then(function (data) {
      activeRunId = data.runId;
      if (progressStatus) progressStatus.textContent = 'Generation started (Run: ' + data.runId + ')';
      startPolling(data.runId);
    })
    .catch(function (err) {
      if (err.message !== 'busy') {
        if (progressStatus) progressStatus.textContent = 'Error: ' + err.message;
        setGenerateButtons(true);
      }
    });
  }

  function startPolling(runId) {
    stopPolling();
    pollTimer = setInterval(function () {
      fetch('/api/status/' + runId)
        .then(function (res) { return res.json(); })
        .then(function (status) {
          var progressFill = document.getElementById('progress-fill');
          var progressStatus = document.getElementById('progress-status');

          // Estimate progress from phases
          var progress = 0;
          var message = 'Running...';
          if (status.phase1) {
            if (status.phase1.status === 'completed') progress = 30;
            else progress = 15;
            message = 'Phase 1: Workload Analysis & Storage Design...';
          }
          if (status.phase2) {
            if (status.phase2.status === 'completed') progress = 90;
            else {
              var totalQ = (status.phase2.queries || []).length;
              var doneQ = Object.keys(status.phase2.pipelines || {}).filter(function (q) {
                return status.phase2.pipelines[q].status === 'completed';
              }).length;
              progress = 30 + (doneQ / Math.max(totalQ, 1)) * 60;
              message = 'Phase 2: Generating code (' + doneQ + '/' + totalQ + ' queries)...';
            }
          }
          if (status.status === 'completed') {
            progress = 100;
            message = 'Generation complete!';
            stopPolling();
            setGenerateButtons(true);
            // Fetch and display results
            fetchAndDisplayResults(runId);
          }

          if (progressFill) progressFill.style.width = progress + '%';
          if (progressStatus) progressStatus.textContent = message;
        })
        .catch(function () {
          // Silently retry on network errors
        });
    }, 2000);
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function fetchAndDisplayResults(runId) {
    fetch('/api/results/' + runId)
      .then(function (res) { return res.json(); })
      .then(function (data) {
        var progressStatus = document.getElementById('progress-status');
        if (progressStatus) {
          progressStatus.innerHTML = 'Generation complete! Results available. ' +
            '<a href="#stage-sql" style="color:var(--indigo);font-weight:600;">View pipeline results below &#8595;</a>';
        }
        // TODO: Load the generated results into the pipeline viewer
        console.log('Generation results:', data);
      })
      .catch(function (err) {
        console.error('Failed to fetch results:', err);
      });
  }

  /* ---------- Public API ---------- */

  return {
    initServerMode: initServerMode,
    isAvailable: function () { return serverAvailable; },
    stopPolling: stopPolling
  };
})();
