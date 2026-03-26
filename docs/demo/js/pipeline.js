/* ============================================================
   GenDB Demo — pipeline.js
   Stage renderers for the 7-stage pipeline visualization.
   Each function receives a container and renders HTML into it.
   ============================================================ */

var Pipeline = (function () {
  'use strict';

  /* ---------- Helpers ---------- */

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function badge(text, variant) {
    var b = el('span', 'badge badge--' + (variant || 'default'), text);
    return b;
  }

  function card(title, variant) {
    var c = el('div', 'card' + (variant ? ' card--' + variant : ''));
    if (title) {
      c.appendChild(el('div', 'card-title', title));
    }
    return c;
  }

  function escapeHtml(str) {
    return (str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  /* Extract table names referenced in a SQL template */
  function extractQueryTables(sql) {
    if (!sql) return [];
    var tables = {};
    // Match FROM / JOIN table names (handles aliases)
    var pattern = /(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*)/gi;
    var m;
    while ((m = pattern.exec(sql)) !== null) {
      tables[m[1].toLowerCase()] = true;
    }
    // Also match comma-separated tables in FROM clause
    // Use word boundaries (\b) for terminators to avoid matching inside table names (e.g. "orders" contains "ORDER")
    var fromMatch = sql.match(/\bFROM\s+([\s\S]*?)(?=\bWHERE\b|\bGROUP\b|\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|;|$)/i);
    if (fromMatch) {
      fromMatch[1].split(',').forEach(function (part) {
        // Strip JOIN keywords and get just the table name
        var cleaned = part.replace(/\b(INNER|LEFT|RIGHT|OUTER|CROSS|NATURAL|FULL)?\s*JOIN\b/gi, '').trim();
        var tbl = cleaned.split(/\s+/)[0];
        if (tbl && /^[a-z_]/i.test(tbl)) tables[tbl.toLowerCase()] = true;
      });
    }
    return Object.keys(tables);
  }

  /* Check if a join involves any of the given tables */
  function joinInvolvesTable(join, tableSet) {
    var leftTbl = (join.left || '').split('.')[0].toLowerCase();
    var rightTbl = (join.right || '').split('.')[0].toLowerCase();
    return tableSet[leftTbl] || tableSet[rightTbl];
  }

  /* ============================================================
     Stage 1: SQL Query
     ============================================================ */

  function renderSQLStage(container, queryData, queriesSQL) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    // Extract query ID and description from template
    var template = queryData.template || '';
    var descMatch = template.match(/\/\*\s*(Q\d+):\s*([^*]+)\*\//);
    var queryId = descMatch ? descMatch[1] : '';
    var queryDesc = descMatch ? descMatch[2].trim() : '';

    // Title
    if (queryId || queryDesc) {
      var titleEl = el('h3', 'stage-subtitle', queryId + (queryDesc ? ': ' + queryDesc : ''));
      section.appendChild(titleEl);
    }

    // Metadata badges
    var badgeRow = el('div', 'badge-row');
    // Count tables from SQL
    var tableCount = 0;
    var fromMatch = template.match(/FROM\s+([\s\S]*?)(?:WHERE|GROUP|ORDER|LIMIT|HAVING|$)/i);
    if (fromMatch) {
      var fromClause = fromMatch[1];
      tableCount = fromClause.split(',').length;
      // Also count JOINs
      var joinMatches = template.match(/\bJOIN\b/gi);
      if (joinMatches) tableCount += joinMatches.length;
    }
    if (tableCount > 0) badgeRow.appendChild(badge(tableCount + ' tables', 'blue'));

    var joinMatches = template.match(/\bJOIN\b/gi);
    if (joinMatches) badgeRow.appendChild(badge(joinMatches.length + ' joins', 'purple'));

    if (/\b(SUM|COUNT|AVG|MIN|MAX|GROUP\s+BY)\b/i.test(template)) {
      badgeRow.appendChild(badge('Aggregation', 'green'));
    }
    if (/\bSELECT\b[\s\S]*\bSELECT\b/i.test(template)) {
      badgeRow.appendChild(badge('Subquery', 'orange'));
    }
    if (/\bORDER\s+BY\b/i.test(template)) {
      badgeRow.appendChild(badge('Sorting', 'gray'));
    }
    if (/\bHAVING\b/i.test(template)) {
      badgeRow.appendChild(badge('Having', 'gray'));
    }
    section.appendChild(badgeRow);

    // SQL code block
    var sqlCard = card('SQL Template');
    var codeBlock = el('div', 'sql-code');
    var sqlText = template;
    // Highlight with hljs if available
    if (typeof hljs !== 'undefined') {
      try {
        codeBlock.innerHTML = '<pre><code class="language-sql">' +
          hljs.highlight(sqlText, { language: 'sql' }).value + '</code></pre>';
      } catch (e) {
        codeBlock.innerHTML = '<pre><code>' + escapeHtml(sqlText) + '</code></pre>';
      }
    } else {
      codeBlock.innerHTML = '<pre><code>' + escapeHtml(sqlText) + '</code></pre>';
    }
    sqlCard.appendChild(codeBlock);
    section.appendChild(sqlCard);

    // Parameters
    var params = queryData.params;
    if (params && params.parameters && params.parameters.length) {
      var paramCard = card('Query Parameters');
      var paramTable = el('table', 'param-table');
      var thead = el('tr', '');
      ['Name', 'Type', 'Default Value', 'Description'].forEach(function (h) {
        thead.appendChild(el('th', '', h));
      });
      paramTable.appendChild(thead);
      params.parameters.forEach(function (p) {
        var row = el('tr', '');
        row.appendChild(el('td', 'param-name', p.name));
        row.appendChild(el('td', '', p.type));
        row.appendChild(el('td', 'param-val', String(p.default)));
        row.appendChild(el('td', '', p.description || ''));
        paramTable.appendChild(row);
      });
      paramCard.appendChild(paramTable);
      section.appendChild(paramCard);
    }

    container.appendChild(section);
  }

  /* ============================================================
     Stage 2: Workload Analysis
     ============================================================ */

  function renderWorkloadStage(container, workloadAnalysis, queryData) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    // Determine which tables this query uses
    var queryTables = extractQueryTables(queryData.template);
    var queryTableSet = {};
    queryTables.forEach(function (t) { queryTableSet[t] = true; });

    // Hardware profile
    var hw = workloadAnalysis.hardware || {};
    var hwCard = card('Hardware Profile', 'accent');
    var hwGrid = el('div', 'hw-grid');
    var hwItems = [
      { icon: 'cpu', label: 'CPU Cores', value: hw.cpu_cores || '—' },
      { icon: 'memory', label: 'Memory', value: (hw.memory_gb || '—') + ' GB' },
      { icon: 'cache', label: 'Cache', value: hw.cache_sizes || '—' },
      { icon: 'simd', label: 'SIMD', value: (hw.simd || '—').toUpperCase() },
      { icon: 'disk', label: 'Disk', value: (hw.disk_type || '—').toUpperCase() }
    ];
    hwItems.forEach(function (item) {
      var hwItem = el('div', 'hw-item');
      hwItem.appendChild(el('div', 'hw-label', item.label));
      hwItem.appendChild(el('div', 'hw-value', item.value));
      hwGrid.appendChild(hwItem);
    });
    hwCard.appendChild(hwGrid);
    section.appendChild(hwCard);

    // Data statistics — only tables used by this query
    var tables = workloadAnalysis.tables || {};
    var statsCard = card('Tables Used by This Query');
    var statsGrid = el('div', 'stats-grid');
    Object.keys(tables).forEach(function (tbl) {
      if (queryTables.length && !queryTableSet[tbl.toLowerCase()]) return;
      var t = tables[tbl];
      var statItem = el('div', 'stat-item');
      statItem.appendChild(el('div', 'stat-name', tbl));
      var countEl = el('div', 'stat-count animate-count');
      countEl.dataset.target = t.exact_row_count || 0;
      countEl.textContent = Charts.formatNumber(t.exact_row_count || 0);
      statItem.appendChild(countEl);
      var roleBadge = badge(t.role || 'table', t.role === 'fact' ? 'orange' : 'blue');
      statItem.appendChild(roleBadge);
      statsGrid.appendChild(statItem);
    });
    statsCard.appendChild(statsGrid);
    section.appendChild(statsCard);

    // Join graph — only joins involving this query's tables
    var allJoins = workloadAnalysis.joins || [];
    var joins = queryTables.length ? allJoins.filter(function (j) {
      return joinInvolvesTable(j, queryTableSet);
    }) : allJoins;
    if (joins.length > 0) {
      var joinCard = card('Join Relationships');
      var joinGraph = el('div', 'join-graph');
      joins.forEach(function (j) {
        var leftTbl = j.left.split('.')[0];
        var rightTbl = j.right.split('.')[0];
        var leftCol = j.left.split('.')[1] || '';
        var rightCol = j.right.split('.')[1] || '';
        var row = el('div', 'join-row');
        var leftBox = el('div', 'join-box');
        leftBox.appendChild(el('div', 'join-table-name', leftTbl));
        leftBox.appendChild(el('div', 'join-col-name', leftCol));
        row.appendChild(leftBox);

        var arrow = el('div', 'join-arrow');
        arrow.innerHTML = '<span class="join-type">' + escapeHtml(j.type || 'FK') + '</span>' +
          '<span class="join-line">&#8594;</span>' +
          '<span class="join-sel">' + (j.selectivity || '') + '</span>';
        row.appendChild(arrow);

        var rightBox = el('div', 'join-box');
        rightBox.appendChild(el('div', 'join-table-name', rightTbl));
        rightBox.appendChild(el('div', 'join-col-name', rightCol));
        row.appendChild(rightBox);

        joinGraph.appendChild(row);
      });
      joinCard.appendChild(joinGraph);
      section.appendChild(joinCard);
    }

    // Query-specific hot columns from guide
    if (queryData && queryData.guide) {
      var guideCard = card('Query Column Analysis');
      var guide = queryData.guide;
      // Parse column references from guide
      var colRefs = guide.match(/### (\S+)\s+\(([^)]+)\)/g);
      if (colRefs && colRefs.length) {
        var colList = el('div', 'col-ref-list');
        colRefs.slice(0, 12).forEach(function (ref) {
          var m = ref.match(/### (\S+)\s+\(([^)]+)\)/);
          if (m) {
            var item = el('div', 'col-ref-item');
            item.appendChild(el('span', 'col-ref-name', m[1]));
            var attrs = m[2].split(',').map(function (a) { return a.trim(); });
            attrs.forEach(function (a) {
              item.appendChild(badge(a, getTypeBadgeVariant(a)));
            });
            colList.appendChild(item);
          }
        });
        guideCard.appendChild(colList);
        section.appendChild(guideCard);
      }
    }

    container.appendChild(section);
    Charts.animateOnScroll(section.querySelectorAll('.card'));
  }

  function getTypeBadgeVariant(type) {
    if (/pk|primary/i.test(type)) return 'purple';
    if (/fk|foreign/i.test(type)) return 'blue';
    if (/int|float|double|decimal/i.test(type)) return 'green';
    if (/dict/i.test(type)) return 'orange';
    if (/date/i.test(type)) return 'teal';
    return 'gray';
  }

  /* ============================================================
     Stage 3: Storage Design
     ============================================================ */

  function renderStorageStage(container, storageDesign, queryData) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    // Determine which tables this query uses
    var queryTables = extractQueryTables(queryData.template);
    var queryTableSet = {};
    queryTables.forEach(function (t) { queryTableSet[t] = true; });

    var allTables = (storageDesign && storageDesign.tables) || {};
    // Filter to query-relevant tables
    var tables = {};
    Object.keys(allTables).forEach(function (tblName) {
      if (queryTables.length && !queryTableSet[tblName.toLowerCase()]) return;
      tables[tblName] = allTables[tblName];
    });

    Object.keys(tables).forEach(function (tblName) {
      var tbl = tables[tblName];
      var tblCard = card('');
      var titleRow = el('div', 'storage-title-row');
      titleRow.appendChild(el('span', 'storage-table-name', tblName));
      if (tbl.estimated_rows) {
        titleRow.appendChild(badge(Charts.formatNumber(tbl.estimated_rows) + ' rows', 'blue'));
      }
      if (tbl.sort_order && tbl.sort_order.length) {
        titleRow.appendChild(badge('Sort: ' + tbl.sort_order.join(', '), 'purple'));
      }
      if (tbl.block_size) {
        titleRow.appendChild(badge('Block: ' + Charts.formatNumber(tbl.block_size), 'gray'));
      }
      tblCard.appendChild(titleRow);

      // Columns
      if (tbl.columns && tbl.columns.length) {
        var colTable = el('table', 'col-table');
        var colHead = el('tr', '');
        ['Column', 'C++ Type', 'Semantic', 'Encoding'].forEach(function (h) {
          colHead.appendChild(el('th', '', h));
        });
        colTable.appendChild(colHead);
        tbl.columns.forEach(function (col) {
          var row = el('tr', '');
          row.appendChild(el('td', 'col-name-cell', col.name));
          row.appendChild(el('td', 'col-type-cell', col.cpp_type || ''));
          var semCell = el('td', '');
          if (col.semantic_type) semCell.appendChild(badge(col.semantic_type, getSemanticVariant(col.semantic_type)));
          row.appendChild(semCell);
          var encCell = el('td', '');
          if (col.encoding) encCell.appendChild(badge(col.encoding, getEncodingVariant(col.encoding)));
          row.appendChild(encCell);
          colTable.appendChild(row);
        });
        tblCard.appendChild(colTable);
      }

      // Indexes
      if (tbl.indexes && tbl.indexes.length) {
        var idxSection = el('div', 'index-section');
        idxSection.appendChild(el('div', 'index-heading', 'Indexes'));
        tbl.indexes.forEach(function (idx) {
          var idxItem = el('div', 'index-item');
          idxItem.appendChild(el('span', 'index-name', idx.name || 'index'));
          idxItem.appendChild(badge(idx.type || 'btree', 'teal'));
          if (idx.columns) {
            idxItem.appendChild(el('span', 'index-cols', '(' + (Array.isArray(idx.columns) ? idx.columns.join(', ') : idx.columns) + ')'));
          }
          if (idx.description) {
            idxItem.appendChild(el('div', 'index-desc', idx.description));
          }
          idxSection.appendChild(idxItem);
        });
        tblCard.appendChild(idxSection);
      }

      section.appendChild(tblCard);
    });

    // Columnar format animation concept
    var animCard = card('Storage Format');
    animCard.innerHTML += '<div class="storage-anim">' +
      '<div class="storage-anim-row">' +
        '<div class="storage-format-box storage-format-box--row">' +
          '<div class="format-label">Row Format</div>' +
          '<div class="format-cells">' +
            '<div class="format-row"><span class="c1">A1</span><span class="c2">B1</span><span class="c3">C1</span></div>' +
            '<div class="format-row"><span class="c1">A2</span><span class="c2">B2</span><span class="c3">C2</span></div>' +
            '<div class="format-row"><span class="c1">A3</span><span class="c2">B3</span><span class="c3">C3</span></div>' +
          '</div>' +
        '</div>' +
        '<div class="storage-arrow">&#10132;</div>' +
        '<div class="storage-format-box storage-format-box--col">' +
          '<div class="format-label">Columnar Format</div>' +
          '<div class="format-cells">' +
            '<div class="format-row"><span class="c1">A1</span><span class="c1">A2</span><span class="c1">A3</span></div>' +
            '<div class="format-row"><span class="c2">B1</span><span class="c2">B2</span><span class="c2">B3</span></div>' +
            '<div class="format-row"><span class="c3">C1</span><span class="c3">C2</span><span class="c3">C3</span></div>' +
          '</div>' +
        '</div>' +
      '</div>' +
    '</div>';
    section.appendChild(animCard);

    container.appendChild(section);
    Charts.animateOnScroll(section.querySelectorAll('.card'));
  }

  function getEncodingVariant(enc) {
    if (enc === 'raw') return 'gray';
    if (/dict/i.test(enc)) return 'blue';
    if (/days_since_epoch|date/i.test(enc)) return 'green';
    if (/rle/i.test(enc)) return 'orange';
    if (/delta/i.test(enc)) return 'purple';
    return 'teal';
  }

  function getSemanticVariant(sem) {
    if (sem === 'pk') return 'purple';
    if (sem === 'fk') return 'blue';
    if (/measure|amount|price|quantity/i.test(sem)) return 'green';
    if (/date|time/i.test(sem)) return 'teal';
    if (/category|flag|status/i.test(sem)) return 'orange';
    return 'gray';
  }

  /* ============================================================
     Stage 4: Execution Plan
     ============================================================ */

  function renderPlanStage(container, queryData) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    var iter0 = (queryData.iterations && queryData.iterations[0]) || {};
    var plan = iter0.plan || {};

    // Pipeline overview
    if (plan.pipeline && plan.pipeline.length) {
      var pipeCard = card('Execution Pipeline');
      var pipeFlow = el('div', 'pipe-flow');
      plan.pipeline.forEach(function (step, idx) {
        if (idx > 0) {
          pipeFlow.appendChild(el('span', 'pipe-arrow', '\u2192'));
        }
        var stepEl = el('span', 'pipe-step', step);
        pipeFlow.appendChild(stepEl);
      });
      pipeCard.appendChild(pipeFlow);
      section.appendChild(pipeCard);
    }

    // Execution steps (phases)
    var steps = plan.execution_steps || plan.execution_phases || [];
    if (Array.isArray(steps) && steps.length) {
      var phaseCard = card('Execution Phases');
      var phaseFlow = el('div', 'phase-flow');
      steps.forEach(function (step, idx) {
        var phaseEl = el('div', 'phase-box');
        var phaseHeader = el('div', 'phase-header');
        phaseHeader.appendChild(badge('Step ' + (step.step != null ? step.step : idx), 'blue'));
        phaseHeader.appendChild(el('span', 'phase-name', step.name || ''));
        phaseEl.appendChild(phaseHeader);

        if (step.operation) {
          phaseEl.appendChild(el('div', 'phase-desc', step.operation));
        }

        var phaseMeta = el('div', 'phase-meta');
        if (step.input_rows) phaseMeta.appendChild(badge('In: ' + Charts.formatNumber(step.input_rows), 'gray'));
        if (step.output_rows) phaseMeta.appendChild(badge('Out: ' + Charts.formatNumber(step.output_rows), 'green'));
        if (step.parallel) phaseMeta.appendChild(badge('Parallel', 'purple'));
        if (step.columns_read) {
          phaseMeta.appendChild(el('span', 'phase-cols', 'Reads: ' + step.columns_read.join(', ')));
        }
        phaseEl.appendChild(phaseMeta);

        phaseFlow.appendChild(phaseEl);

        if (idx < steps.length - 1) {
          phaseFlow.appendChild(el('div', 'phase-connector', '\u2193'));
        }
      });
      phaseCard.appendChild(phaseFlow);
      section.appendChild(phaseCard);
    }

    // Join order
    if (plan.join_order && plan.join_order.length) {
      var joinCard = card('Join Order');
      var joinFlow = el('div', 'join-order-flow');
      plan.join_order.forEach(function (j, idx) {
        var joinEl = el('div', 'join-order-item');
        var buildProbe = el('div', 'join-bp');
        var buildBox = el('div', 'join-bp-box join-bp-build');
        buildBox.appendChild(el('div', 'join-bp-label', 'BUILD'));
        buildBox.appendChild(el('div', 'join-bp-value', j.build || ''));
        if (j.build_cardinality) buildBox.appendChild(badge(Charts.formatNumber(j.build_cardinality) + ' rows', 'blue'));
        buildProbe.appendChild(buildBox);

        buildProbe.appendChild(el('span', 'join-bp-arrow', '\u2192'));

        var probeBox = el('div', 'join-bp-box join-bp-probe');
        probeBox.appendChild(el('div', 'join-bp-label', 'PROBE'));
        probeBox.appendChild(el('div', 'join-bp-value', j.probe || ''));
        if (j.probe_cardinality) probeBox.appendChild(badge(Charts.formatNumber(j.probe_cardinality) + ' rows', 'gray'));
        buildProbe.appendChild(probeBox);

        joinEl.appendChild(buildProbe);

        var stratRow = el('div', 'join-strat-row');
        stratRow.appendChild(badge(j.strategy || 'hash', 'teal'));
        stratRow.appendChild(badge(j.type || 'inner', 'purple'));
        joinEl.appendChild(stratRow);

        if (j.notes) joinEl.appendChild(el('div', 'join-notes', j.notes));
        joinFlow.appendChild(joinEl);
      });
      joinCard.appendChild(joinFlow);
      section.appendChild(joinCard);
    }

    // Filters
    if (plan.filters && plan.filters.length) {
      var filterCard = card('Filter Selectivities');
      plan.filters.forEach(function (f) {
        var fRow = el('div', 'filter-row');
        fRow.appendChild(el('div', 'filter-pred', f.predicate || (f.column + ' filter')));

        var barTrack = el('div', 'filter-bar-track');
        var barFill = el('div', 'filter-bar-fill');
        var selectivity = f.selectivity || 0;
        barFill.style.width = (selectivity * 100) + '%';
        barFill.style.backgroundColor = selectivity < 0.3 ? '#10b981' : selectivity < 0.6 ? '#f59e0b' : '#ef4444';
        barTrack.appendChild(barFill);
        fRow.appendChild(barTrack);

        fRow.appendChild(el('span', 'filter-pct', (selectivity * 100).toFixed(0) + '%'));
        if (f.output_rows) {
          fRow.appendChild(el('span', 'filter-rows', Charts.formatNumber(f.output_rows) + ' rows'));
        }
        filterCard.appendChild(fRow);
      });
      section.appendChild(filterCard);
    }

    // Data structures
    if (plan.data_structures) {
      var dsCard = card('Data Structures');
      var ds = plan.data_structures;
      Object.keys(ds).forEach(function (key) {
        var val = ds[key];
        var dsItem = el('div', 'ds-item');
        dsItem.appendChild(el('div', 'ds-name', key));
        if (typeof val === 'string') {
          dsItem.appendChild(el('div', 'ds-detail', val));
        } else if (typeof val === 'object') {
          if (val.type) dsItem.appendChild(badge(val.type, 'blue'));
          if (val.description) dsItem.appendChild(el('div', 'ds-detail', val.description));
          if (val.estimated_size) dsItem.appendChild(badge(val.estimated_size, 'green'));
          if (val.notes) dsItem.appendChild(el('div', 'ds-notes', val.notes));
        }
        dsCard.appendChild(dsItem);
      });
      section.appendChild(dsCard);
    }

    // Parallelism
    if (plan.parallelism) {
      var parCard = card('Parallelism Strategy');
      var par = plan.parallelism;
      var parGrid = el('div', 'par-grid');
      Object.keys(par).forEach(function (key) {
        var item = el('div', 'par-item');
        item.appendChild(el('div', 'par-key', key.replace(/_/g, ' ')));
        item.appendChild(el('div', 'par-val', String(par[key])));
        parGrid.appendChild(item);
      });
      parCard.appendChild(parGrid);
      section.appendChild(parCard);
    }

    container.appendChild(section);
    Charts.animateOnScroll(section.querySelectorAll('.card'));
  }

  /* ============================================================
     Stage 5: Generated Code
     ============================================================ */

  function renderCodeStage(container, queryData) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    var iter0 = (queryData.iterations && queryData.iterations[0]) || {};
    var code = iter0.code || queryData.bestCode || '// No code available';

    var codeCard = card('Generated C++ Code (Initial)');
    CodeViewer.createCodeViewer(codeCard, {
      code: code,
      language: 'cpp',
      lineNumbers: true,
      maxHeight: 600,
      collapsible: true,
      collapsedLines: 35,
      title: 'Iteration 0 — Initial Generation'
    });
    section.appendChild(codeCard);

    container.appendChild(section);
  }

  /* ============================================================
     Stage 6: Optimization
     ============================================================ */

  function renderOptimizationStage(container, queryData) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    var optHistory = queryData.optimizationHistory || {};
    var iterations = optHistory.iterations || [];
    var codeIters = queryData.iterations || [];

    if (!iterations.length) {
      section.appendChild(el('p', 'empty-msg', 'No optimization history available.'));
      container.appendChild(section);
      return;
    }

    // Speedup callout
    if (iterations.length > 1) {
      var first = iterations[0].timing_ms;
      var last = iterations[iterations.length - 1].timing_ms;
      var speedup = first / last;
      var callout = el('div', 'speedup-callout');
      callout.innerHTML = '<span class="speedup-value">' + speedup.toFixed(1) + 'x</span>' +
        '<span class="speedup-label">improvement</span>' +
        '<span class="speedup-detail">' + Charts.formatMs(first) + ' &rarr; ' + Charts.formatMs(last) + '</span>';
      section.appendChild(callout);
    }

    // Iteration timeline chart
    var timelineCard = card('Iteration Timeline');
    Charts.createIterationTimeline(timelineCard, iterations);
    section.appendChild(timelineCard);

    // Iteration detail cards
    var detailCard = card('Iteration Details');
    iterations.forEach(function (it, idx) {
      var iterEl = el('div', 'iter-detail');

      var iterHeader = el('div', 'iter-detail-header');
      iterHeader.appendChild(badge('Iter ' + it.iteration, 'blue'));
      iterHeader.appendChild(badge(Charts.formatMs(it.timing_ms), it.validation === 'pass' ? 'green' : 'red'));
      if (it.validation !== 'pass') iterHeader.appendChild(badge('FAILED', 'red'));
      if (it.categories) {
        it.categories.forEach(function (cat) {
          iterHeader.appendChild(badge(cat, 'purple'));
        });
      }
      iterEl.appendChild(iterHeader);

      // Strategy
      if (it.strategy) {
        var stratEl = el('div', 'iter-strategy');
        var stratToggle = el('button', 'iter-strategy-toggle', 'Strategy');
        var stratContent = el('div', 'iter-strategy-content');
        stratContent.textContent = it.strategy;
        stratContent.style.display = idx === 0 ? 'block' : 'none';
        stratToggle.addEventListener('click', (function (content) {
          return function () {
            content.style.display = content.style.display === 'none' ? 'block' : 'none';
          };
        })(stratContent));
        stratEl.appendChild(stratToggle);
        stratEl.appendChild(stratContent);
        iterEl.appendChild(stratEl);
      }

      // Operation timing breakdown
      if (it.operation_timings) {
        var opTimings = it.operation_timings;
        var segments = [];
        var OP_COLORS = ['#6366f1', '#8b5cf6', '#10b981', '#f59e0b', '#3b82f6', '#ec4899', '#14b8a6', '#f97316', '#64748b'];
        var colorIdx = 0;
        Object.keys(opTimings).forEach(function (key) {
          if (key !== 'total' && key !== 'output') {
            segments.push({
              label: key.replace(/_/g, ' '),
              value: opTimings[key],
              color: OP_COLORS[colorIdx % OP_COLORS.length]
            });
            colorIdx++;
          }
        });
        if (segments.length) {
          var opBar = el('div', 'iter-op-bar');
          Charts.createStackedBar(opBar, segments);
          iterEl.appendChild(opBar);
        }
      }

      detailCard.appendChild(iterEl);
    });
    section.appendChild(detailCard);

    // Tabbed code viewer for iteration versions
    if (codeIters.length > 1) {
      var codeCard = card('Code Versions');
      var tabs = codeIters.map(function (it, idx) {
        return {
          label: 'Iteration ' + it.iteration,
          code: it.code || '// No code',
          active: idx === codeIters.length - 1
        };
      });
      // Also add best code
      if (queryData.bestCode) {
        tabs.push({
          label: 'Best (Final)',
          code: queryData.bestCode,
          active: false
        });
      }
      CodeViewer.createTabbedCodeViewer(codeCard, tabs);
      section.appendChild(codeCard);
    }

    container.appendChild(section);
    Charts.animateOnScroll(section.querySelectorAll('.card'));
  }

  /* ============================================================
     Stage 7: Performance Comparison
     ============================================================ */

  function renderPerformanceStage(container, data, queryId) {
    container.innerHTML = '';
    var section = el('div', 'stage-content');

    var benchResults = data.benchmarkResults || {};

    // Per-query bar chart
    var queryCard = card(queryId + ' Performance');
    var chartData = [];
    var gendbMs = 0;

    // Order: GenDB first, then others alphabetically
    var systems = Object.keys(benchResults).sort(function (a, b) {
      if (a === 'GenDB') return -1;
      if (b === 'GenDB') return 1;
      return a.localeCompare(b);
    });

    systems.forEach(function (sys) {
      var result = benchResults[sys][queryId];
      if (!result) return;
      var avgMs = result.average_ms;
      if (sys === 'GenDB') gendbMs = avgMs;
      chartData.push({
        label: sys,
        value: avgMs,
        highlight: sys === 'GenDB',
        color: sys === 'GenDB' ? '#10b981' : '#94a3b8'
      });
    });

    Charts.createBarChart(queryCard, chartData, {
      horizontal: true,
      animated: true,
      unit: 'ms',
      formatValue: Charts.formatMs
    });

    // Speedup badges
    if (gendbMs > 0) {
      var speedupRow = el('div', 'speedup-row');
      systems.forEach(function (sys) {
        if (sys === 'GenDB') return;
        var result = benchResults[sys][queryId];
        if (!result) return;
        var ratio = result.average_ms / gendbMs;
        if (ratio > 1) {
          var sBadge = el('span', 'speedup-badge', ratio.toFixed(1) + 'x faster than ' + sys);
          speedupRow.appendChild(sBadge);
        }
      });
      queryCard.appendChild(speedupRow);
    }
    section.appendChild(queryCard);

    // Overall workload total
    var totalCard = card('Full Workload Total');
    var totalData = [];
    var gendbTotal = 0;
    systems.forEach(function (sys) {
      var sysResults = benchResults[sys];
      var total = 0;
      var count = 0;
      Object.keys(sysResults).forEach(function (q) {
        if (sysResults[q] && sysResults[q].average_ms != null) {
          total += sysResults[q].average_ms;
          count++;
        }
      });
      if (count > 0) {
        if (sys === 'GenDB') gendbTotal = total;
        totalData.push({
          label: sys + ' (' + count + 'q)',
          value: total,
          highlight: sys === 'GenDB',
          color: sys === 'GenDB' ? '#10b981' : '#94a3b8'
        });
      }
    });

    Charts.createBarChart(totalCard, totalData, {
      horizontal: true,
      animated: true,
      unit: 'ms',
      formatValue: Charts.formatMs
    });

    // Total speedup badges
    if (gendbTotal > 0) {
      var totalSpeedupRow = el('div', 'speedup-row');
      systems.forEach(function (sys) {
        if (sys === 'GenDB') return;
        var sysResults = benchResults[sys];
        var total = 0;
        Object.keys(sysResults).forEach(function (q) {
          if (sysResults[q] && sysResults[q].average_ms != null) total += sysResults[q].average_ms;
        });
        if (total > 0) {
          var ratio = total / gendbTotal;
          if (ratio > 1) {
            totalSpeedupRow.appendChild(el('span', 'speedup-badge', ratio.toFixed(1) + 'x faster than ' + sys));
          }
        }
      });
      totalCard.appendChild(totalSpeedupRow);
    }
    section.appendChild(totalCard);

    container.appendChild(section);
    Charts.animateOnScroll(section.querySelectorAll('.chart'));
  }

  /* ---------- Public API ---------- */

  return {
    renderSQLStage: renderSQLStage,
    renderWorkloadStage: renderWorkloadStage,
    renderStorageStage: renderStorageStage,
    renderPlanStage: renderPlanStage,
    renderCodeStage: renderCodeStage,
    renderOptimizationStage: renderOptimizationStage,
    renderPerformanceStage: renderPerformanceStage
  };
})();
