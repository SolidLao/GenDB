/* ============================================================
   GenDB Demo — viz-renderer.js
   Two-column progressive canvas.
   Left: SQL + Code. Right: Plan tree + Tables.
   ============================================================ */
var VizRenderer = (function () {
  'use strict';
  var agents = [], selectedIdx = 0, canvasEl = null, detailEl = null, svgEl = null, canvasData = null;

  function el(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; }
  function esc(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  var LAYERS = ['sql','wa','sd','qp','cg','opt'];
  var STEP_NUMS = { sql: '①', wa: '②', sd: '③', qp: '④', cg: '⑤', opt: '⑥' };
  var AGENT_COLORS = { sql: '#6b7280', wa: '#6366f1', sd: '#059669', qp: '#d97706', cg: '#8b5cf6', opt: '#e11d48' };

  function stepBadge(agentId) {
    var b = el('span', 'cv-step-num');
    b.style.color = AGENT_COLORS[agentId];
    b.textContent = STEP_NUMS[agentId];
    return b;
  }

  /* ═══ Main ═══ */
  function render(container, vizData) {
    container.innerHTML = '';
    agents = vizData.agents || [];
    canvasData = vizData.canvas;
    selectedIdx = 0;
    var wrap = el('div', 'viz-container');

    // Pipeline bar
    var pSec = el('div', 'viz-pipeline-section');
    buildPipelineBar(pSec);
    wrap.appendChild(pSec);

    // Canvas (two-column)
    canvasEl = el('div', 'canvas');
    buildCanvas(canvasEl, canvasData);
    wrap.appendChild(canvasEl);

    // Detail panel
    detailEl = el('div', 'agent-detail-panel');
    wrap.appendChild(detailEl);

    // Benchmark
    if (vizData.benchmark) {
      var bench = el('div', 'viz-bench-section');
      buildBenchmark(bench, vizData.benchmark, canvasData.speedup);
      wrap.appendChild(bench);
    }
    container.appendChild(wrap);
    selectAgent(0, false);
    // Don't draw edges on initial load — only when QP is shown
  }

  /* ═══ Pipeline Bar ═══ */
  function buildPipelineBar(container) {
    var bar = el('div', 'pipeline-bar');
    agents.forEach(function (a, i) {
      if (i > 0) {
        var c = el('div', 'pipeline-connector');
        c.innerHTML = '<svg width="28" height="12" viewBox="0 0 28 12"><path d="M0 6L20 6M16 2L20 6L16 10" stroke="currentColor" stroke-width="1.5" fill="none" stroke-linecap="round"/></svg>';
        bar.appendChild(c);
      }
      var n = el('div', 'pipeline-node' + (i === 0 ? ' selected' : ' dimmed'));
      n.dataset.idx = i;
      n.style.setProperty('--node-color', a.color);
      var circ = el('div', 'pn-circle');
      circ.textContent = a.shortName;
      circ.style.backgroundColor = a.color;
      n.appendChild(circ);
      var lbl = el('div', 'pn-label');
      lbl.innerHTML = '<span class="pn-step">' + STEP_NUMS[a.id] + '</span> ' + a.name;
      n.appendChild(lbl);
      n.addEventListener('click', function () { selectAgent(i, true); });
      bar.appendChild(n);
    });
    container.appendChild(bar);
  }

  /* ═══ Select Agent ═══ */
  function selectAgent(idx, animate) {
    selectedIdx = idx;
    var aid = agents[idx].id;
    // Pipeline states
    document.querySelectorAll('.pipeline-node').forEach(function (n) {
      var i = +n.dataset.idx;
      n.classList.remove('selected','dimmed','completed');
      n.classList.add(i === idx ? 'selected' : i < idx ? 'completed' : 'dimmed');
    });
    document.querySelectorAll('.pipeline-connector').forEach(function (c, i) { c.classList.toggle('active', i < idx); });
    // Layer visibility
    if (canvasEl) {
      LAYERS.forEach(function (l, li) { canvasEl.classList.toggle('show-' + l, li <= idx); });
      LAYERS.forEach(function (l) { canvasEl.classList.remove('active-' + l); });
      canvasEl.classList.add('active-' + aid);
    }
    // Redraw SVG edges when QP tree is visible (idx >= 3)
    // Clear edges if QP not visible, redraw with delay for CSS transitions
    if (idx >= 3) {
      setTimeout(function () { redrawEdges(); }, 400);
    } else if (svgEl) {
      svgEl.innerHTML = '';
    }
    // Detail panel
    if (detailEl) {
      if (animate) {
        detailEl.classList.add('fading');
        setTimeout(function () { buildDetail(detailEl, agents[idx]); detailEl.classList.remove('fading'); }, 220);
      } else {
        buildDetail(detailEl, agents[idx]);
      }
    }
  }

  /* ═══ Build Canvas ═══ */
  function buildCanvas(container, c) {
    var left = el('div', 'cv-left');
    var right = el('div', 'cv-right');

    // ── LEFT: SQL ──
    var sqlWrap = el('div', 'cv-sql cv-layer-sql');
    var sqlHdr = el('div', 'cv-section-hdr cv-agent-sql');
    sqlHdr.innerHTML = '<span class="cv-agent-dot" style="background:#6b7280"></span>SQL Query';
    sqlHdr.appendChild(stepBadge('sql'));
    sqlWrap.appendChild(sqlHdr);
    var pre = document.createElement('pre');
    pre.className = 'cv-sql-pre';
    var code = document.createElement('code');
    code.className = 'language-sql';
    code.innerHTML = overlayHL(syntaxHL(c.sql), c.sql, c.params || []);
    pre.appendChild(code);
    sqlWrap.appendChild(pre);
    // Hint: template for reuse
    var hint = el('div', 'cv-sql-hint');
    hint.innerHTML = 'Parameterized as <strong>SQL template</strong> for reuse across predicate values';
    sqlWrap.appendChild(hint);
    left.appendChild(sqlWrap);

    // ── LEFT: Code preview (CG layer) ──
    var codeWrap = el('div', 'cv-gencode cv-layer-cg');
    var codeHdr = el('div', 'cv-section-hdr cv-agent-cg');
    codeHdr.innerHTML = '<span class="cv-agent-dot" style="background:#8b5cf6"></span>Generated C++ <span class="cv-code-lines">' + (c.codeLines || 289) + ' lines</span>';
    codeHdr.appendChild(stepBadge('cg'));
    codeWrap.appendChild(codeHdr);
    var codeStr = getCode('tpch', 'Q3', 0);
    if (codeStr) {
      var codePre = document.createElement('pre');
      codePre.className = 'cv-code-pre';
      var codeEl = document.createElement('code');
      codeEl.className = 'language-cpp';
      if (typeof hljs !== 'undefined') {
        try { codeEl.innerHTML = hljs.highlight(codeStr, { language: 'cpp' }).value; }
        catch (e) { codeEl.textContent = codeStr; }
      } else { codeEl.textContent = codeStr; }
      codePre.appendChild(codeEl);
      codeWrap.appendChild(codePre);
    }
    left.appendChild(codeWrap);

    // SVG overlay for edges (on right column so it spans tree→tables)
    svgEl = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svgEl.setAttribute('class', 'cv-svg');
    right.appendChild(svgEl);

    // ── RIGHT: Operator tree (QP layer) ──
    var treeWrap = el('div', 'cv-tree');
    treeWrap.setAttribute('data-layer', 'qp');
    var treeHdr = el('div', 'cv-section-hdr cv-agent-qp');
    treeHdr.innerHTML = '<span class="cv-agent-dot" style="background:#d97706"></span>Execution Plan';
    treeHdr.appendChild(stepBadge('qp'));
    treeWrap.appendChild(treeHdr);
    var tnodes = el('div', 'cv-tnodes');
    var opMap = {};
    (c.operators || []).forEach(function (o) { opMap[o.id] = o; });
    var childSet = {};
    (c.operators || []).forEach(function (o) { (o.children||[]).forEach(function (ci) { childSet[ci] = true; }); });
    var rootId = null;
    (c.operators || []).forEach(function (o) { if (!childSet[o.id]) rootId = o.id; });
    if (rootId) buildOpNode(tnodes, opMap[rootId], opMap, c);
    treeWrap.appendChild(tnodes);
    right.appendChild(treeWrap);

    // ── RIGHT: Tables (WA layer) ──
    var tblHdr = el('div', 'cv-section-hdr cv-agent-wa');
    tblHdr.setAttribute('data-layer', 'wa');
    tblHdr.innerHTML = '<span class="cv-agent-dot" style="background:#6366f1"></span>Data Profile';
    tblHdr.appendChild(stepBadge('wa'));
    right.appendChild(tblHdr);

    var tblRow = el('div', 'cv-tables');
    tblRow.setAttribute('data-layer', 'wa');
    (c.tables || []).forEach(function (tbl, ti) {
      if (ti > 0 && c.joins[ti - 1]) {
        var jc = el('div', 'cv-jconn');
        jc.innerHTML = '<span class="cv-jarrow">\u2194</span><span class="cv-jlabel">' + esc(c.joins[ti-1].label) + '</span>';
        tblRow.appendChild(jc);
      }
      var group = el('div', 'cv-tbl-group');
      var box = el('div', 'cv-tbl cv-role-' + tbl.role);
      box.setAttribute('data-table', tbl.id);
      var hdr = el('div', 'cv-tbl-hdr');
      hdr.appendChild(el('span', 'cv-tbl-name', tbl.name));
      hdr.appendChild(el('span', 'cv-tbl-role', tbl.role));
      box.appendChild(hdr);
      box.appendChild(el('div', 'cv-tbl-rows', Charts.formatNumber(tbl.rows) + ' rows'));
      // Selectivity
      if (tbl.predicate) {
        var sf = el('div', 'cv-sel-section');
        sf.appendChild(el('div', 'cv-sel-pred', tbl.predicate));
        var bar = el('div', 'cv-sel-track');
        var fill = el('div', 'cv-sel-fill');
        fill.style.width = Math.round(tbl.selectivity * 100) + '%';
        bar.appendChild(fill);
        sf.appendChild(bar);
        var stat = el('div', 'cv-sel-stat');
        stat.innerHTML = Charts.formatNumber(tbl.rows) + ' <span class="cv-sel-arrow">\u2192</span> <strong>' +
          Charts.formatNumber(tbl.outputRows) + '</strong> <span class="cv-sel-pct">(' + Math.round(tbl.selectivity * 100) + '%)</span>';
        sf.appendChild(stat);
        box.appendChild(sf);
      }
      group.appendChild(box);
      // SD: encoding badges — standalone blocks below table, connected by group line
      (c.encodings || []).forEach(function (enc) {
        if (enc.table === tbl.id) {
          var b = el('div', 'cv-badge cv-badge-enc');
          b.setAttribute('data-layer', 'sd');
          b.innerHTML = '<span class="cv-step-micro cv-step-sd">\u2462</span><span class="cv-badge-tag">ENC</span> ' +
            esc(enc.col) + ': <strong>' + esc(enc.enc) + '</strong> \u2014 ' +
            '<span class="cv-badge-reason">' + esc(enc.why) + '</span>';
          group.appendChild(b);
        }
      });
      // SD: index badges
      (c.indexes || []).forEach(function (idx) {
        if (idx.table === tbl.id) {
          var b = el('div', 'cv-badge cv-badge-idx');
          b.setAttribute('data-layer', 'sd');
          b.innerHTML = '<span class="cv-step-micro cv-step-sd">\u2462</span><span class="cv-badge-tag">IDX</span> ' +
            (idx.mapping ? '<span class="cv-idx-mapping">' + esc(idx.mapping) + '</span>' : esc(idx.name)) +
            '<br><span class="cv-badge-reason">' + esc(idx.why) + '</span>';
          group.appendChild(b);
        }
      });
      tblRow.appendChild(group);
    });
    right.appendChild(tblRow);

    // ── RIGHT: Hardware (WA layer) ──
    if (c.hardware) {
      var hwHdr = el('div', 'cv-section-hdr cv-agent-wa');
      hwHdr.setAttribute('data-layer', 'wa');
      hwHdr.innerHTML = '<span class="cv-agent-dot" style="background:#6366f1"></span>Hardware';
      hwHdr.appendChild(stepBadge('wa'));
      right.appendChild(hwHdr);
      var hwRow = el('div', 'cv-hw');
      hwRow.setAttribute('data-layer', 'wa');
      c.hardware.forEach(function (h) {
        var chip = el('span', 'cv-hw-chip');
        chip.innerHTML = '<strong>' + esc(h.fact) + '</strong> \u2192 ' + esc(h.strategy);
        hwRow.appendChild(chip);
      });
      right.appendChild(hwRow);
    }

    container.appendChild(left);
    container.appendChild(right);
  }

  /* ═══ Build operator node (recursive) ═══ */
  function buildOpNode(container, op, opMap, cd) {
    var lvl = el('div', 'cv-level');
    var node = createOpEl(op, cd);
    lvl.appendChild(node);

    // Children — operators that are children in the tree
    var childOps = (op.children || []).filter(function (cid) { return opMap[cid]; });
    // Table sources — dashed edges drawn by SVG, but we don't render table scan nodes

    if (childOps.length) {
      var ch = el('div', 'cv-children');
      childOps.forEach(function (cid) { buildOpNode(ch, opMap[cid], opMap, cd); });
      lvl.appendChild(ch);
    }
    container.appendChild(lvl);
  }

  function createOpEl(op, cd) {
    var node = el('div', 'cv-op cv-op-' + op.type);
    node.setAttribute('data-op', op.id);
    var icons = { scan: '\u25A4', join: '\u25C8', aggregate: '\u03A3', sort: '\u25B2' };

    var hdr = el('div', 'cv-op-hdr');
    hdr.appendChild(el('span', 'cv-op-ico cv-ico-' + op.type, icons[op.type] || '\u25CF'));
    hdr.appendChild(el('span', 'cv-op-lbl', op.label));
    if (op.rows != null) hdr.appendChild(el('span', 'cv-op-rows', Charts.formatNumber(op.rows)));
    if (op.joinType) hdr.appendChild(el('span', 'cv-op-jt', op.joinType));
    node.appendChild(hdr);

    // CG: code ref badge with step indicator
    (cd.codeRefs || []).forEach(function (cr) {
      if (cr.op === op.id) {
        var ref = el('span', 'cv-cref');
        ref.setAttribute('data-layer', 'cg');
        ref.innerHTML = '<span class="cv-step-micro">\u2464</span>L' + cr.lines;
        node.appendChild(ref);
      }
    });

    // OPT: timing annotation with reason
    var t = cd.timings ? cd.timings[op.id] : null;
    if (t) {
      var tb = el('div', 'cv-timing');
      tb.setAttribute('data-layer', 'opt');
      var trow = '<div class="cv-t-row"><span class="cv-step-micro cv-step-opt">\u2465</span>';
      if (t[1] > 0) {
        trow += '<span class="cv-t0">' + t[0].toFixed(1) + '</span>\u2192<span class="cv-t1">' + t[1].toFixed(1) + '</span><span class="cv-tu">ms</span>';
      } else {
        trow += '<span class="cv-t0">' + t[0].toFixed(1) + '</span>\u2192<span class="cv-t1">fused</span>';
      }
      trow += '</div>';
      tb.innerHTML = trow;
      if (t[2]) tb.innerHTML += '<span class="cv-t-reason">' + esc(t[2]) + '</span>';
      node.appendChild(tb);
    }

    return node;
  }

  /* ═══ SVG edges (redrawn on every agent switch when tree visible) ═══ */
  function redrawEdges() {
    if (!svgEl || !canvasData) return;
    svgEl.innerHTML = '';
    // SVG is a child of .cv-right — use it as the coordinate reference
    var rightCol = svgEl.parentElement;
    if (!rightCol) return;
    var rect = rightCol.getBoundingClientRect();
    svgEl.setAttribute('width', rightCol.offsetWidth);
    svgEl.setAttribute('height', rightCol.offsetHeight);
    svgEl.style.cssText = 'position:absolute;top:0;left:0;width:' + rightCol.offsetWidth + 'px;height:' + rightCol.offsetHeight + 'px;pointer-events:none;overflow:visible;z-index:0;';
    var cvEl = rightCol;

    var opMap = {};
    (canvasData.operators||[]).forEach(function (o) { opMap[o.id] = o; });

    function drawPath(fromEl, toEl, color, dashed) {
      if (!fromEl || !toEl) return;
      var fR = fromEl.getBoundingClientRect();
      var tR = toEl.getBoundingClientRect();
      var fx = fR.left + fR.width / 2 - rect.left, fy = fR.bottom - rect.top;
      var tx = tR.left + tR.width / 2 - rect.left, ty = tR.top - rect.top;
      var my = fy + (ty - fy) * 0.5;
      var p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      p.setAttribute('d', 'M' + fx + ' ' + fy + 'C' + fx + ' ' + my + ',' + tx + ' ' + my + ',' + tx + ' ' + ty);
      p.setAttribute('stroke', color || '#d4a574');
      p.setAttribute('stroke-width', '1.5');
      p.setAttribute('fill', 'none');
      if (dashed) p.setAttribute('stroke-dasharray', '4 3');
      svgEl.appendChild(p);

      // Edge label (for table sources)
      return { mx: (fx + tx) / 2, my: my };
    }

    function draw(op) {
      var pEl = cvEl.querySelector('[data-op="' + op.id + '"]');
      // Edges to child operators
      (op.children || []).forEach(function (cid) {
        var cEl = cvEl.querySelector('[data-op="' + cid + '"]');
        drawPath(pEl, cEl, '#d4a574', false);
        if (opMap[cid]) draw(opMap[cid]);
      });
      // Edges to table boxes (tableSources)
      (op.tableSources || []).forEach(function (ts) {
        var tblEl = cvEl.querySelector('[data-table="' + ts.table + '"]');
        var pos = drawPath(pEl, tblEl, '#94a3b8', true);
        // Add label on the edge with white background
        if (pos) {
          var labelText = ts.label;
          var textW = labelText.length * 5.5 + 6;
          var textH = 12;
          // White background rect
          var bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
          bg.setAttribute('x', pos.mx - textW / 2);
          bg.setAttribute('y', pos.my - textH + 1);
          bg.setAttribute('width', textW);
          bg.setAttribute('height', textH);
          bg.setAttribute('rx', '2');
          bg.setAttribute('fill', '#fafafa');
          bg.setAttribute('stroke', '#e5e7eb');
          bg.setAttribute('stroke-width', '0.5');
          svgEl.appendChild(bg);
          // Text
          var txt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          txt.setAttribute('x', pos.mx);
          txt.setAttribute('y', pos.my);
          txt.setAttribute('text-anchor', 'middle');
          txt.setAttribute('font-size', '8');
          txt.setAttribute('font-family', 'Inter, sans-serif');
          txt.setAttribute('fill', '#6b7280');
          txt.textContent = labelText;
          svgEl.appendChild(txt);
        }
      });
    }

    var childSet = {};
    (canvasData.operators||[]).forEach(function (o) { (o.children||[]).forEach(function (c2) { childSet[c2] = true; }); });
    (canvasData.operators||[]).forEach(function (o) { if (!childSet[o.id]) draw(o); });
  }

  /* ═══ SQL helpers ═══ */
  function syntaxHL(sql) {
    if (typeof hljs !== 'undefined') { try { return hljs.highlight(sql, { language: 'sql' }).value; } catch (e) {} }
    return esc(sql);
  }
  function overlayHL(html, raw, params) {
    var ranges = [];
    params.forEach(function (p) {
      var needle = p.context ? p.context + p.value : p.value;
      var idx = raw.indexOf(needle);
      if (idx >= 0) { var s = idx + (p.context ? p.context.length : 0); ranges.push({ start: s, end: s + p.value.length, color: p.color }); }
    });
    if (!ranges.length) return html;
    ranges.sort(function (a, b) { return a.start - b.start; });
    var res = '', ri = 0, rawI = 0, i = 0, act = null;
    while (i < html.length) {
      if (html[i] === '<') { var te = html.indexOf('>', i); if (te >= 0) { res += html.substring(i, te + 1); i = te + 1; continue; } }
      var adv = 1;
      if (html[i] === '&') { var si = html.indexOf(';', i); if (si >= 0 && si - i < 10) adv = si + 1 - i; }
      if (!act && ri < ranges.length && rawI === ranges[ri].start) {
        act = ranges[ri];
        res += '<span class="sql-hl" style="background:' + act.color + '18;border-bottom:2px solid ' + act.color + '">';
      }
      res += html.substring(i, i + adv); rawI++; i += adv;
      if (act && rawI === act.end) { res += '</span>'; ri++; act = null; }
    }
    if (act) res += '</span>';
    return res;
  }
  function getCode(bench, qid, iter) {
    if (bench === 'tpch' && typeof TPCH_DATA !== 'undefined') {
      var qd = TPCH_DATA.queries[qid];
      if (qd && qd.iterations && qd.iterations[iter]) return qd.iterations[iter].code || '';
    }
    return '';
  }

  /* ═══ Detail Panel ═══ */
  function buildDetail(panel, agent) {
    panel.innerHTML = '';
    var hdr = el('div', 'agent-header');
    var hc = el('span', 'agent-header-circle'); hc.textContent = agent.shortName; hc.style.backgroundColor = agent.color;
    hdr.appendChild(hc);
    var ht = el('div', 'agent-header-text');
    ht.appendChild(el('div', 'agent-header-name', agent.name));
    ht.appendChild(el('div', 'agent-header-role', agent.role));
    hdr.appendChild(ht);
    panel.appendChild(hdr);
    var d = agent.detail;
    if (!d) return;
    switch (d.type) {
      case 'sqlParams': renderSqlD(panel, d); break;
      case 'waDetail': renderWaD(panel, d); break;
      case 'sdDetail': renderSdD(panel, d); break;
      case 'qpDetail': renderQpD(panel, d); break;
      case 'cgDetail': renderCgD(panel, d); break;
      case 'optDetail': VizJourney.render(panel, d.journey); break;
    }
  }

  function renderSqlD(p, d) {
    var t = el('table', 'sql-param-table');
    var th = el('tr'); ['Placeholder','Value','Type'].forEach(function (h) { th.appendChild(el('th','',h)); }); t.appendChild(th);
    d.params.forEach(function (pr) {
      var r = el('tr');
      var nc = el('td','spt-name'); nc.innerHTML = '<span class="spt-dot" style="background:'+pr.color+'"></span><code>'+esc(pr.name)+'</code>'; r.appendChild(nc);
      var vc = el('td'); vc.innerHTML = '<span class="spt-val" style="background:'+pr.color+'15;color:'+pr.color+'">'+esc(pr.value)+'</span>'; r.appendChild(vc);
      r.appendChild(el('td','spt-type',pr.type)); t.appendChild(r);
    });
    p.appendChild(t);
    if (d.tags) { var tg = el('div','agent-badges'); d.tags.forEach(function (t2) { tg.appendChild(el('span','agent-badge',t2)); }); p.appendChild(tg); }
  }
  function renderWaD(p, d) {
    var g = el('div','wa-disc-grid');
    d.discoveries.forEach(function (di) {
      var c = el('div','wa-disc-card wa-disc-'+di.type);
      c.innerHTML = '<div class="wa-disc-header"><span class="wa-disc-icon">'+di.icon+'</span><span class="wa-disc-label">'+esc(di.label)+'</span></div><div class="wa-disc-text">'+esc(di.text)+'</div>';
      g.appendChild(c);
    });
    p.appendChild(g);
  }
  function renderSdD(p, d) {
    var r = el('div','sd-shift-vis');
    r.innerHTML = '<div class="sd-shift-box sd-shift-from"><div class="sd-shift-label">Before</div><div class="sd-shift-text">'+esc(d.shift.from)+'</div></div>' +
      '<div class="sd-shift-arrow"><svg width="32" height="16" viewBox="0 0 32 16"><path d="M0 8L24 8M19 3L24 8L19 13" stroke="var(--emerald)" stroke-width="2" fill="none" stroke-linecap="round"/></svg></div>' +
      '<div class="sd-shift-box sd-shift-to"><div class="sd-shift-label">After</div><div class="sd-shift-text">'+esc(d.shift.to)+'</div></div>';
    p.appendChild(r);
    var b = el('div','sd-benefits');
    d.benefits.forEach(function (x) { b.innerHTML += '<span class="sd-benefit-chip">\u2713 '+esc(x)+'</span>'; });
    p.appendChild(b);
  }
  function renderQpD(p, d) {
    if (d.reduction) {
      var f = el('div','qp-funnel'); var mx = d.reduction[0].rows;
      d.reduction.forEach(function (s, i) {
        var st = el('div','qp-funnel-stage');
        var bw = Math.max(3, Math.round(s.rows / mx * 100));
        var br = el('div','qp-funnel-bar'+(i===d.reduction.length-1?' qp-funnel-final':'')); br.style.width = bw+'%'; st.appendChild(br);
        var info = el('div','qp-funnel-info'); info.appendChild(el('span','qp-funnel-label',s.label)); info.appendChild(el('span','qp-funnel-rows',Charts.formatNumber(s.rows))); st.appendChild(info);
        f.appendChild(st);
        if (i < d.reduction.length-1) f.appendChild(el('div','qp-funnel-arrow','\u2193'));
      });
      p.appendChild(f);
    }
    if (d.decisions) {
      var g = el('div','wa-disc-grid');
      d.decisions.forEach(function (t) {
        var c = el('div','wa-disc-card wa-disc-strategy');
        c.innerHTML = '<div class="wa-disc-header"><span class="wa-disc-icon">\u2699</span><span class="wa-disc-label">Strategy</span></div><div class="wa-disc-text">'+esc(t)+'</div>';
        g.appendChild(c);
      });
      p.appendChild(g);
    }
  }
  function renderCgD(p, d) {
    // Full code viewer for exploration
    var codeStr = '';
    if (d.codeSource) {
      var parts = d.codeSource.split(':');
      if (parts[0] === 'tpch' && typeof TPCH_DATA !== 'undefined') {
        var qd = TPCH_DATA.queries[parts[1]];
        if (qd && qd.iterations) { var it = qd.iterations[parseInt(parts[2].replace('iter',''),10)]; if (it) codeStr = it.code || ''; }
      }
    }
    if (codeStr) {
      CodeViewer.createCodeViewer(p, {
        code: codeStr, language: 'cpp', lineNumbers: true,
        maxHeight: 500, collapsible: true, collapsedLines: 30,
        title: 'Generated C++ \u2014 ' + d.result.lines + ' lines'
      });
    }
  }

  /* ═══ Benchmark ═══ */
  function buildBenchmark(container, bm, sp) {
    container.appendChild(el('div','viz-bench-title','Performance & Cost'));

    var cols = el('div','bench-cols');

    /* ── Left: Query Execution ── */
    var left = el('div','bench-col');
    left.appendChild(el('div','bench-col-title','Query Execution'));
    // Bar chart
    var cd = bm.queryMs.map(function (s) { return {label:s.name,value:s.ms,highlight:s.highlight,color:s.highlight?'#10b981':'#94a3b8'}; });
    var cw = el('div','bench-chart-wrap');
    Charts.createBarChart(cw, cd, {horizontal:true,animated:true,unit:'ms',formatValue:Charts.formatMs,relativeToMin:true});
    left.appendChild(cw);
    // Speedup badges
    if (bm.gendbSpeedups) {
      var bg = el('div','benchmark-badges');
      bm.gendbSpeedups.forEach(function (s) { bg.appendChild(el('span','benchmark-badge',s.factor.toFixed(1)+'x vs '+s.vs)); });
      left.appendChild(bg);
    }
    cols.appendChild(left);

    /* ── Right: Code Generation ── */
    var right = el('div','bench-col');
    right.appendChild(el('div','bench-col-title','Code Generation'));
    // Self-optimization hero
    if (sp) {
      var h = el('div','speedup-hero');
      h.appendChild(el('div','speedup-hero-value',sp.factor.toFixed(1)+'x'));
      h.appendChild(el('div','speedup-hero-label','self-optimization'));
      h.appendChild(el('div','speedup-hero-detail',Charts.formatMs(sp.from)+' \u2192 '+Charts.formatMs(sp.to)));
      right.appendChild(h);
    }
    // Generation stats grid
    if (bm.generation) {
      var g = bm.generation;
      var gw = el('div','gen-stats');
      var items = [
        { label: 'Generation Time', value: Math.floor(g.timeSec/60)+'m '+g.timeSec%60+'s', icon: '\u23f1' },
        { label: 'LLM Cost', value: '$'+g.costUsd.toFixed(2), icon: '\ud83d\udcb0' },
        { label: 'Iterations', value: g.iterations, icon: '\ud83d\udd04' },
        { label: 'Lines of C++', value: g.linesOfCode, icon: '\ud83d\udcdd' }
      ];
      items.forEach(function (it) {
        var s = el('div','gen-stat');
        s.appendChild(el('div','gen-stat-icon',it.icon));
        var txt = el('div','gen-stat-text');
        txt.appendChild(el('div','gen-stat-val',String(it.value)));
        txt.appendChild(el('div','gen-stat-lbl',it.label));
        s.appendChild(txt);
        gw.appendChild(s);
      });
      right.appendChild(gw);
    }
    cols.appendChild(right);

    container.appendChild(cols);
  }

  return { render: render };
})();
