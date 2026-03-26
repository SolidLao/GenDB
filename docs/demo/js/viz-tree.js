/* ============================================================
   GenDB Demo — viz-tree.js
   Execution tree renderer with animated edges and expandable nodes.
   ============================================================ */

var VizTree = (function () {
  'use strict';

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  var NODE_ICONS = {
    filter: '\u25A7',  // dotted square
    scan:   '\u25A4',  // square with horizontal fill
    join:   '\u25C8',  // diamond in square
    sort:   '\u25B2'   // triangle
  };

  /* ─── Render ─── */

  function render(container, tree, vizData) {
    var wrap = el('div', 'exec-tree');

    tree.nodes.forEach(function (node, idx) {
      // Render node
      var nodeEl = renderNode(node, vizData);
      wrap.appendChild(nodeEl);

      // Render edge (if not last)
      if (idx < tree.edges.length) {
        var edge = tree.edges[idx];
        var edgeWrap = el('div', 'tree-edge-wrapper');
        var edgeLine = el('div', 'tree-edge');
        edgeWrap.appendChild(edgeLine);
        var label = el('span', 'tree-edge-label', edge.label + ' (' + edge.dataflow + ')');
        edgeWrap.appendChild(label);
        wrap.appendChild(edgeWrap);
      }
    });

    container.appendChild(wrap);
  }

  /* ─── Render Node ─── */

  function renderNode(node, vizData) {
    var nodeEl = el('div', 'tree-node');

    // Header row
    var header = el('div', 'tree-node-header');

    var icon = el('div', 'tree-node-icon type-' + node.type);
    icon.textContent = NODE_ICONS[node.type] || '\u25CF';
    header.appendChild(icon);

    header.appendChild(el('span', 'tree-node-label', node.label));

    if (node.parallel) {
      header.appendChild(el('span', 'tree-node-parallel', 'parallel'));
    }

    var outText = node.output.rows != null ? Charts.formatNumber(node.output.rows) + ' rows' : node.output.description;
    header.appendChild(el('span', 'tree-node-output', outText));

    nodeEl.appendChild(header);

    // Expandable detail
    var detail = el('div', 'tree-node-detail');

    // Description
    if (node.detail) {
      detail.appendChild(el('div', 'tree-node-desc', node.detail));
    }

    // Per-iteration timing bars
    if (node.timingMs) {
      var timingTitle = el('div', 'tree-node-desc');
      timingTitle.style.borderTop = 'none';
      timingTitle.style.paddingTop = '0';
      timingTitle.innerHTML = '<strong style="font-size:0.75rem;color:var(--gray-400);text-transform:uppercase;letter-spacing:0.06em">Timing per Iteration</strong>';
      detail.appendChild(timingTitle);

      var bars = el('div', 'tree-timing-bars');
      var iterations = Object.keys(node.timingMs).sort();
      var maxMs = 0;
      iterations.forEach(function (k) { if (node.timingMs[k] > maxMs) maxMs = node.timingMs[k]; });

      // Find best iteration
      var bestIter = iterations[0];
      iterations.forEach(function (k) {
        if (node.timingMs[k] < node.timingMs[bestIter]) bestIter = k;
      });

      iterations.forEach(function (k) {
        var col = el('div', 'tree-timing-col');
        var barH = Math.max(4, Math.round((node.timingMs[k] / maxMs) * 40));
        var bar = el('div', 'tree-timing-bar' + (k === bestIter ? ' best' : ''));
        bar.style.height = barH + 'px';
        col.appendChild(el('div', 'tree-timing-val', node.timingMs[k].toFixed(1) + 'ms'));
        col.appendChild(bar);
        col.appendChild(el('div', 'tree-timing-label', 'Iter ' + k));
        bars.appendChild(col);
      });
      detail.appendChild(bars);
    }

    // Input/output badges
    var ioBadges = el('div', 'beat-annotations');
    if (node.input.table) {
      var inBadge = el('span', 'beat-annotation');
      inBadge.innerHTML = '<span class="ann-key">Input</span> <span class="ann-value">' +
        node.input.table + ' (' + Charts.formatNumber(node.input.rows) + ')</span>';
      ioBadges.appendChild(inBadge);
    } else if (node.input.description) {
      var inBadge2 = el('span', 'beat-annotation');
      inBadge2.innerHTML = '<span class="ann-key">Input</span> <span class="ann-value">' +
        escapeHtml(node.input.description) + '</span>';
      ioBadges.appendChild(inBadge2);
    }
    var outBadge = el('span', 'beat-annotation');
    outBadge.innerHTML = '<span class="ann-key">Output</span> <span class="ann-value">' +
      escapeHtml(node.output.description) + '</span>';
    ioBadges.appendChild(outBadge);
    detail.appendChild(ioBadges);

    nodeEl.appendChild(detail);

    // Click to expand
    nodeEl.addEventListener('click', function (e) {
      e.stopPropagation();
      // Close other nodes
      document.querySelectorAll('.tree-node.expanded').forEach(function (n) {
        if (n !== nodeEl) n.classList.remove('expanded');
      });
      nodeEl.classList.toggle('expanded');
    });

    return nodeEl;
  }

  function escapeHtml(str) {
    return (str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  return { render: render };
})();
