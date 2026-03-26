/* ============================================================
   GenDB Demo — viz-journey.js
   Optimization journey: headline + timeline + detail panel.
   Clean, paper-screenshot-ready layout.
   ============================================================ */

var VizJourney = (function () {
  'use strict';

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function render(container, journey) {
    // ── Headline: speedup summary ──
    var headline = el('div', 'opt-headline');
    var hlLeft = el('div', 'opt-hl-left');
    hlLeft.appendChild(el('div', 'opt-hl-value', journey.totalSpeedup.toFixed(1) + 'x'));
    hlLeft.appendChild(el('div', 'opt-hl-label', 'self-optimization'));
    headline.appendChild(hlLeft);
    var hlRight = el('div', 'opt-hl-right');
    hlRight.innerHTML =
      '<span class="opt-hl-from">' + Charts.formatMs(journey.startMs) + '</span>' +
      '<span class="opt-hl-arrow">\u2192</span>' +
      '<span class="opt-hl-to">' + Charts.formatMs(journey.endMs) + '</span>' +
      '<span class="opt-hl-iters">' + journey.iterations.length + ' iterations</span>';
    headline.appendChild(hlRight);
    container.appendChild(headline);

    // ── Compact timeline bar ──
    var timeline = el('div', 'opt-timeline');
    var maxMs = journey.startMs;

    journey.iterations.forEach(function (iter, idx) {
      if (idx > 0 && iter.speedupVsPrev) {
        var arrow = el('div', 'opt-tl-arrow');
        arrow.textContent = iter.speedupVsPrev.toFixed(1) + 'x';
        timeline.appendChild(arrow);
      }

      var item = el('div', 'opt-tl-item' + (idx === 0 ? ' selected' : ''));
      item.dataset.iter = idx;

      var barH = Math.max(6, Math.round((iter.timingMs / maxMs) * 56));
      var barCls = idx === 0 ? 'opt-tl-bar baseline' :
        (idx === journey.iterations.length - 1 ? 'opt-tl-bar final' : 'opt-tl-bar improved');
      var bar = el('div', barCls);
      bar.style.height = barH + 'px';

      item.appendChild(el('div', 'opt-tl-ms', Math.round(iter.timingMs) + 'ms'));
      var barWrap = el('div', 'opt-tl-bar-wrap');
      barWrap.appendChild(bar);
      item.appendChild(barWrap);
      item.appendChild(el('div', 'opt-tl-iter', 'Iter ' + iter.iteration));
      item.appendChild(el('div', 'opt-tl-label', iter.title));

      item.addEventListener('click', (function (i) {
        return function () {
          container.querySelectorAll('.opt-tl-item').forEach(function (x) { x.classList.remove('selected'); });
          item.classList.add('selected');
          renderDetail(detailPanel, journey.iterations[i]);
        };
      })(idx));

      timeline.appendChild(item);
    });
    container.appendChild(timeline);

    // ── Detail panel ──
    var detailPanel = el('div', 'opt-detail');
    renderDetail(detailPanel, journey.iterations[0]);
    container.appendChild(detailPanel);
  }

  function renderDetail(panel, iter) {
    panel.innerHTML = '';

    // Header row
    var header = el('div', 'opt-d-header');
    header.appendChild(el('span', 'opt-d-title', iter.title));
    header.appendChild(el('span', 'opt-d-timing', Math.round(iter.timingMs) + ' ms'));
    var catBadge = el('span', 'opt-d-cat', iter.categoryLabel);
    header.appendChild(catBadge);
    if (iter.speedupVsPrev) {
      header.appendChild(el('span', 'opt-d-speedup', iter.speedupVsPrev.toFixed(1) + 'x faster'));
    }
    panel.appendChild(header);

    // Two-column: bottleneck (left) + fix (right) — or full width if only one
    if (iter.bottleneck || iter.fix) {
      var bfRow = el('div', 'opt-bf-row');

      if (iter.bottleneck) {
        var bn = el('div', 'opt-bottleneck');
        bn.appendChild(el('div', 'opt-bf-label opt-bf-problem', 'Problem'));
        bn.appendChild(el('div', 'opt-bf-name', iter.bottleneck.name));
        bn.appendChild(el('div', 'opt-bf-desc', iter.bottleneck.description));
        // Percentage bar
        var pct = el('div', 'opt-bf-pct');
        var pctBar = el('div', 'opt-pct-bar');
        var pctFill = el('div', 'opt-pct-fill');
        pctFill.style.width = iter.bottleneck.pct + '%';
        pctBar.appendChild(pctFill);
        pct.appendChild(pctBar);
        pct.appendChild(el('span', 'opt-pct-text', iter.bottleneck.pct + '% of runtime'));
        bn.appendChild(pct);
        bfRow.appendChild(bn);
      }

      if (iter.fix) {
        var fix = el('div', 'opt-fix');
        fix.appendChild(el('div', 'opt-bf-label opt-bf-solution', 'Solution'));
        fix.appendChild(el('div', 'opt-bf-desc', iter.fix));
        // Changes diff
        if (iter.changes && iter.changes.length) {
          var diff = el('div', 'opt-changes');
          iter.changes.forEach(function (ch) {
            var item = el('div', 'opt-change opt-change-' + ch.type);
            var icon = ch.type === 'add' ? '+' : (ch.type === 'remove' ? '\u2212' : '~');
            item.innerHTML = '<span class="opt-ch-icon">' + icon + '</span>' +
              '<span class="opt-ch-text">' + escapeHtml(ch.what) + '</span>';
            diff.appendChild(item);
          });
          fix.appendChild(diff);
        }
        bfRow.appendChild(fix);
      }

      panel.appendChild(bfRow);
    }

    // Operation timing waterfall
    if (iter.operationTimings && iter.operationTimings.length) {
      var wf = el('div', 'opt-waterfall');
      wf.appendChild(el('div', 'wa-section-title', 'Operation Breakdown'));
      var totalMs = 0;
      iter.operationTimings.forEach(function (op) { totalMs += op.ms; });

      var barRow = el('div', 'opt-wf-bar');
      var legendRow = el('div', 'opt-wf-legend');
      iter.operationTimings.forEach(function (op) {
        var pct = (op.ms / totalMs * 100);
        var seg = el('div', 'opt-wf-seg');
        seg.style.width = pct + '%';
        seg.style.backgroundColor = op.color;
        seg.title = op.name + ': ' + op.ms.toFixed(1) + 'ms';
        barRow.appendChild(seg);

        var legItem = el('div', 'opt-wf-leg-item');
        legItem.innerHTML = '<span class="opt-wf-dot" style="background:' + op.color + '"></span>' +
          '<span class="opt-wf-leg-name">' + escapeHtml(op.name) + '</span>' +
          '<span class="opt-wf-leg-ms">' + op.ms.toFixed(1) + 'ms</span>';
        legendRow.appendChild(legItem);
      });
      wf.appendChild(barRow);
      wf.appendChild(legendRow);
      panel.appendChild(wf);
    }

    // Insight
    if (iter.insight) {
      panel.appendChild(el('div', 'opt-insight', iter.insight));
    }
  }

  function escapeHtml(s) {
    return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  return { render: render };
})();
