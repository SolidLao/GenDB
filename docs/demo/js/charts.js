/* ============================================================
   GenDB Demo — charts.js
   Pure CSS/JS chart utilities (no external charting library)
   ============================================================ */

var Charts = (function () {
  'use strict';

  /* ---------- Formatters ---------- */

  function formatMs(ms) {
    if (ms == null || isNaN(ms)) return '—';
    if (ms >= 1000) return (ms / 1000).toFixed(2) + ' s';
    if (ms >= 100) return Math.round(ms) + ' ms';
    return ms.toFixed(1) + ' ms';
  }

  function formatNumber(n) {
    if (n == null || isNaN(n)) return '—';
    return Number(n).toLocaleString('en-US');
  }

  function formatSpeedup(baseline, gendb) {
    if (!baseline || !gendb || gendb <= 0) return '';
    var ratio = baseline / gendb;
    if (ratio >= 1) return ratio.toFixed(1) + 'x faster';
    return (1 / ratio).toFixed(1) + 'x slower';
  }

  /* ---------- Utility ---------- */

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  /* ---------- createBarChart ---------- */

  function createBarChart(container, data, options) {
    options = options || {};
    var horizontal = options.horizontal !== false; // default horizontal
    var animated = options.animated !== false;
    var unit = options.unit || 'ms';
    var fmtValue = options.formatValue || (unit === 'ms' ? formatMs : formatNumber);
    // Relative mode: proportional bars with automatic outlier detection.
    // Normal systems: bars proportional to actual values, scaled so the slowest
    // non-outlier reaches 100%. Outlier systems (> 4x the median): full-width
    // striped bar with an "Nx" badge showing how much slower.
    var relativeToMin = options.relativeToMin || false;

    var maxValue = options.maxValue || 0;
    var minValue = Infinity;
    if (!maxValue) {
      data.forEach(function (d) { if (d.value > maxValue) maxValue = d.value; });
    }
    data.forEach(function (d) { if (d.value > 0 && d.value < minValue) minValue = d.value; });
    if (maxValue <= 0) maxValue = 1;
    if (minValue === Infinity) minValue = 1;

    // Outlier detection via median
    var outlierThreshold = Infinity;
    var scaleMax = maxValue;
    if (relativeToMin && data.length >= 3) {
      var sorted = data.map(function (d) { return d.value; }).sort(function (a, b) { return a - b; });
      var mid = Math.floor(sorted.length / 2);
      var median = sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
      outlierThreshold = median * 4;
      // scaleMax = largest non-outlier value
      scaleMax = 0;
      data.forEach(function (d) {
        if (d.value <= outlierThreshold && d.value > scaleMax) scaleMax = d.value;
      });
      if (scaleMax <= 0) scaleMax = maxValue;
    }

    var chart = el('div', 'chart bar-chart' + (horizontal ? ' bar-chart--horizontal' : ' bar-chart--vertical'));

    data.forEach(function (d) {
      var row = el('div', 'bar-row');

      var label = el('span', 'bar-label', d.label);
      row.appendChild(label);

      var track = el('div', 'bar-track');
      var isOutlier = relativeToMin && d.value > outlierThreshold;
      var bar = el('div', 'bar-fill'
        + (d.highlight ? ' bar-fill--highlight' : '')
        + (isOutlier ? ' bar-fill--outlier' : ''));
      var pct;
      if (relativeToMin) {
        if (isOutlier) {
          pct = 100;
        } else {
          // Linear: fastest → minBarPct, scaleMax → 100%
          pct = (d.value / scaleMax) * 100;
        }
      } else {
        pct = Math.min((d.value / maxValue) * 100, 100);
      }
      if (animated) {
        bar.style.width = '0%';
        bar.dataset.targetWidth = pct + '%';
      } else {
        bar.style.width = pct + '%';
      }
      if (d.color && !isOutlier) bar.style.backgroundColor = d.color;
      track.appendChild(bar);
      row.appendChild(track);

      var valSpan = el('span', 'bar-value', fmtValue(d.value));
      row.appendChild(valSpan);

      // Outlier "Nx" badge
      if (isOutlier && minValue > 0) {
        var factor = (d.value / minValue);
        var badge = el('span', 'bar-badge bar-badge--outlier',
          (factor >= 10 ? Math.round(factor) : factor.toFixed(1)) + 'x');
        row.appendChild(badge);
      }

      if (d.badge) {
        var badge = el('span', 'bar-badge', d.badge);
        row.appendChild(badge);
      }

      chart.appendChild(row);
    });

    container.appendChild(chart);

    if (animated) {
      animateOnScroll([chart]);
    }

    return chart;
  }

  /* ---------- createStackedBar ---------- */

  function createStackedBar(container, segments, options) {
    options = options || {};
    var total = 0;
    segments.forEach(function (s) { total += s.value; });
    if (total <= 0) total = 1;

    var wrapper = el('div', 'stacked-bar-wrapper');

    var bar = el('div', 'stacked-bar');
    segments.forEach(function (s) {
      var seg = el('div', 'stacked-segment');
      seg.style.width = ((s.value / total) * 100).toFixed(2) + '%';
      if (s.color) seg.style.backgroundColor = s.color;
      seg.title = s.label + ': ' + formatMs(s.value);
      seg.dataset.label = s.label;
      seg.dataset.value = formatMs(s.value);
      bar.appendChild(seg);
    });
    wrapper.appendChild(bar);

    // Legend
    var legend = el('div', 'stacked-legend');
    segments.forEach(function (s) {
      var item = el('span', 'stacked-legend-item');
      var swatch = el('span', 'swatch');
      if (s.color) swatch.style.backgroundColor = s.color;
      item.appendChild(swatch);
      item.appendChild(document.createTextNode(s.label + ' ' + formatMs(s.value)));
      legend.appendChild(item);
    });
    wrapper.appendChild(legend);

    container.appendChild(wrapper);
    return wrapper;
  }

  /* ---------- createIterationTimeline ---------- */

  var ITER_COLORS = ['#6366f1', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444', '#3b82f6', '#ec4899'];

  function createIterationTimeline(container, iterations, options) {
    options = options || {};
    if (!iterations || !iterations.length) return null;

    var maxTiming = 0;
    iterations.forEach(function (it) {
      if (it.timing_ms > maxTiming) maxTiming = it.timing_ms;
    });
    if (maxTiming <= 0) maxTiming = 1;

    var wrapper = el('div', 'iteration-timeline');

    var barsRow = el('div', 'iter-bars');
    iterations.forEach(function (it, idx) {
      var col = el('div', 'iter-col');

      var valLabel = el('div', 'iter-val', formatMs(it.timing_ms));
      col.appendChild(valLabel);

      var barOuter = el('div', 'iter-bar-outer');
      var barInner = el('div', 'iter-bar-inner');
      var h = Math.max(((it.timing_ms / maxTiming) * 100), 4);
      barInner.style.height = h + '%';
      barInner.style.backgroundColor = it.validation === 'pass'
        ? ITER_COLORS[idx % ITER_COLORS.length]
        : '#ef4444';
      if (it.validation !== 'pass') {
        barInner.style.backgroundImage = 'repeating-linear-gradient(45deg, transparent, transparent 4px, rgba(0,0,0,0.15) 4px, rgba(0,0,0,0.15) 8px)';
      }
      barOuter.appendChild(barInner);
      col.appendChild(barOuter);

      var iterLabel = el('div', 'iter-label', 'Iter ' + it.iteration);
      col.appendChild(iterLabel);

      col.title = 'Iteration ' + it.iteration + ': ' + formatMs(it.timing_ms) +
        (it.validation !== 'pass' ? ' (FAILED)' : '') +
        (it.strategy ? '\n' + it.strategy.slice(0, 120) : '');

      barsRow.appendChild(col);
    });
    wrapper.appendChild(barsRow);

    // Trend line (improvement summary)
    if (iterations.length > 1) {
      var first = iterations[0].timing_ms;
      var last = iterations[iterations.length - 1].timing_ms;
      var speedup = first / last;
      var trend = el('div', 'iter-trend');
      trend.innerHTML = '<span class="iter-trend-arrow">&#8600;</span> ' +
        formatMs(first) + ' &rarr; ' + formatMs(last) +
        ' <span class="iter-speedup-badge">' + speedup.toFixed(1) + 'x faster</span>';
      wrapper.appendChild(trend);
    }

    container.appendChild(wrapper);
    animateOnScroll([wrapper]);
    return wrapper;
  }

  /* ---------- animateOnScroll ---------- */

  function animateOnScroll(elements, threshold) {
    threshold = threshold || 0.2;
    if (!('IntersectionObserver' in window)) {
      elements.forEach(function (el) { el.classList.add('visible'); });
      return;
    }
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          // Animate bars inside
          var bars = entry.target.querySelectorAll('.bar-fill[data-target-width]');
          bars.forEach(function (bar) {
            bar.style.width = bar.dataset.targetWidth;
          });
          observer.unobserve(entry.target);
        }
      });
    }, { threshold: threshold });

    elements.forEach(function (el) {
      observer.observe(el);
    });
  }

  /* ---------- Public API ---------- */

  return {
    createBarChart: createBarChart,
    createStackedBar: createStackedBar,
    createIterationTimeline: createIterationTimeline,
    animateOnScroll: animateOnScroll,
    formatMs: formatMs,
    formatNumber: formatNumber,
    formatSpeedup: formatSpeedup
  };
})();
