/* ============================================================
   GenDB Demo — code-viewer.js
   C++ code viewer with syntax highlighting, line numbers,
   copy button, collapsible sections, and tabbed view.
   Light theme matching the website style.
   ============================================================ */

var CodeViewer = (function () {
  'use strict';

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  }

  /* ---------- createCodeViewer ---------- */

  function createCodeViewer(container, options) {
    options = options || {};
    var code = options.code || '';
    var language = options.language || 'cpp';
    var showLineNumbers = options.lineNumbers !== false;
    var maxHeight = options.maxHeight || 500;
    var collapsible = options.collapsible || false;
    var collapsedLines = options.collapsedLines || 30;
    var title = options.title || '';

    var lines = code.split('\n');
    var lineCount = lines.length;

    var wrapper = el('div', 'code-viewer');

    // Header bar
    var header = el('div', 'code-viewer-header');
    if (title) {
      header.appendChild(el('span', 'code-viewer-title', title));
    }
    var langBadge = el('span', 'code-viewer-lang', language.toUpperCase());
    header.appendChild(langBadge);
    var linesBadge = el('span', 'code-viewer-lines', lineCount + ' lines');
    header.appendChild(linesBadge);

    // Copy button
    var copyBtn = el('button', 'code-viewer-copy', 'Copy');
    copyBtn.addEventListener('click', function () {
      if (navigator.clipboard) {
        navigator.clipboard.writeText(code).then(function () {
          copyBtn.textContent = 'Copied!';
          setTimeout(function () { copyBtn.textContent = 'Copy'; }, 1500);
        });
      } else {
        var ta = document.createElement('textarea');
        ta.value = code;
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        copyBtn.textContent = 'Copied!';
        setTimeout(function () { copyBtn.textContent = 'Copy'; }, 1500);
      }
    });
    header.appendChild(copyBtn);
    wrapper.appendChild(header);

    // Code body
    var body = el('div', 'code-viewer-body');
    body.style.maxHeight = maxHeight + 'px';

    renderCodeBody(body, code, language, showLineNumbers);

    wrapper.appendChild(body);

    // Collapsible
    var expanded = !collapsible;
    if (collapsible && lineCount > collapsedLines) {
      body.style.maxHeight = (collapsedLines * 22) + 'px';
      body.classList.add('code-viewer-collapsed');

      var showMore = el('button', 'code-viewer-toggle', 'Show all ' + lineCount + ' lines');
      showMore.addEventListener('click', function () {
        expanded = !expanded;
        if (expanded) {
          body.style.maxHeight = maxHeight + 'px';
          body.classList.remove('code-viewer-collapsed');
          showMore.textContent = 'Collapse';
        } else {
          body.style.maxHeight = (collapsedLines * 22) + 'px';
          body.classList.add('code-viewer-collapsed');
          showMore.textContent = 'Show all ' + lineCount + ' lines';
        }
      });
      wrapper.appendChild(showMore);
    }

    container.appendChild(wrapper);

    return {
      element: wrapper,
      setCode: function (newCode, newLang) {
        code = newCode;
        lines = code.split('\n');
        lineCount = lines.length;
        linesBadge.textContent = lineCount + ' lines';
        if (newLang) {
          language = newLang;
          langBadge.textContent = language.toUpperCase();
        }
        renderCodeBody(body, code, language, showLineNumbers);
      }
    };
  }

  /* ---------- Render code body with line numbers ---------- */

  function renderCodeBody(body, code, language, showLineNumbers) {
    var highlighted;
    if (typeof hljs !== 'undefined') {
      try {
        highlighted = hljs.highlight(code, { language: language }).value;
      } catch (e) {
        highlighted = escapeHtml(code);
      }
    } else {
      highlighted = escapeHtml(code);
    }

    if (showLineNumbers) {
      var htmlLines = highlighted.split('\n');
      // Build a table for proper line number alignment
      var tableHtml = '<table class="cv-table"><tbody>';
      for (var i = 0; i < htmlLines.length; i++) {
        tableHtml += '<tr><td class="cv-gutter">' + (i + 1) + '</td>' +
          '<td class="cv-code">' + (htmlLines[i] || '&nbsp;') + '</td></tr>';
      }
      tableHtml += '</tbody></table>';
      body.innerHTML = tableHtml;
    } else {
      body.innerHTML = '<pre><code class="language-' + language + '">' +
        highlighted + '</code></pre>';
    }
  }

  /* ---------- createTabbedCodeViewer ---------- */

  function createTabbedCodeViewer(container, tabs) {
    if (!tabs || !tabs.length) return null;

    var wrapper = el('div', 'tabbed-code-viewer');

    // Tab bar
    var tabBar = el('div', 'code-tab-bar');
    var viewers = [];
    var codeContainer = el('div', 'code-tab-content');

    tabs.forEach(function (tab, idx) {
      var isActive = tab.active || (idx === 0 && !tabs.some(function(t){return t.active;}));
      var tabBtn = el('button', 'code-tab' + (isActive ? ' code-tab--active' : ''), tab.label);
      tabBtn.dataset.index = idx;
      tabBtn.addEventListener('click', function () {
        var allTabs = tabBar.querySelectorAll('.code-tab');
        allTabs.forEach(function (t) { t.classList.remove('code-tab--active'); });
        tabBtn.classList.add('code-tab--active');
        viewers.forEach(function (v, i) {
          v.element.style.display = i === idx ? '' : 'none';
        });
      });
      tabBar.appendChild(tabBtn);

      // Create viewer (hidden unless active)
      var viewerContainer = el('div', 'code-tab-panel');
      if (!isActive) {
        viewerContainer.style.display = 'none';
      }
      var viewer = createCodeViewer(viewerContainer, {
        code: tab.code || '',
        language: tab.language || 'cpp',
        lineNumbers: true,
        maxHeight: 500,
        collapsible: true,
        collapsedLines: 40,
        title: tab.title || ''
      });
      viewer.element = viewerContainer;
      viewers.push(viewer);
      codeContainer.appendChild(viewerContainer);
    });

    wrapper.appendChild(tabBar);
    wrapper.appendChild(codeContainer);
    container.appendChild(wrapper);

    return {
      element: wrapper,
      setActiveTab: function (idx) {
        var allTabs = tabBar.querySelectorAll('.code-tab');
        allTabs.forEach(function (t, i) {
          t.classList.toggle('code-tab--active', i === idx);
        });
        viewers.forEach(function (v, i) {
          v.element.style.display = i === idx ? '' : 'none';
        });
      }
    };
  }

  /* ---------- Helpers ---------- */

  function escapeHtml(str) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  }

  /* ---------- Public API ---------- */

  return {
    createCodeViewer: createCodeViewer,
    createTabbedCodeViewer: createTabbedCodeViewer
  };
})();
