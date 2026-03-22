'use strict';

// ----------------------------------------------------------------
// Color maps
// ----------------------------------------------------------------
const NODE_COLORS = {
    Bus:       { bg: '#4a90d9', border: '#2171b5', text: '#fff' },
    Generator: { bg: '#4caf50', border: '#2e7d32', text: '#fff' },
    Load:      { bg: '#ff9800', border: '#e65100', text: '#fff' },
    Shunt:     { bg: '#ffeb3b', border: '#f9a825', text: '#333' },
    __other__: { bg: '#90a4ae', border: '#546e7a', text: '#fff' },
};
const EDGE_COLORS = {
    LINEA: '#aaaaaa', LINE: '#aaaaaa',
    TRANSFORMADOR_2W: '#f44336', TRANSFORMER_2W: '#f44336',
    GENERACION: '#81c784', CONSUMO: '#ffb74d',
    __other__: '#607d8b',
};
const KNOWN_EDGE_TYPES = new Set(Object.keys(EDGE_COLORS));

// ----------------------------------------------------------------
// Global state
// ----------------------------------------------------------------
let cy         = null;
let dbLoaded   = false;
let queryHistory = [];
const nodeFilters = new Set(['Bus','Generator','Load','Shunt','__other__']);
const edgeFilters = new Set([...KNOWN_EDGE_TYPES]);

// ----------------------------------------------------------------
// Log console
// ----------------------------------------------------------------
let logState = 'collapsed';
let logHasError = false, logHasWarn = false;

window.appendLog = function(level, msg) {
    const body = document.getElementById('log-body');
    const dot  = document.getElementById('log-dot');
    const time = new Date().toTimeString().slice(0, 8);

    const line = document.createElement('div');
    line.className = 'log-line';
    line.innerHTML =
        `<span class="log-time">${time}</span>` +
        `<span class="log-level ${level}">${level}</span>` +
        `<span class="log-msg">${escHtml(msg)}</span>`;
    body.appendChild(line);
    body.scrollTop = body.scrollHeight;

    if (level === 'ERROR') {
        logHasError = true;
        dot.className = 'has-error';
        if (logState === 'collapsed') setLogState('expanded');
    } else if (level === 'WARN' && !logHasError) {
        logHasWarn = true;
        dot.className = 'has-warn';
    }
};

function escHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function setLogState(s) {
    logState = s;
    const bar = document.getElementById('log-bar');
    bar.className = s === 'collapsed' ? '' : s === 'expanded' ? 'expanded' : 'maximized';
    document.getElementById('btn-log-max').textContent = s === 'maximized' ? '⤡' : '⤢';
}

// ----------------------------------------------------------------
// Cytoscape init
// ----------------------------------------------------------------
function initCy() {
    cy = cytoscape({
        container: document.getElementById('cy'),
        style: cyStyle(),
        layout: { name: 'random' },
        wheelSensitivity: 0.3,
        minZoom: 0.02,
        maxZoom: 10,
    });
    cy.on('tap', 'node', e => showProps(e.target, 'node'));
    cy.on('tap', 'edge', e => showProps(e.target, 'edge'));
    cy.on('tap', e => { if (e.target === cy) closePropPanel(); });
    wireTooltip();
    updateStatus();
}

function cyStyle() {
    return [
        { selector: 'node', style: {
            'background-color': 'data(bg)',
            'border-color':     'data(borderColor)',
            'border-width':     2,
            'color':            'data(textColor)',
            'label':            'data(label)',
            'font-size':        9,
            'text-valign':      'center',
            'text-halign':      'center',
            'width':            28,
            'height':           28,
            'text-wrap':        'ellipsis',
            'text-max-width':   26,
        }},
        { selector: 'node.highlighted', style: {
            'border-width': 4, 'border-color': '#00e5ff',
            'background-color': '#005f73', 'color': '#fff',
        }},
        { selector: 'node:selected', style: {
            'border-width': 3, 'border-color': '#ffffff',
        }},
        { selector: 'node.filtered-out', style: { 'display': 'none' }},
        { selector: 'edge', style: {
            'line-color':         'data(edgeColor)',
            'target-arrow-color': 'data(edgeColor)',
            'target-arrow-shape': 'triangle',
            'arrow-scale':        0.7,
            'width':              'data(edgeWidth)',
            'curve-style':        'bezier',
            'opacity':            0.7,
        }},
        { selector: 'edge.highlighted', style: {
            'line-color': '#00e5ff', 'width': 4, 'opacity': 1,
        }},
        { selector: 'edge.filtered-out', style: { 'display': 'none' }},
    ];
}

// ----------------------------------------------------------------
// Element builders
// ----------------------------------------------------------------
function nodeColor(labels) {
    for (const l of (labels || '').split(' '))
        if (NODE_COLORS[l]) return NODE_COLORS[l];
    return NODE_COLORS.__other__;
}
function edgeColor(type) {
    return EDGE_COLORS[type] || EDGE_COLORS.__other__;
}
function edgeWidth(props) {
    const mva = props.rate_mva || props.rate_A_MVA || props.rate_a_mva || 0;
    return mva ? parseFloat(Math.max(1, Math.min(6, mva / 80)).toFixed(2)) : 1.5;
}
function nodeLabel(labels, props) {
    const v = props.nombre || props.name || props.numero || props.id || '';
    return v ? String(v) : (labels || '').split(' ')[0] || '?';
}
function mkNode(n) {
    const c = nodeColor(n.labels);
    return { group: 'nodes', data: {
        id: n.id, label: nodeLabel(n.labels, n.props),
        labels: n.labels, props: n.props,
        bg: c.bg, borderColor: c.border, textColor: c.text,
    }};
}
function mkEdge(e) {
    return { group: 'edges', data: {
        id: e.id, source: e.source, target: e.target,
        type: e.type, props: e.props,
        edgeColor: edgeColor(e.type),
        edgeWidth: edgeWidth(e.props),
    }};
}

// ----------------------------------------------------------------
// Tooltip
// ----------------------------------------------------------------
const TT = { x: 14, y: 12 };
let ttTimer = null;

function ttShow(el, kind, mx, my) {
    clearTimeout(ttTimer);
    const tip  = document.getElementById('cy-tooltip');
    const body = document.getElementById('cy-tooltip-body');
    const badge = document.getElementById('tt-badge');
    const ttid  = document.getElementById('tt-id');
    body.innerHTML = '';

    if (kind === 'node') {
        const labels = (el.data('labels') || '').split(' ').filter(Boolean);
        const c = nodeColor(el.data('labels'));
        badge.textContent = labels[0] || 'Node';
        badge.style.background = c.bg;
        badge.style.color      = c.text;
        ttid.textContent = `id: ${el.id()}`;
        const props = el.data('props') || {};
        const keys = Object.keys(props);
        if (keys.length === 0) addTTRow(body, '—', 'sin propiedades', true);
        else keys.forEach(k => addTTRow(body, k, props[k]));
        if (labels.length > 1) addTTRow(body, 'labels', labels.join(', '));
    } else {
        const type = el.data('type') || '';
        badge.textContent = type || 'Edge';
        badge.style.background = edgeColor(type);
        badge.style.color = '#fff';
        ttid.textContent = `id: ${el.id()}`;
        const arrow = document.createElement('div');
        arrow.className = 'tt-edge-arrow';
        arrow.innerHTML = `<strong>${el.data('source')}</strong> &rarr; <strong>${el.data('target')}</strong>`;
        body.appendChild(arrow);
        const props = el.data('props') || {};
        const keys = Object.keys(props);
        if (keys.length === 0) addTTRow(body, '—', 'sin propiedades', true);
        else keys.forEach(k => addTTRow(body, k, props[k]));
    }

    tip.style.display = 'block';
    ttPos(tip, mx, my);
}

function addTTRow(container, key, value, dim = false) {
    const row = document.createElement('div');
    row.className = 'tt-row';
    if (dim) {
        const v = document.createElement('span');
        v.className = 'tt-val null'; v.textContent = value;
        row.appendChild(v);
    } else {
        const k = document.createElement('span');
        k.className = 'tt-key'; k.textContent = key;
        const v = document.createElement('span');
        let cls = 'tt-val';
        let txt = value;
        if (value === null || value === undefined) { cls += ' null'; txt = 'null'; }
        else if (typeof value === 'boolean') { cls += ' bool'; txt = String(value); }
        else if (typeof value === 'number')  { cls += ' num';  txt = Number.isInteger(value) ? value : parseFloat(value.toFixed(4)); }
        else txt = String(value);
        v.className = cls; v.textContent = txt;
        row.appendChild(k); row.appendChild(v);
    }
    container.appendChild(row);
}

function ttPos(tip, mx, my) {
    const W = window.innerWidth, H = window.innerHeight;
    tip.style.left = tip.style.top = tip.style.right = tip.style.bottom = 'auto';
    const tw = tip.offsetWidth || 280, th = tip.offsetHeight || 200;
    let x = mx + TT.x, y = my + TT.y;
    if (x + tw > W - 10) x = mx - tw - TT.x;
    if (y + th > H - 10) y = my - th - TT.y;
    tip.style.left = Math.max(4, x) + 'px';
    tip.style.top  = Math.max(4, y) + 'px';
}

function ttHide(delay = 120) {
    clearTimeout(ttTimer);
    ttTimer = setTimeout(() => {
        document.getElementById('cy-tooltip').style.display = 'none';
    }, delay);
}

function wireTooltip() {
    cy.on('mouseover', 'node', e => {
        ttShow(e.target, 'node', e.originalEvent.clientX, e.originalEvent.clientY);
    });
    cy.on('mouseover', 'edge', e => {
        ttShow(e.target, 'edge', e.originalEvent.clientX, e.originalEvent.clientY);
    });
    cy.on('mousemove', 'node, edge', e => {
        const tip = document.getElementById('cy-tooltip');
        if (tip.style.display !== 'none')
            ttPos(tip, e.originalEvent.clientX, e.originalEvent.clientY);
    });
    cy.on('mouseout',  'node, edge', () => ttHide());
    cy.on('viewport',              () => ttHide(0));
    cy.on('tap',                   () => ttHide(0));
}

// ----------------------------------------------------------------
// Load graph
// ----------------------------------------------------------------
async function loadGraph() {
    if (!dbLoaded) { window.appendLog('WARN', 'Abre una base de datos primero.'); return; }
    spinner(true); clearHighlight(); closePropPanel();
    try {
        const r = await window.pywebview.api.load_graph();
        if (!r.ok) { qResult('error', r.error); return; }

        cy.startBatch();
        cy.elements().remove();
        cy.add([...r.nodes.map(mkNode), ...r.edges.map(mkEdge)]);
        cy.endBatch();

        applyFilters();
        runLayout();

        if (r.truncated)
            showTrunc(`Mostrando ${r.shown_nodes} de ${r.total_nodes} nodos (limite 500, por grado)`);
        else
            hideTrunc();

        updateStatus();
    } catch(e) {
        qResult('error', String(e));
        window.appendLog('ERROR', String(e));
    } finally {
        spinner(false);
    }
}

// ----------------------------------------------------------------
// Run Cypher query — replaces graph with query results
// ----------------------------------------------------------------
async function runQuery() {
    if (!dbLoaded) { window.appendLog('WARN', 'Abre una base de datos primero.'); return; }
    const cypher = document.getElementById('cypher-input').value.trim();
    if (!cypher) return;
    pushHistory(cypher);
    spinner(true);
    closePropPanel();
    try {
        const r = await window.pywebview.api.run_query(cypher);
        if (!r.ok) { qResult('error', r.error); return; }

        // replace graph entirely with query results
        cy.startBatch();
        cy.elements().remove();
        cy.add([...r.nodes.map(mkNode), ...r.edges.map(mkEdge)]);
        cy.endBatch();

        applyFilters();
        runLayout();

        if (r.truncated)
            showTrunc(
                `Consulta: mostrando ${r.nodes.length} de ${r.total_found} ` +
                `nodos (limite ${500}, por grado)`
            );
        else
            hideTrunc();

        qResult('ok',
            `${r.rows.length} filas — ${r.nodes.length} nodos, ${r.edges.length} aristas`);
        updateStatus();
    } catch(e) {
        qResult('error', String(e));
        window.appendLog('ERROR', String(e));
    } finally {
        spinner(false);
    }
}

// ----------------------------------------------------------------
// Layout
// ----------------------------------------------------------------
function runLayout() {
    const name = document.getElementById('layout-select').value;
    const opts = { name, animate: true, animationDuration: 500, fit: true, padding: 30 };
    if (name === 'cose') {
        Object.assign(opts, { nodeRepulsion: 4500, idealEdgeLength: 80,
            numIter: 1000, coolingFactor: 0.99, gravity: 1.2, animationThreshold: 250 });
    }
    if (name === 'concentric') {
        opts.concentric = n => n.degree();
        opts.levelWidth = () => 2;
    }
    if (name === 'breadthfirst') {
        opts.directed = false;
        opts.spacingFactor = 1.3;
    }
    const visible = cy.nodes(':visible');
    if (visible.length) visible.layout(opts).run();
}

// ----------------------------------------------------------------
// Filters
// ----------------------------------------------------------------
function applyFilters() {
    cy.startBatch();
    cy.nodes().forEach(n => {
        const labels = (n.data('labels') || '').split(' ');
        const vis = labels.some(l => nodeFilters.has(l)) ||
                    (labels.every(l => !NODE_COLORS[l]) && nodeFilters.has('__other__'));
        n.toggleClass('filtered-out', !vis);
    });
    cy.edges().forEach(e => {
        const type = e.data('type') || '';
        const vis  = edgeFilters.has(type) ||
                     (!KNOWN_EDGE_TYPES.has(type) && edgeFilters.has('__other__'));
        const ends = e.source().hasClass('filtered-out') ||
                     e.target().hasClass('filtered-out');
        e.toggleClass('filtered-out', !vis || ends);
    });
    cy.endBatch();
    updateStatus();
}

// ----------------------------------------------------------------
// Properties panel
// ----------------------------------------------------------------
function showProps(el, kind) {
    const body  = document.getElementById('prop-body');
    const title = document.getElementById('prop-title');
    body.innerHTML = '';
    if (kind === 'node') {
        title.textContent = 'Nodo';
        addPRow(body, 'id', el.id());
        (el.data('labels') || '').split(' ').filter(Boolean).forEach(l => {
            const b = document.createElement('span');
            b.className = 'prop-badge'; b.textContent = l;
            body.appendChild(b);
        });
        Object.entries(el.data('props') || {}).forEach(([k, v]) => addPRow(body, k, v));
    } else {
        title.textContent = 'Arista';
        addPRow(body, 'id', el.id());
        addPRow(body, 'tipo', el.data('type'));
        addPRow(body, 'origen', el.data('source'));
        addPRow(body, 'destino', el.data('target'));
        Object.entries(el.data('props') || {}).forEach(([k, v]) => addPRow(body, k, v));
    }
    document.getElementById('prop-panel').classList.add('open');
}
function addPRow(c, k, v) {
    const r  = document.createElement('div'); r.className = 'prop-row';
    const kk = document.createElement('span'); kk.className = 'prop-key'; kk.textContent = k;
    const vv = document.createElement('span'); vv.className = 'prop-value';
    vv.textContent = (v === null || v === undefined) ? 'null' : String(v);
    r.appendChild(kk); r.appendChild(vv); c.appendChild(r);
}
function closePropPanel() {
    document.getElementById('prop-panel').classList.remove('open');
}

// ----------------------------------------------------------------
// UI helpers
// ----------------------------------------------------------------
function spinner(on) {
    document.getElementById('spinner').style.display = on ? 'block' : 'none';
}
function qResult(cls, msg) {
    const el = document.getElementById('q-result');
    el.className = cls; el.textContent = msg;
}
function showTrunc(msg) {
    const e = document.getElementById('trunc-msg');
    e.textContent = msg; e.style.display = 'block';
}
function hideTrunc() {
    document.getElementById('trunc-msg').style.display = 'none';
}
function clearHighlight() {
    if (cy) cy.elements().removeClass('highlighted');
}
function updateStatus() {
    if (!cy) return;
    const n = cy.nodes(':visible').length;
    const e = cy.edges(':visible').length;
    document.getElementById('statusbar').textContent =
        `${n} nodos / ${e} aristas visibles`;
}

// ----------------------------------------------------------------
// DB state
// ----------------------------------------------------------------
function setDbLoaded(info) {
    dbLoaded = true;
    document.getElementById('db-badge').textContent =
        `${info.db_name}  |  ${info.total_nodes} nodos, ${info.total_rels} aristas`;
    document.getElementById('db-badge').className = 'loaded';
    document.getElementById('welcome').style.display = 'none';
    ['btn-load','btn-center','btn-export','btn-run','btn-close-db']
        .forEach(id => document.getElementById(id).disabled = false);
    document.title = `graphdb Visualizer — ${info.db_name}`;
}
function setDbClosed() {
    dbLoaded = false;
    document.getElementById('db-badge').textContent = 'Sin base de datos';
    document.getElementById('db-badge').className   = '';
    document.getElementById('welcome').style.display = 'flex';
    if (cy) cy.elements().remove();
    hideTrunc(); closePropPanel(); updateStatus();
    ['btn-load','btn-center','btn-export','btn-run','btn-close-db']
        .forEach(id => document.getElementById(id).disabled = true);
    document.title = 'graphdb Visualizer';
}

// ----------------------------------------------------------------
// Query history
// ----------------------------------------------------------------
function pushHistory(q) {
    queryHistory = queryHistory.filter(x => x !== q);
    queryHistory.unshift(q);
    if (queryHistory.length > 10) queryHistory.pop();
    const sel = document.getElementById('q-history');
    sel.innerHTML = '<option value="">— Historial —</option>';
    queryHistory.forEach(qry => {
        const o = document.createElement('option');
        o.value = qry;
        o.textContent = qry.length > 58 ? qry.slice(0, 55) + '...' : qry;
        sel.appendChild(o);
    });
}

// ----------------------------------------------------------------
// Export PNG
// ----------------------------------------------------------------
async function exportPNG() {
    if (!cy || cy.nodes().length === 0) {
        window.appendLog('WARN', 'No hay grafo que exportar.'); return;
    }
    const png64  = cy.png({ output: 'base64uri', full: true, scale: 2 });
    const fname  = `graphdb_${Date.now()}.png`;
    const result = await window.pywebview.api.save_png(png64, fname);
    qResult(result.ok ? 'ok' : 'error',
            result.ok ? `PNG guardado: ${result.path}` : result.error);
}

// ----------------------------------------------------------------
// Wire all events
// ----------------------------------------------------------------
function wire() {

    // Open / close DB
    const doOpen = async () => {
        window.appendLog('INFO', 'Abriendo dialogo de archivo...');
        try {
            const r = await window.pywebview.api.open_database();
            if (r && r.ok) {
                setDbLoaded(r);
            } else if (r && !r.cancelled) {
                window.appendLog('ERROR', r.error || 'Error al abrir BD.');
            }
        } catch(e) {
            window.appendLog('ERROR', String(e));
        }
    };

    document.getElementById('btn-open').addEventListener('click', doOpen);
    document.getElementById('btn-open-welcome').addEventListener('click', doOpen);

    document.getElementById('btn-close-db').addEventListener('click', async () => {
        try {
            await window.pywebview.api.close_database();
        } catch(e) {}
        setDbClosed();
    });

    // Graph controls
    document.getElementById('btn-load').addEventListener('click', loadGraph);
    document.getElementById('btn-center').addEventListener('click', () => {
        if (cy) cy.fit(cy.elements(':visible'), 30);
    });
    document.getElementById('btn-clear').addEventListener('click', () => {
        if (cy) cy.elements().remove();
        hideTrunc(); closePropPanel(); updateStatus(); qResult('', '');
    });
    document.getElementById('btn-export').addEventListener('click', exportPNG);

    // Zoom
    document.getElementById('btn-zoom-in').addEventListener('click', () => {
        cy.zoom(cy.zoom() + 0.25); cy.center();
    });
    document.getElementById('btn-zoom-out').addEventListener('click', () => {
        cy.zoom(Math.max(0.05, cy.zoom() - 0.25)); cy.center();
    });

    // Layout selector
    document.getElementById('layout-select').addEventListener('change', () => {
        if (cy && cy.nodes().length) runLayout();
    });

    // Query
    document.getElementById('btn-run').addEventListener('click', runQuery);
    document.getElementById('cypher-input').addEventListener('keydown', e => {
        if (e.ctrlKey && e.key === 'Enter') runQuery();
    });
    document.getElementById('q-history').addEventListener('change', e => {
        if (e.target.value)
            document.getElementById('cypher-input').value = e.target.value;
    });
    document.getElementById('btn-clear-hl').addEventListener('click', clearHighlight);

    // Properties panel
    document.getElementById('btn-close-prop').addEventListener('click', closePropPanel);

    // Filters — nodes
    document.querySelectorAll('[data-filter="node"]').forEach(cb => {
        cb.addEventListener('change', e => {
            e.target.checked
                ? nodeFilters.add(e.target.dataset.label)
                : nodeFilters.delete(e.target.dataset.label);
            applyFilters();
        });
    });

    // Filters — edges
    document.querySelectorAll('[data-filter="edge"]').forEach(cb => {
        cb.addEventListener('change', e => {
            e.target.checked
                ? edgeFilters.add(e.target.dataset.label)
                : edgeFilters.delete(e.target.dataset.label);
            applyFilters();
        });
    });

    // Log bar
    document.getElementById('log-header').addEventListener('click', () => {
        setLogState(logState === 'collapsed' ? 'expanded' : 'collapsed');
    });
    document.getElementById('btn-log-max').addEventListener('click', e => {
        e.stopPropagation();
        setLogState(logState === 'maximized' ? 'expanded' : 'maximized');
    });
    document.getElementById('btn-log-clear').addEventListener('click', e => {
        e.stopPropagation();
        document.getElementById('log-body').innerHTML = '';
        logHasError = logHasWarn = false;
        document.getElementById('log-dot').className = '';
    });
}

// ----------------------------------------------------------------
// Bootstrap: wait for pywebview API
// ----------------------------------------------------------------
function waitForAPI(fn, tries = 0) {
    if (window.pywebview && window.pywebview.api) {
        fn();
    } else if (tries < 100) {
        setTimeout(() => waitForAPI(fn, tries + 1), 100);
    } else {
        window.appendLog('ERROR', 'No se pudo conectar con la API Python.');
    }
}

window.addEventListener('DOMContentLoaded', () => {
    initCy();
    wire();
    window.appendLog('INFO', 'Interfaz lista.');
    waitForAPI(() => {
        window.appendLog('INFO', 'API Python conectada. Usa "Abrir BD" para comenzar.');
    });
});
