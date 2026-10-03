/**
 * Token Observer - 记录每次对话消耗的 Token，并统计缓存命中/未命中情况。
 *
 * 工作原理：
 *   1. 拦截前端发往 /api/backends/chat-completions 的请求（Provider 的原始响应由酒馆后端原样转发）。
 *   2. 复制一份响应流，逐块扫描其中的 usage 字段（流式 SSE 与普通 JSON 都支持）。
 *   3. 把命中 / 未命中 / 输出 token 汇总，显示在顶栏与扩展设置面板中，并估算费用。
 *
 * 无需修改酒馆服务端，也无需开启 server plugins。
 */

import { extension_settings, renderExtensionTemplateAsync } from '../../../extensions.js';
import { saveSettingsDebounced } from '../../../../script.js';
import { eventSource, event_types } from '../../../events.js';
import { POPUP_TYPE, callGenericPopup } from '../../../popup.js';

const MODULE_NAME = 'token-observer';
const EXTENSION_PATH = 'third-party/token-observer';

const defaultSettings = {
    enabled: true,
    showWidget: true,
    showCost: true,
    autoResetOnChatChange: false,
    priceHit: 0.02,             // 命中缓存 输入 token 单价（元 / 百万 token）
    priceMiss: 1.0,             // 未命中缓存 输入 token 单价（元 / 百万 token）
    priceOut: 2.0,              // 输出 token 单价（元 / 百万 token）
};

/** @type {typeof defaultSettings} */
let settings = { ...defaultSettings };

/** 本次会话累计 */
const session = { hit: 0, miss: 0, out: 0, requests: 0 };
/** 最近一次生成 */
let last = { hit: 0, miss: 0, out: 0, total: 0, model: '', ts: 0 };

const EMPTY_LAST = { hit: 0, miss: 0, out: 0, total: 0, model: '', ts: 0 };

/* ------------------------------------------------------------------ *
 * 工具函数
 * ------------------------------------------------------------------ */

function fmt(n) {
    n = Number(n) || 0;
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return String(Math.round(n));
}

/** 把不同 Provider 的 usage 结构归一化成 { hit, miss, out, total } */
function normalizeUsage(usage) {
    if (!usage || typeof usage !== 'object') return null;

    const prompt = Number(usage.prompt_tokens ?? usage.input_tokens ?? 0) || 0;
    let hit = 0;
    let miss = 0;

    if (usage.prompt_cache_hit_tokens != null || usage.prompt_cache_miss_tokens != null) {
        // DeepSeek
        hit = Number(usage.prompt_cache_hit_tokens ?? 0) || 0;
        miss = Number(usage.prompt_cache_miss_tokens ?? Math.max(prompt - hit, 0)) || 0;
    } else if (usage.prompt_tokens_details && usage.prompt_tokens_details.cached_tokens != null) {
        // OpenAI 系
        hit = Number(usage.prompt_tokens_details.cached_tokens || 0);
        miss = Math.max(prompt - hit, 0);
    } else if (usage.cache_read_input_tokens != null || usage.cache_creation_input_tokens != null) {
        // Anthropic 系
        hit = Number(usage.cache_read_input_tokens || 0);
        miss = (Number(usage.input_tokens || prompt) || 0) + (Number(usage.cache_creation_input_tokens || 0) || 0);
    } else {
        hit = 0;
        miss = prompt;
    }

    const out = Number(usage.completion_tokens ?? usage.output_tokens ?? 0) || 0;
    const total = Number(usage.total_tokens ?? (prompt + out)) || (hit + miss + out);
    return { hit, miss, out, total };
}

function hitRate(hit, miss) {
    const inTokens = hit + miss;
    return inTokens > 0 ? (hit / inTokens) * 100 : 0;
}

function costOf(s) {
    return (s.hit / 1e6) * Number(settings.priceHit || 0)
        + (s.miss / 1e6) * Number(settings.priceMiss || 0)
        + (s.out / 1e6) * Number(settings.priceOut || 0);
}

/* ------------------------------------------------------------------ *
 * 记录 usage
 * ------------------------------------------------------------------ */

function recordUsage(usage, model) {
    if (!settings.enabled) return;
    const u = normalizeUsage(usage);
    if (!u) return;

    last = { ...u, model: model || last.model || '', ts: Date.now() };
    session.hit += u.hit;
    session.miss += u.miss;
    session.out += u.out;
    session.requests += 1;

    renderWidget();
    renderSettingsSummary();
}

function handleSseLine(line) {
    if (!line || !line.startsWith('data:')) return false;
    const payload = line.slice(5).trim();
    if (!payload || payload === '[DONE]') return false;
    try {
        const json = JSON.parse(payload);
        if (json && json.usage) {
            recordUsage(json.usage, json.model);
            return true;
        }
    } catch {
        /* 不完整或非 JSON 的行，忽略 */
    }
    return false;
}

/**
 * 读取一份响应副本，从中提取 usage。
 * 不使用 Content-Type 判断：酒馆后端是直接 pipe 上游响应体，并不会转发 Content-Type，
 * 因此这里按响应内容自行识别（SSE 的 data: 行，或整体 JSON）。
 */
async function inspectClone(clone) {
    const decoder = new TextDecoder();

    let reader = null;
    try {
        reader = clone.body && clone.body.getReader ? clone.body.getReader() : null;
    } catch {
        reader = null;
    }

    if (!reader) {
        let text = '';
        try { text = await clone.text(); } catch { return; }
        harvestText(text);
        return;
    }

    let full = '';
    let buffer = '';
    let found = false;
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const chunk = decoder.decode(value, { stream: true });
            full += chunk;
            buffer += chunk;
            let idx;
            while ((idx = buffer.indexOf('\n')) >= 0) {
                const line = buffer.slice(0, idx).replace(/\r$/, '');
                buffer = buffer.slice(idx + 1);
                if (handleSseLine(line)) found = true;
            }
        }
        full += decoder.decode();
        if (buffer && handleSseLine(buffer)) found = true;
    } catch {
        /* 请求被中断等情况，忽略 */
    }

    if (!found) harvestText(full);
}

/** 处理非流式（整体 JSON）响应 */
function harvestText(text) {
    const trimmed = String(text || '').trim();
    if (!trimmed || !(trimmed.startsWith('{') || trimmed.startsWith('['))) return;
    try {
        const data = JSON.parse(trimmed);
        if (data && data.usage) recordUsage(data.usage, data.model);
    } catch {
        /* 不是 JSON，忽略 */
    }
}

/* ------------------------------------------------------------------ *
 * 拦截 fetch
 * ------------------------------------------------------------------ */

const originalFetch = window.fetch.bind(window);

window.fetch = async function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const isChatCompletion = typeof url === 'string' && url.includes('/api/backends/chat-completions');

    if (!isChatCompletion) {
        return originalFetch(input, init);
    }

    const response = await originalFetch(input, init);

    if (settings.enabled) {
        try {
            const clone = response.clone();
            inspectClone(clone);
        } catch {
            /* 响应不可克隆时忽略，不影响正常生成 */
        }
    }

    return response;
};

/* ------------------------------------------------------------------ *
 * 悬浮统计显示
 * ------------------------------------------------------------------ */

function renderWidget() {
    const el = document.getElementById('token-observer-widget');
    if (!el) return;

    if (!settings.showWidget) {
        el.style.display = 'none';
        return;
    }
    el.style.display = '';

    const inputs = session.hit + session.miss;
    const rate = hitRate(session.hit, session.miss);
    const rateClass = rate >= 60 ? 'to-good' : rate >= 30 ? 'to-mid' : 'to-bad';

    let html = '';
    if (inputs === 0 && session.out === 0) {
        html = `<span class="to-chip to-idle" title="发送一条消息后开始统计"><i class="fa-solid fa-database"></i> Token Observer 待命</span>`;
    } else {
        html = `<span class="to-chip" title="命中缓存 / 未命中缓存 输入 token">`
            + `<i class="fa-solid fa-bolt"></i> 命中 ${fmt(session.hit)} / 未命中 ${fmt(session.miss)}`
            + `</span>`
            + `<span class="to-chip ${rateClass}" title="缓存命中率">${rate.toFixed(1)}%</span>`
            + `<span class="to-chip" title="输出 token"><i class="fa-solid fa-arrow-up-from-bracket"></i> ${fmt(session.out)}</span>`;
        if (settings.showCost) {
            html += `<span class="to-chip" title="按设置中的单价估算">¥${costOf(session).toFixed(4)}</span>`;
        }
    }

    el.innerHTML = html;
}

function injectWidget() {
    if (document.getElementById('token-observer-widget')) return;

    const el = document.createElement('div');
    el.id = 'token-observer-widget';
    el.className = 'token-observer-widget';
    el.title = '点击查看本次生成与累计明细';
    el.addEventListener('click', () => showDetails());

    // 锚定在对话区（#sheld）右下角、输入框上方；顶栏已被酒馆自身图标占满，故不使用顶栏
    const sheld = document.getElementById('sheld');
    if (sheld) {
        sheld.appendChild(el);
    } else {
        const sendForm = document.getElementById('send_form');
        if (!sendForm) return;
        el.classList.add('token-observer-inline');
        sendForm.appendChild(el);
    }
    renderWidget();
}

/* ------------------------------------------------------------------ *
 * 设置面板
 * ------------------------------------------------------------------ */

async function injectSettingsPanel() {
    if (document.getElementById('token-observer-settings')) return;

    const html = await renderExtensionTemplateAsync(EXTENSION_PATH, 'settings');
    const container = document.getElementById('extensions_settings2') || document.getElementById('extensions_settings');
    if (!container) {
        console.warn('[Token Observer] 未找到扩展设置容器，设置面板未挂载');
        return;
    }
    container.insertAdjacentHTML('beforeend', html);
    bindSettingsUI();
    renderSettingsSummary();
}

function bindSettingsUI() {
    const bindCheckbox = (id, key) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.checked = !!settings[key];
        el.addEventListener('change', () => {
            settings[key] = !!el.checked;
            persist();
            renderWidget();
            renderSettingsSummary();
        });
    };

    bindCheckbox('to_enabled', 'enabled');
    bindCheckbox('to_show_widget', 'showWidget');
    bindCheckbox('to_show_cost', 'showCost');
    bindCheckbox('to_auto_reset', 'autoResetOnChatChange');

    const bindPrice = (id, key) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.value = String(settings[key]);
        el.addEventListener('input', () => {
            const v = Number(el.value);
            if (!Number.isFinite(v) || v < 0) return;
            settings[key] = v;
            persist();
            renderWidget();
            renderSettingsSummary();
        });
    };

    bindPrice('to_price_hit', 'priceHit');
    bindPrice('to_price_miss', 'priceMiss');
    bindPrice('to_price_out', 'priceOut');

    const resetBtn = document.getElementById('to_reset');
    if (resetBtn) resetBtn.addEventListener('click', () => resetSession(true));

    const detailBtn = document.getElementById('to_details');
    if (detailBtn) detailBtn.addEventListener('click', () => showDetails());
}

function renderSettingsSummary() {
    const box = document.getElementById('to_summary');
    if (!box) return;

    const inputs = session.hit + session.miss;
    const rate = hitRate(session.hit, session.miss);
    const lines = [
        `累计请求：${session.requests} 次`,
        `命中缓存：${session.hit.toLocaleString()} tokens`,
        `未命中缓存：${session.miss.toLocaleString()} tokens`,
        `缓存命中率：${rate.toFixed(1)}%（输入共 ${inputs.toLocaleString()} tokens）`,
        `输出：${session.out.toLocaleString()} tokens`,
    ];
    if (settings.showCost) {
        lines.push(`估算费用：¥${costOf(session).toFixed(4)}`);
    }
    if (last.ts) {
        lines.push(`—— 最近一次：命中 ${last.hit.toLocaleString()} / 未命中 ${last.miss.toLocaleString()} / 输出 ${last.out.toLocaleString()}${last.model ? ' · ' + last.model : ''}`);
    }
    box.textContent = lines.join('\n');
}

function resetSession(notify) {
    session.hit = 0;
    session.miss = 0;
    session.out = 0;
    session.requests = 0;
    last = { ...EMPTY_LAST };
    renderWidget();
    renderSettingsSummary();
    if (notify) toastr.info('Token Observer 统计已重置', 'Token Observer');
}

/* ------------------------------------------------------------------ *
 * 明细弹窗
 * ------------------------------------------------------------------ */

async function showDetails() {
    const inputs = session.hit + session.miss;
    const rate = hitRate(session.hit, session.miss);

    const rows = [
        ['本次请求数', String(session.requests)],
        ['命中缓存 (输入)', session.hit.toLocaleString()],
        ['未命中缓存 (输入)', session.miss.toLocaleString()],
        ['输入合计', inputs.toLocaleString()],
        ['缓存命中率', rate.toFixed(1) + '%'],
        ['输出', session.out.toLocaleString()],
    ];
    if (settings.showCost) {
        rows.push(['估算费用', '¥' + costOf(session).toFixed(4)]);
    }
    if (last.ts) {
        rows.push(['最近一次 · 时间', new Date(last.ts).toLocaleTimeString()]);
        rows.push(['最近一次 · 模型', last.model || '—']);
        rows.push(['最近一次 · 命中/未命中/输出', `${last.hit.toLocaleString()} / ${last.miss.toLocaleString()} / ${last.out.toLocaleString()}`]);
    }

    const table = rows
        .map(([k, v]) => `<tr><td class="to-k">${k}</td><td class="to-v">${v}</td></tr>`)
        .join('');

    const html = `<div class="token-observer-popup">
        <h3>Token Observer · Token 消耗明细</h3>
        <table class="to-table">${table}</table>
        <p class="to-hint">数据来源为 Provider 返回的 usage 字段（DeepSeek / OpenAI / Anthropic 均已适配）。费用按设置中的单价估算，仅作参考。</p>
        <div class="to-actions">
            <div id="to_popup_reset" class="menu_button">重置统计</div>
        </div>
    </div>`;

    const dialog = $(html);
    dialog.find('#to_popup_reset').on('click', () => {
        resetSession(false);
        toastr.info('Token Observer 统计已重置', 'Token Observer');
        dialog.find('.to-table').replaceWith(buildDetailTable());
    });

    await callGenericPopup(dialog, POPUP_TYPE.TEXT, '', { wide: false, large: false, allowVerticalScrolling: true });
}

function buildDetailTable() {
    const inputs = session.hit + session.miss;
    const rate = hitRate(session.hit, session.miss);
    const rows = [
        ['本次请求数', String(session.requests)],
        ['命中缓存 (输入)', session.hit.toLocaleString()],
        ['未命中缓存 (输入)', session.miss.toLocaleString()],
        ['输入合计', inputs.toLocaleString()],
        ['缓存命中率', rate.toFixed(1) + '%'],
        ['输出', session.out.toLocaleString()],
    ];
    if (settings.showCost) rows.push(['估算费用', '¥' + costOf(session).toFixed(4)]);
    return `<table class="to-table">${rows.map(([k, v]) => `<tr><td class="to-k">${k}</td><td class="to-v">${v}</td></tr>`).join('')}</table>`;
}

/* ------------------------------------------------------------------ *
 * 初始化
 * ------------------------------------------------------------------ */

function persist() {
    extension_settings[MODULE_NAME] = settings;
    saveSettingsDebounced();
}

async function init() {
    settings = Object.assign({}, defaultSettings, extension_settings[MODULE_NAME] || {});
    extension_settings[MODULE_NAME] = settings;

    injectWidget();
    await injectSettingsPanel();
    renderWidget();
    renderSettingsSummary();

    eventSource.on(event_types.CHAT_CHANGED, () => {
        if (settings.autoResetOnChatChange) resetSession(false);
    });
}

eventSource.on(event_types.APP_READY, init);