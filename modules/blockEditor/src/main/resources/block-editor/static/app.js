const NODE_LIBRARY = {
  events: [
    { type: 'EVENT_ON_ENABLE', title: 'PluginEnableEvent' },
    { type: 'EVENT_ON_PLAYER_JOIN', title: 'PlayerJoinEvent' },
    { type: 'EVENT_ON_PLAYER_CHAT', title: 'PlayerChatEvent' }
  ],
  functions: [
    { type: 'FUNCTION_SEND_MESSAGE', title: 'Player.sendMessage', defaultMessage: 'Hello from function' },
    { type: 'FUNCTION_BROADCAST_MESSAGE', title: 'broadcastMessage', defaultMessage: 'Hello everyone' }
  ],
  operations: [
    { type: 'MATH_ADD', title: '+' },
    { type: 'MATH_SUB', title: '-' },
    { type: 'MATH_MUL', title: '×' },
    { type: 'MATH_DIV', title: '÷' }
  ],
  variables: [
    { type: 'VAR_TEXT', title: '문자열', defaultValue: 'hello' },
    { type: 'VAR_INTEGER', title: '정수', defaultValue: '0' },
    { type: 'VAR_DECIMAL', title: '실수', defaultValue: '0.0' },
    { type: 'VAR_BOOLEAN', title: '논리형', defaultValue: 'false' }
  ]
};

const typeTitleMap = new Map([...NODE_LIBRARY.events, ...NODE_LIBRARY.functions, ...NODE_LIBRARY.operations, ...NODE_LIBRARY.variables].map((it) => [it.type, it.title]));

const state = {
  blocks: [],
  links: [],
  comments: [],
  projectMeta: {
    pluginId: 'aerogel-studio-plugin',
    pluginName: 'Aerogel Studio Plugin',
    version: '1.0.0'
  },
  workspaceItems: [],
  workspaceSnapshots: {},
  selectedWorkspaceId: null,
  currentWorkspaceId: null,
  nextId: 1,
  nextCommentId: 1,
  selectedId: null,
  selectedCommentId: null,
  panX: 0,
  panY: 0,
  scale: 1,
  draggingNode: null,
  draggingComment: null,
  resizingComment: null,
  commentDraft: null,
  panning: null,
  spaceDown: false,
  connectionDraft: null,
  linkingActive: false,
  draggingItemId: null,
  draggingItemKind: null,
  workspaceDropFolderId: null,
  workspaceDropItemId: null,
  workspaceDropAfter: false,
  workspaceMenuTargetId: null,
  mouseWorld: { x: 0, y: 0 },
  contextMenuWorld: { x: 0, y: 0 },
  nodeClipboard: null,
  clipboardPasteCount: 0
};

const BLOCK_NODE_WIDTH = 260;
const BLOCK_NODE_ESTIMATED_HEIGHT = 132;
const GRID_SIZE = 24;
const WORKSPACE_STORAGE_KEY = 'aerogel.blockEditor.workspaces.v1';
const PRIVATE_KEY = new URLSearchParams(window.location.search).get('privatekey') || '';
const HISTORY_LIMIT = 200;
const AI_MODEL_CACHE_NAME = 'aerogel.blockEditor.aiModels.v1';
const LOCAL_EMBED_MODEL_ID = 'Xenova/bge-small-en-v1.5';
const LOCAL_GENERATE_MODEL_ID = 'onnx-community/Qwen2.5-0.5B-Instruct';
const TRANSFORMERS_JS_CDN = 'https://cdn.jsdelivr.net/npm/@huggingface/transformers@3.8.1';
const AI_MODEL_ASSET_LIST = [
  'https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main/config.json',
  'https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main/tokenizer.json',
  'https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main/tokenizer_config.json',
  'https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main/special_tokens_map.json',
  'https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main/onnx/model_quantized.onnx',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/config.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/generation_config.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/tokenizer.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/tokenizer_config.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/special_tokens_map.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/added_tokens.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/merges.txt',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/vocab.json',
  'https://huggingface.co/onnx-community/Qwen2.5-0.5B-Instruct/resolve/main/onnx/model_quantized.onnx'
];
const historyState = {
  undo: [],
  redo: [],
  applying: false,
  lastSerialized: ''
};
const autosaveState = {
  timer: null,
  serverTimer: null,
  serverInFlight: false,
  lastServerSerialized: ''
};
const realtimeState = {
  clientId: `c_${Math.random().toString(36).slice(2, 10)}`,
  eventSource: null,
  connected: false,
  lastPublishedSerialized: '',
  lastPresenceSerialized: '',
  publishTimer: null,
  presenceTimer: null,
  publishInFlight: false,
  publishQueued: false,
  presenceInFlight: false,
  presenceQueued: false,
  peers: {},
  smoothedPeers: {},
  presenceAnimationFrame: null,
  lastPresenceFrameAt: 0,
  suspended: false
};
const renderPresenceOverrides = {
  nodePositions: new Map(),
  commentRects: new Map()
};
const SHOW_SELF_PRESENCE_DEBUG = false;
const autoCreateState = {
  generateInFlight: false,
  targetWorkspaceId: null,
  targetOriginWorld: null
};
const actionBarState = {
  timer: null,
  progressHideTimer: null,
  logs: [],
  expanded: false,
  lastProgressMessage: '',
  progressVisible: false,
  progressPercent: 0,
  progressLabel: ''
};
const minimapState = {
  bounds: null,
  scale: 1,
  offsetX: 0,
  offsetY: 0
};
const hoverTooltipState = {
  el: null,
  target: null
};
const graphDecisionState = {
  timer: null,
  mlInFlight: false,
  mlQueued: false,
  lastFingerprint: '',
  lastPublished: null,
  rule: null,
  ml: null
};
const aiModelState = {
  prefetchInFlight: false,
  prefetchDone: false,
  downloadedBytes: 0,
  downloadedCount: 0,
  totalCount: AI_MODEL_ASSET_LIST.length,
  error: ''
};
const localAiRuntimeState = {
  loading: false,
  ready: false,
  error: '',
  transformers: null,
  embedder: null,
  generator: null,
  prototypeReady: false,
  goodPrototype: null,
  badPrototype: null
};
const aiLoadProgressState = {
  active: false,
  downloadRatio: 0,
  runtimeRatio: 0,
  lastPercent: 0,
  finished: false
};

const el = {
  pluginSelectField: document.querySelector('.plugin-select-field'),
  pluginSelectTrigger: document.getElementById('pluginSelectTrigger'),
  pluginSelectLabel: document.getElementById('pluginSelectLabel'),
  pluginSelectMenu: document.getElementById('pluginSelectMenu'),
  pluginSelect: document.getElementById('pluginSelect'),
  workspaceHub: document.querySelector('.workspace-hub'),
  workspaceTree: document.getElementById('workspaceTree'),
  workspaceHint: document.getElementById('workspaceHint'),
  workspaceTreeMenu: document.getElementById('workspaceTreeMenu'),
  addFolderMenuBtn: document.getElementById('addFolderMenuBtn'),
  addWorkspaceMenuBtn: document.getElementById('addWorkspaceMenuBtn'),
  renameWorkspaceMenuBtn: document.getElementById('renameWorkspaceMenuBtn'),
  deleteWorkspaceMenuBtn: document.getElementById('deleteWorkspaceMenuBtn'),
  nameDialog: document.getElementById('nameDialog'),
  nameDialogTitle: document.getElementById('nameDialogTitle'),
  nameDialogInput: document.getElementById('nameDialogInput'),
  nameDialogCancelBtn: document.getElementById('nameDialogCancelBtn'),
  nameDialogConfirmBtn: document.getElementById('nameDialogConfirmBtn'),
  confirmDialog: document.getElementById('confirmDialog'),
  confirmDialogTitle: document.getElementById('confirmDialogTitle'),
  confirmDialogMessage: document.getElementById('confirmDialogMessage'),
  confirmDialogCancelBtn: document.getElementById('confirmDialogCancelBtn'),
  confirmDialogOkBtn: document.getElementById('confirmDialogOkBtn'),
  autoCreateDialog: document.getElementById('autoCreateDialog'),
  autoCreateDialogTitle: document.getElementById('autoCreateDialogTitle'),
  autoCreateAuthStatus: document.getElementById('autoCreateAuthStatus'),
  autoCreatePromptInput: document.getElementById('autoCreatePromptInput'),
  autoCreateCancelBtn: document.getElementById('autoCreateCancelBtn'),
  autoCreateGenerateBtn: document.getElementById('autoCreateGenerateBtn'),
  saveBtn: document.getElementById('saveBtn'),
  workspace: document.getElementById('workspace'),
  workspaceToolbar: document.getElementById('workspaceToolbar'),
  actionBar: document.getElementById('actionBar'),
  actionBarToggle: document.getElementById('actionBarToggle'),
  actionBarMessage: document.getElementById('actionBarMessage'),
  actionBarProgress: document.getElementById('actionBarProgress'),
  actionBarProgressFill: document.getElementById('actionBarProgressFill'),
  actionBarProgressLabel: document.getElementById('actionBarProgressLabel'),
  actionBarLogs: document.getElementById('actionBarLogs'),
  minimapCanvas: document.getElementById('minimapCanvas'),
  canvas: document.getElementById('canvas'),
  worldInfo: document.getElementById('worldInfo'),
  contextMenu: document.getElementById('contextMenu')
};

function ensureActionBarStructure() {
  if (!(el.actionBar instanceof HTMLElement)) return;
  let head = el.actionBar.querySelector('.action-bar-head');
  if (!(head instanceof HTMLElement)) {
    head = document.createElement('div');
    head.className = 'action-bar-head';
    el.actionBar.appendChild(head);
  }
  if (!(el.actionBarMessage instanceof HTMLElement)) {
    const message = document.createElement('div');
    message.id = 'actionBarMessage';
    message.className = 'action-bar-message';
    head.appendChild(message);
    el.actionBarMessage = message;
  } else if (!head.contains(el.actionBarMessage)) {
    head.appendChild(el.actionBarMessage);
  }
  if (el.actionBarToggle instanceof HTMLElement) {
    el.actionBarToggle.remove();
    el.actionBarToggle = null;
  }
  if (!(el.actionBarLogs instanceof HTMLElement)) {
    const logs = document.createElement('div');
    logs.id = 'actionBarLogs';
    logs.className = 'action-bar-logs';
    el.actionBar.appendChild(logs);
    el.actionBarLogs = logs;
  } else if (!el.actionBar.contains(el.actionBarLogs)) {
    el.actionBar.appendChild(el.actionBarLogs);
  }
  if (!(el.actionBarProgress instanceof HTMLElement)) {
    const progress = document.createElement('div');
    progress.id = 'actionBarProgress';
    progress.className = 'action-bar-progress hidden';
    const track = document.createElement('div');
    track.className = 'action-bar-progress-track';
    const fill = document.createElement('div');
    fill.id = 'actionBarProgressFill';
    fill.className = 'action-bar-progress-fill';
    track.appendChild(fill);
    const label = document.createElement('div');
    label.id = 'actionBarProgressLabel';
    label.className = 'action-bar-progress-label';
    label.textContent = '0%';
    progress.appendChild(track);
    progress.appendChild(label);
    if (el.actionBarLogs && el.actionBarLogs.parentElement === el.actionBar) {
      el.actionBar.insertBefore(progress, el.actionBarLogs);
    } else {
      el.actionBar.appendChild(progress);
    }
    el.actionBarProgress = progress;
    el.actionBarProgressFill = fill;
    el.actionBarProgressLabel = label;
  }
}

function renderActionBarProgress() {
  ensureActionBarStructure();
  if (!(el.actionBarProgress instanceof HTMLElement)) return;
  const visible = !!actionBarState.progressVisible;
  el.actionBarProgress.classList.toggle('hidden', !visible);
  const percent = Math.max(0, Math.min(100, Number(actionBarState.progressPercent || 0)));
  if (el.actionBarProgressFill instanceof HTMLElement) {
    el.actionBarProgressFill.style.width = `${percent.toFixed(1)}%`;
  }
  if (el.actionBarProgressLabel instanceof HTMLElement) {
    el.actionBarProgressLabel.textContent = `${Math.round(percent)}%`;
  }
}

function setActionBarProgress(percent, label = '', options = {}) {
  const reset = !!options.reset;
  actionBarState.progressVisible = true;
  const next = Math.max(0, Math.min(100, Number(percent || 0)));
  actionBarState.progressPercent = reset
    ? next
    : Math.max(Math.max(0, Math.min(100, Number(actionBarState.progressPercent || 0))), next);
  actionBarState.progressLabel = String(label || '');
  renderActionBarProgress();
}

function hideActionBarProgress() {
  if (actionBarState.progressHideTimer) {
    clearTimeout(actionBarState.progressHideTimer);
    actionBarState.progressHideTimer = null;
  }
  actionBarState.progressVisible = false;
  actionBarState.progressPercent = 0;
  actionBarState.progressLabel = '';
  renderActionBarProgress();
}

function resetUnifiedAiLoadProgress() {
  aiLoadProgressState.active = false;
  aiLoadProgressState.downloadRatio = 0;
  aiLoadProgressState.runtimeRatio = 0;
  aiLoadProgressState.lastPercent = 0;
  aiLoadProgressState.finished = false;
}

function beginUnifiedAiLoadProgress() {
  if (aiLoadProgressState.active) return;
  aiLoadProgressState.active = true;
  aiLoadProgressState.downloadRatio = 0;
  aiLoadProgressState.runtimeRatio = 0;
  aiLoadProgressState.lastPercent = 0;
  aiLoadProgressState.finished = false;
  showActionBar('AI 모델 준비 중...', '', 0, { progress: 0, progressReset: true, keepProgress: true });
}

function updateUnifiedAiLoadProgress(part, ratio, statusMessage = '') {
  beginUnifiedAiLoadProgress();
  const r = Math.max(0, Math.min(1, Number(ratio || 0)));
  if (part === 'download') {
    aiLoadProgressState.downloadRatio = Math.max(aiLoadProgressState.downloadRatio, r);
  } else if (part === 'runtime') {
    aiLoadProgressState.runtimeRatio = Math.max(aiLoadProgressState.runtimeRatio, r);
  }
  const weighted = (aiLoadProgressState.downloadRatio * 0.75) + (aiLoadProgressState.runtimeRatio * 0.25);
  const percent = Math.max(aiLoadProgressState.lastPercent, Math.round(weighted * 100));
  aiLoadProgressState.lastPercent = percent;
  setActionBarProgress(percent, '');
  if (statusMessage) {
    showActionBar(statusMessage, '', 0, { keepProgress: true });
  }
}

function finalizeUnifiedAiLoadProgress(sizeText = '') {
  if (aiLoadProgressState.finished) return;
  aiLoadProgressState.finished = true;
  aiLoadProgressState.downloadRatio = 1;
  aiLoadProgressState.runtimeRatio = 1;
  aiLoadProgressState.lastPercent = 100;
  showActionBar(`AI 모델 준비 완료${sizeText}`, 'ok', 1200, { progress: 100, progressLabel: '완료' });
  setTimeout(() => {
    resetUnifiedAiLoadProgress();
  }, 1400);
}

function setResult(value) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  console.info(`[AerogelStudio] ${text}`);
}

function waitForNextUiTick() {
  return new Promise((resolve) => {
    requestAnimationFrame(() => {
      setTimeout(resolve, 0);
    });
  });
}

function hasActiveTextSelection() {
  try {
    const selection = window.getSelection();
    return !!selection && !selection.isCollapsed && selection.toString().length > 0;
  } catch {
    return false;
  }
}

function formatBytes(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return '0B';
  if (num < 1024) return `${Math.round(num)}B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)}KB`;
  if (num < 1024 * 1024 * 1024) return `${(num / (1024 * 1024)).toFixed(1)}MB`;
  return `${(num / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}

function fileNameFromUrl(url) {
  const raw = String(url || '');
  const idx = raw.lastIndexOf('/');
  if (idx < 0) return raw;
  return raw.slice(idx + 1) || raw;
}

async function downloadAssetWithProgress(url, onProgress) {
  const response = await fetch(url, { method: 'GET' });
  if (!response.ok) throw new Error(`다운로드 실패 (${response.status})`);
  const total = Number(response.headers.get('content-length') || 0);
  const readable = response.body;
  if (!readable || typeof readable.getReader !== 'function') {
    const buf = await response.arrayBuffer();
    const bytes = new Uint8Array(buf);
    if (typeof onProgress === 'function') onProgress(bytes.byteLength, bytes.byteLength, 1);
    return {
      bytes,
      totalBytes: bytes.byteLength,
      headers: response.headers
    };
  }

  const reader = readable.getReader();
  const chunks = [];
  let loaded = 0;
  let done = false;
  while (!done) {
    const read = await reader.read();
    done = !!read.done;
    if (done) break;
    const value = read.value;
    if (!(value instanceof Uint8Array)) continue;
    chunks.push(value);
    loaded += value.byteLength;
    if (typeof onProgress === 'function') {
      const ratio = total > 0 ? Math.max(0, Math.min(1, loaded / total)) : 0;
      onProgress(loaded, total, ratio);
    }
  }
  const merged = new Uint8Array(loaded);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  if (typeof onProgress === 'function') onProgress(loaded, total, 1);
  return {
    bytes: merged,
    totalBytes: loaded,
    headers: response.headers
  };
}

function averageVectors(vectors) {
  if (!Array.isArray(vectors) || vectors.length === 0) return null;
  const first = Array.isArray(vectors[0]) ? vectors[0] : null;
  if (!first || first.length === 0) return null;
  const out = new Array(first.length).fill(0);
  for (const v of vectors) {
    if (!Array.isArray(v) || v.length !== out.length) continue;
    for (let i = 0; i < out.length; i += 1) out[i] += Number(v[i] || 0);
  }
  for (let i = 0; i < out.length; i += 1) out[i] /= Math.max(1, vectors.length);
  return out;
}

function cosineSimilarity(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length === 0 || a.length !== b.length) return 0;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i += 1) {
    const av = Number(a[i] || 0);
    const bv = Number(b[i] || 0);
    dot += av * bv;
    na += av * av;
    nb += bv * bv;
  }
  if (na <= 0 || nb <= 0) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
}

async function extractEmbeddingVector(embedder, text) {
  const out = await embedder(String(text || ''), { pooling: 'mean', normalize: true });
  const data = out?.data;
  if (!data || typeof data.length !== 'number') return null;
  return Array.from(data);
}

async function ensureLocalAiRuntime(options = {}) {
  const silent = !!options.silent;
  if (localAiRuntimeState.ready) return true;
  if (localAiRuntimeState.loading) return false;
  localAiRuntimeState.loading = true;
  localAiRuntimeState.error = '';
  showActionBar('로컬 ONNX 런타임 로드 중...', '', 0, { keepProgress: true });
  beginUnifiedAiLoadProgress();
  updateUnifiedAiLoadProgress('runtime', 0.06, '로컬 ONNX 런타임 로드 중...');
  try {
    updateUnifiedAiLoadProgress('runtime', 0.18, 'transformers 로딩');
    const tfm = await import(TRANSFORMERS_JS_CDN);
    const env = tfm?.env || {};
    const browserCacheAvailable = !!window.caches && typeof window.caches.open === 'function';
    env.allowRemoteModels = true;
    env.allowLocalModels = false;
    env.useBrowserCache = browserCacheAvailable;
    env.useFSCache = false;
    if (env?.backends?.onnx?.wasm) {
      // Keep UI responsive during heavy inference by proxying wasm execution to a worker.
      env.backends.onnx.wasm.proxy = true;
    }
    const supportsWebGpu = !!navigator.gpu;
    const preferredDevice = supportsWebGpu ? 'webgpu' : 'wasm';

    updateUnifiedAiLoadProgress('runtime', 0.55, '임베딩 모델 로드');
    let embedder;
    try {
      embedder = await tfm.pipeline('feature-extraction', LOCAL_EMBED_MODEL_ID, {
        dtype: 'q8',
        device: preferredDevice
      });
    } catch {
      embedder = await tfm.pipeline('feature-extraction', LOCAL_EMBED_MODEL_ID, {
        dtype: 'q8',
        device: 'wasm'
      });
    }
    updateUnifiedAiLoadProgress('runtime', 0.82, '생성 모델 로드');
    let generator;
    try {
      generator = await tfm.pipeline('text-generation', LOCAL_GENERATE_MODEL_ID, {
        dtype: 'q8',
        model_file_name: 'model',
        device: preferredDevice
      });
    } catch {
      generator = await tfm.pipeline('text-generation', LOCAL_GENERATE_MODEL_ID, {
        dtype: 'q8',
        model_file_name: 'model',
        device: 'wasm'
      });
    }

    localAiRuntimeState.transformers = tfm;
    localAiRuntimeState.embedder = embedder;
    localAiRuntimeState.generator = generator;
    localAiRuntimeState.ready = true;
    updateUnifiedAiLoadProgress('runtime', 1, '런타임 로드 완료');
    refreshAutoCreateGenerateEnabled();
    if (!(el.autoCreateDialog?.classList?.contains('hidden'))) {
      setAutoCreateAuthStatus('로컬 AI 준비 완료', 'ok');
    }
    if (aiModelState.prefetchDone) {
      finalizeUnifiedAiLoadProgress(aiModelState.downloadedBytes > 0 ? ` (${formatBytes(aiModelState.downloadedBytes)})` : '');
    } else if (!silent) {
      showActionBar('로컬 런타임 준비 완료 (다운로드 진행 중)', '', 0, { keepProgress: true });
    }
    return true;
  } catch (error) {
    localAiRuntimeState.error = error?.message || 'unknown error';
    refreshAutoCreateGenerateEnabled();
    resetUnifiedAiLoadProgress();
    if (!silent) {
      showActionBar(`로컬 AI 로드 실패: ${localAiRuntimeState.error}`, 'error', 3000);
    } else {
      showActionBar(`로컬 AI 로드 실패: ${localAiRuntimeState.error}`, 'error', 1400, { progress: Number.NaN });
    }
    return false;
  } finally {
    localAiRuntimeState.loading = false;
  }
}

function buildLocalRiskText(snapshot, ruleResult) {
  const blocks = Array.isArray(snapshot?.blocks) ? snapshot.blocks : [];
  const links = Array.isArray(snapshot?.links) ? snapshot.links : [];
  const issues = Array.isArray(ruleResult?.issues) ? ruleResult.issues : [];
  const warnings = Array.isArray(ruleResult?.warnings) ? ruleResult.warnings : [];
  const nodeTypeCount = new Map();
  for (const node of blocks) {
    const t = String(node?.type || 'UNKNOWN');
    nodeTypeCount.set(t, (nodeTypeCount.get(t) || 0) + 1);
  }
  const topTypes = Array.from(nodeTypeCount.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([k, v]) => `${k}:${v}`)
    .join(', ');
  const topIssue = issues.slice(0, 5).map((it) => `${it.code}:${it.message}`).join(' | ');
  const topWarn = warnings.slice(0, 5).map((it) => `${it.code}:${it.message}`).join(' | ');
  return [
    `nodes=${blocks.length}, links=${links.length}`,
    `canRun=${ruleResult?.canRun ? 'yes' : 'no'}`,
    `types=${topTypes}`,
    `issues=${topIssue || 'none'}`,
    `warnings=${topWarn || 'none'}`
  ].join('\n');
}

function buildRenderedAabbSnapshot(limit = 120) {
  const items = [];
  for (const node of state.blocks.slice(0, limit)) {
    const rect = nodeRectOf(node);
    items.push({
      id: node.id,
      type: node.type,
      x: Math.round(rect.x),
      y: Math.round(rect.y),
      width: Math.round(rect.width),
      height: Math.round(rect.height)
    });
  }
  return items;
}

async function ensureLocalRiskPrototypes() {
  if (localAiRuntimeState.prototypeReady) return true;
  const embedder = localAiRuntimeState.embedder;
  if (!embedder) return false;
  const goodCases = [
    'nodes=3 links=2 canRun=yes issues=none warnings=none event connected to action and required inputs linked',
    'nodes=6 links=7 canRun=yes issues=none warnings=none valid type links and reachable actions',
    'clean execution chain with event root and message/player inputs satisfied'
  ];
  const badCases = [
    'canRun=no invalid_link_type type mismatch between output and input',
    'canRun=no dangling_link link points to missing node',
    'warnings unreachable_action and missing event root with sparse links',
    'required input missing player or message and execution disconnected'
  ];
  const goodVectors = [];
  for (const text of goodCases) {
    const v = await extractEmbeddingVector(embedder, text);
    if (v) goodVectors.push(v);
  }
  const badVectors = [];
  for (const text of badCases) {
    const v = await extractEmbeddingVector(embedder, text);
    if (v) badVectors.push(v);
  }
  localAiRuntimeState.goodPrototype = averageVectors(goodVectors);
  localAiRuntimeState.badPrototype = averageVectors(badVectors);
  localAiRuntimeState.prototypeReady = !!localAiRuntimeState.goodPrototype && !!localAiRuntimeState.badPrototype;
  return localAiRuntimeState.prototypeReady;
}

async function runLocalModelRiskAssist(snapshot, ruleResult) {
  if (!localAiRuntimeState.ready) return null;
  if (!localAiRuntimeState.embedder) return null;
  const protoReady = await ensureLocalRiskPrototypes();
  if (!protoReady) return null;
  const text = buildLocalRiskText(snapshot, ruleResult);
  const vector = await extractEmbeddingVector(localAiRuntimeState.embedder, text);
  if (!vector) return null;
  const goodSim = cosineSimilarity(vector, localAiRuntimeState.goodPrototype);
  const badSim = cosineSimilarity(vector, localAiRuntimeState.badPrototype);
  const risk = clamp01(sigmoid((badSim - goodSim) * 6.5));
  let reason = '유사 사례 기반 낮은 위험';
  if (risk >= 0.65) reason = '유사 사례 기반 높은 실패 위험';
  else if (risk >= 0.45) reason = '유사 사례 기반 보통 실패 위험';
  return {
    failProbability: Number(risk.toFixed(4)),
    reason,
    sims: {
      good: Number(goodSim.toFixed(4)),
      bad: Number(badSim.toFixed(4))
    }
  };
}

function cleanGeneratedText(value) {
  return String(value || '')
    .replace(/<\s*pad\s*>/gi, ' ')
    .replace(/<\s*\/s\s*>/gi, ' ')
    .replace(/<\s*s\s*>/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function collectGeneratorTexts(value, out, depth = 0) {
  if (depth > 6 || !out) return;
  if (typeof value === 'string') {
    const cleaned = cleanGeneratedText(value);
    if (cleaned) out.push(cleaned);
    return;
  }
  if (!value || typeof value !== 'object') return;
  if (Array.isArray(value)) {
    for (const item of value) collectGeneratorTexts(item, out, depth + 1);
    return;
  }
  const directKeys = ['generated_text', 'summary_text', 'text', 'content', 'answer', 'output'];
  for (const key of directKeys) {
    if (key in value) collectGeneratorTexts(value[key], out, depth + 1);
  }
  for (const key of Object.keys(value)) {
    if (directKeys.includes(key)) continue;
    collectGeneratorTexts(value[key], out, depth + 1);
  }
}

function textFromGeneratorOutput(output) {
  const candidates = [];
  collectGeneratorTexts(output, candidates, 0);
  if (!candidates.length) return '';
  candidates.sort((a, b) => b.length - a.length);
  return candidates[0] || '';
}

async function generateWithLocalModel(promptText, maxNewTokens = 180) {
  const ready = localAiRuntimeState.ready || await ensureLocalAiRuntime({ silent: true });
  if (!ready || !localAiRuntimeState.generator) {
    throw new Error(localAiRuntimeState.error || '로컬 생성 모델을 사용할 수 없습니다.');
  }
  const rawPrompt = String(promptText || '');
  const MAX_LOCAL_PROMPT_CHARS = 5200;
  const prompt = rawPrompt.length > MAX_LOCAL_PROMPT_CHARS
    ? `${rawPrompt.slice(0, 2200)}\n[...truncated...]\n${rawPrompt.slice(-800)}`
    : rawPrompt;
  const output = await localAiRuntimeState.generator(prompt, {
    max_new_tokens: maxNewTokens,
    min_new_tokens: 4,
    do_sample: false,
    temperature: 0.1,
    return_full_text: false
  });
  const text = textFromGeneratorOutput(output);
  if (!text) throw new Error('로컬 생성 모델 출력이 비어 있습니다.');
  return text;
}

function normalizeAiActionAliases(text) {
  const raw = String(text || '').toLowerCase();
  if (!raw.trim()) return '';
  let normalized = raw;
  normalized = normalized.replace(/실행\s*연결|exec\s*연결|실행\s*포트/g, ' connect_exec ');
  normalized = normalized.replace(/메시지\s*연결|message\s*연결|텍스트\s*연결/g, ' connect_message ');
  normalized = normalized.replace(/플레이어\s*연결|player\s*연결/g, ' connect_player ');
  normalized = normalized.replace(/math\s*a\s*input|a\s*입력|connect\s*a/g, ' connect_math_a ');
  normalized = normalized.replace(/math\s*b\s*input|b\s*입력|connect\s*b/g, ' connect_math_b ');
  normalized = normalized.replace(/수학\s*입력|수학\s*연결|a\/b\s*입력|math\s*inputs?/g, ' connect_math_a connect_math_b ');
  return normalized;
}

function extractAiActionsFromText(text, allowedActions) {
  const allowed = Array.isArray(allowedActions) ? allowedActions : [];
  if (!allowed.length) return [];
  const out = [];
  const seen = new Set();
  const push = (action) => {
    if (!allowed.includes(action) || seen.has(action)) return;
    seen.add(action);
    out.push(action);
  };

  const parsed = extractJsonObjectFromText(text);
  if (Array.isArray(parsed?.actions)) {
    for (const item of parsed.actions) {
      push(String(item || '').trim());
    }
  }

  const normalizedText = normalizeAiActionAliases(text);
  for (const action of allowed) {
    const pattern = new RegExp(`\\b${action.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&')}\\b`, 'i');
    if (pattern.test(normalizedText)) push(action);
  }
  return out;
}

function extractAiActionPlan(text, allowedActions) {
  const allowed = Array.isArray(allowedActions) ? allowedActions : [];
  const out = [];
  const parsed = extractJsonObjectFromText(text);
  const pushStep = (stepLike) => {
    const type = typeof stepLike === 'string' ? stepLike : stepLike?.type;
    const t = String(type || '').trim();
    if (!allowed.includes(t)) return;
    if (typeof stepLike === 'string') {
      out.push({ type: t, source: '', sourceNodeId: '', sourceNodeIdA: '', sourceNodeIdB: '' });
      return;
    }
    out.push({
      type: t,
      source: String(stepLike?.source || '').trim(),
      sourceNodeId: String(stepLike?.sourceNodeId || '').trim(),
      sourceNodeIdA: String(stepLike?.sourceNodeIdA || '').trim(),
      sourceNodeIdB: String(stepLike?.sourceNodeIdB || '').trim()
    });
  };

  if (Array.isArray(parsed?.actions)) {
    for (const action of parsed.actions) {
      if (typeof action === 'string') pushStep(action);
      if (action && typeof action === 'object') pushStep(action);
    }
  }
  if (out.length > 0) return out;
  return extractAiActionsFromText(text, allowed)
    .map((type) => ({ type, source: '', sourceNodeId: '', sourceNodeIdA: '', sourceNodeIdB: '' }));
}

async function ensureAiModelAssets(options = {}) {
  const silent = !!options.silent;
  const force = !!options.force;
  if (!force && aiModelState.prefetchDone) return true;
  if (aiModelState.prefetchInFlight) return false;
  aiModelState.prefetchInFlight = true;
  aiModelState.error = '';
  beginUnifiedAiLoadProgress();
  updateUnifiedAiLoadProgress('download', 0.01, 'AI 모델 다운로드 준비 중...');
  try {
    const cacheSupported = !!window.caches && typeof window.caches.open === 'function';
    let cache = null;
    if (cacheSupported) {
      cache = await window.caches.open(AI_MODEL_CACHE_NAME);
    } else {
      if (!silent) showActionBar('AI 모델 캐시 미지원 환경: 직접 다운로드 모드', '', 2200);
    }
    let downloadedCount = 0;
    let downloadedBytes = 0;
    for (let i = 0; i < AI_MODEL_ASSET_LIST.length; i += 1) {
      const url = AI_MODEL_ASSET_LIST[i];
      const fileName = fileNameFromUrl(url);
      const cached = cache && !force ? await cache.match(url) : null;
      if (cached) {
        downloadedCount += 1;
        const ratio = downloadedCount / Math.max(1, AI_MODEL_ASSET_LIST.length);
        updateUnifiedAiLoadProgress('download', ratio, `모델 확인 ${downloadedCount}/${AI_MODEL_ASSET_LIST.length}`);
        continue;
      }
      if (!silent) {
        showActionBar(`AI 모델 다운로드 ${i + 1}/${AI_MODEL_ASSET_LIST.length}`, '', 0, { keepProgress: true });
      }
      let lastProgressAt = 0;
      const asset = await downloadAssetWithProgress(url, (loaded, total, ratio) => {
        const now = Date.now();
        if ((now - lastProgressAt) < 80 && ratio < 1) return;
        lastProgressAt = now;
        const boundedRatio = Number.isFinite(ratio) ? Math.max(0, Math.min(1, ratio)) : 0;
        const overallRatio = (downloadedCount + boundedRatio) / Math.max(1, AI_MODEL_ASSET_LIST.length);
        updateUnifiedAiLoadProgress('download', overallRatio, `모델 다운로드 ${i + 1}/${AI_MODEL_ASSET_LIST.length}`);
      });
      const size = Number(asset.totalBytes || 0);
      if (cache) {
        const cachedResponse = new Response(asset.bytes, {
          headers: asset.headers
        });
        await cache.put(url, cachedResponse);
      }
      downloadedBytes += Math.max(0, size);
      downloadedCount += 1;
      const ratio = downloadedCount / Math.max(1, AI_MODEL_ASSET_LIST.length);
      updateUnifiedAiLoadProgress('download', ratio, `모델 다운로드 ${downloadedCount}/${AI_MODEL_ASSET_LIST.length}`);
      aiModelState.downloadedCount = downloadedCount;
      aiModelState.downloadedBytes = downloadedBytes;
    }
    aiModelState.prefetchDone = true;
    aiModelState.downloadedCount = downloadedCount;
    aiModelState.downloadedBytes = downloadedBytes;
    const sizeText = downloadedBytes > 0 ? ` (${formatBytes(downloadedBytes)})` : '';
    updateUnifiedAiLoadProgress('download', 1, '모델 다운로드 완료');
    if (localAiRuntimeState.ready) {
      finalizeUnifiedAiLoadProgress(sizeText);
    } else if (localAiRuntimeState.loading) {
      if (!silent) {
        showActionBar('모델 다운로드 완료 (런타임 로드 진행 중)', '', 0, { keepProgress: true });
      }
    } else if (!silent) {
      showActionBar(`AI 모델 다운로드 완료${sizeText}`, 'ok', 0, { progress: Number.NaN, keepProgress: true });
    } else {
      // Silent prefetch only: clear progress UI instead of leaving "준비 중" state visible.
      hideActionBarProgress();
      resetUnifiedAiLoadProgress();
      if (el.actionBar instanceof HTMLElement) {
        el.actionBar.classList.add('hidden');
      }
    }
    return true;
  } catch (error) {
    aiModelState.error = error?.message || 'unknown error';
    resetUnifiedAiLoadProgress();
    if (!silent) {
      showActionBar(`AI 모델 다운로드 실패: ${aiModelState.error}`, 'error', 3200);
    }
    return false;
  } finally {
    aiModelState.prefetchInFlight = false;
  }
}

function renderActionBarLogs() {
  ensureActionBarStructure();
  if (!el.actionBarLogs) return;
  el.actionBarLogs.innerHTML = '';
  for (const item of actionBarState.logs) {
    const row = document.createElement('div');
    row.className = `action-bar-log-item${item.kind === 'ok' ? ' ok' : item.kind === 'error' ? ' error' : ''}`;
    row.textContent = item.message;
    el.actionBarLogs.appendChild(row);
  }
  el.actionBarLogs.scrollTop = el.actionBarLogs.scrollHeight;
}

function appendActionBarLog(message, kind = '') {
  const text = String(message || '').trim();
  if (!text) return;
  if (el.actionBar) el.actionBar.classList.remove('hidden');
  actionBarState.logs.push({ message: text, kind });
  if (actionBarState.logs.length > 300) {
    actionBarState.logs.splice(0, actionBarState.logs.length - 300);
  }
  renderActionBarLogs();
}

function setActionBarExpanded(expanded) {
  ensureActionBarStructure();
  actionBarState.expanded = false;
  if (el.actionBar) el.actionBar.classList.remove('expanded');
  if (el.actionBarLogs) el.actionBarLogs.classList.add('hidden');
}

function showActionBar(message, kind = '', duration = 2200, options = {}) {
  ensureActionBarStructure();
  if (!el.actionBar) return;
  const shouldLog = options.log === true;
  if (el.actionBarMessage) {
    el.actionBarMessage.textContent = message;
  } else {
    el.actionBar.textContent = message;
  }
  el.actionBar.classList.remove('hidden', 'ok', 'error');
  if (kind === 'ok' || kind === 'error') {
    el.actionBar.classList.add(kind);
  }
  if (shouldLog) {
    const last = actionBarState.logs[actionBarState.logs.length - 1];
    if (!last || last.message !== message || last.kind !== kind) {
      appendActionBarLog(message, kind);
    }
  }
  if (Object.prototype.hasOwnProperty.call(options, 'progress')) {
    const progressValue = Number(options.progress);
    if (Number.isFinite(progressValue)) {
      setActionBarProgress(progressValue, options.progressLabel || '', { reset: !!options.progressReset });
      if (progressValue >= 100) {
        if (actionBarState.progressHideTimer) {
          clearTimeout(actionBarState.progressHideTimer);
          actionBarState.progressHideTimer = null;
        }
        actionBarState.progressHideTimer = setTimeout(() => {
          hideActionBarProgress();
        }, 700);
      }
    } else {
      hideActionBarProgress();
    }
  } else if (duration > 0 && !options.keepProgress) {
    hideActionBarProgress();
  }
  if (actionBarState.timer) {
    clearTimeout(actionBarState.timer);
    actionBarState.timer = null;
  }
  if (duration > 0) {
    actionBarState.timer = setTimeout(() => {
      el.actionBar.classList.add('hidden');
      actionBarState.timer = null;
    }, duration);
  }
}

function withPrivateKey(path) {
  const url = new URL(path, window.location.origin);
  if (PRIVATE_KEY) {
    url.searchParams.set('privatekey', PRIVATE_KEY);
  }
  return `${url.pathname}${url.search}`;
}

function isEventType(type) {
  return type === 'EVENT_ON_ENABLE' || type === 'EVENT_ON_PLAYER_JOIN' || type === 'EVENT_ON_PLAYER_CHAT';
}

function isActionType(type) {
  return type.startsWith('ACTION_') || type.startsWith('FUNCTION_') || type === 'ON_ENABLE_LOG' || type === 'ON_PLAYER_JOIN_MESSAGE' || type === 'ON_PLAYER_JOIN_SET_JOIN_MESSAGE';
}

function isOperationType(type) {
  return type === 'MATH_ADD' || type === 'MATH_SUB' || type === 'MATH_MUL' || type === 'MATH_DIV';
}

function isVariableType(type) {
  return type.startsWith('VAR_');
}

function isExecOutputType(type) {
  return isEventType(type) || isActionType(type);
}

function isExecInputType(type) {
  return isActionType(type);
}

function isDataOutputType(type) {
  return type === 'VAR_TEXT' || type === 'VAR_INTEGER' || type === 'VAR_DECIMAL' || type === 'VAR_BOOLEAN' || type === 'MATH_ADD' || type === 'MATH_SUB' || type === 'MATH_MUL' || type === 'MATH_DIV';
}

function isDataInputType(type) {
  return type === 'FUNCTION_SEND_MESSAGE' || type === 'FUNCTION_BROADCAST_MESSAGE' || type === 'EVENT_ON_PLAYER_JOIN' || type === 'EVENT_ON_PLAYER_CHAT' || type === 'MATH_ADD' || type === 'MATH_SUB' || type === 'MATH_MUL' || type === 'MATH_DIV';
}

function canStartLink(type) {
  return true;
}

function isDataKind(kind) {
  return kind === 'data' || String(kind).startsWith('data-');
}

function linkFamily(kind) {
  const k = String(kind || 'exec');
  if (isDataKind(k)) return 'data';
  if (k === 'exec') return 'exec';
  if (k === 'player') return 'player';
  if (k === 'context') return 'context';
  return k;
}

function linkFamilyByPortClass(portClass, fallbackKind = 'exec') {
  const p = String(portClass || '');
  if (p === 'in-player' || p === 'out-player') return 'player';
  if (p === 'in-exec' || p === 'out-exec') return 'exec';
  if (p === 'in-cancel' || p.startsWith('in-data') || p.startsWith('out-data')) return 'data';
  return linkFamily(fallbackKind);
}

function linkKindFromType(type) {
  if (type === 'VAR_TEXT') return 'data-text';
  if (type === 'VAR_INTEGER') return 'data-int';
  if (type === 'VAR_DECIMAL') return 'data-decimal';
  if (type === 'VAR_BOOLEAN') return 'data-bool';
  if (type === 'MATH_ADD') return 'data-any';
  if (type === 'MATH_MUL') return 'data-number';
  if (type === 'MATH_DIV') return 'data-number';
  if (type === 'MATH_SUB') return 'data-number';
  if (isExecOutputType(type)) return 'exec';
  return 'none';
}

function targetPortClassForLink(kind, toType, preferredPortClass = '') {
  if (preferredPortClass) return preferredPortClass;
  if (isDataKind(kind) && toType === 'MATH_ADD') return 'in-data-a';
  if (isDataKind(kind) && toType === 'MATH_SUB') return 'in-data-a';
  if (isDataKind(kind) && toType === 'MATH_MUL') return 'in-data-a';
  if (isDataKind(kind) && toType === 'MATH_DIV') return 'in-data-a';
  if (isDataKind(kind) && toType === 'FUNCTION_SEND_MESSAGE') return 'in-data';
  if (isDataKind(kind) && toType === 'FUNCTION_BROADCAST_MESSAGE') return 'in-data';
  if (isDataKind(kind) && toType === 'EVENT_ON_PLAYER_JOIN') return 'in-data';
  if (toType === 'EVENT_ON_PLAYER_CHAT' && kind === 'data-bool') return 'in-cancel';
  if (isDataKind(kind) && toType === 'EVENT_ON_PLAYER_CHAT') return 'in-data';
  if (kind === 'player' && toType === 'FUNCTION_SEND_MESSAGE') return 'in-player';
  return 'in-exec';
}

function isNumericDataKind(kind) {
  return kind === 'data-int' || kind === 'data-decimal' || kind === 'data-number';
}

function canLinkKindToType(kind, toType) {
  if (kind === 'data-text' || kind === 'data-int' || kind === 'data-decimal' || kind === 'data-bool' || kind === 'data-any' || kind === 'data') {
    if (toType === 'MATH_SUB' || toType === 'MATH_MUL' || toType === 'MATH_DIV') return isNumericDataKind(kind);
    return isDataInputType(toType);
  }
  if (kind === 'data-number') {
    if (toType === 'MATH_SUB' || toType === 'MATH_MUL' || toType === 'MATH_DIV') return true;
    return isDataInputType(toType);
  }
  if (kind === 'exec') return isExecInputType(toType);
  if (kind === 'player') return toType === 'FUNCTION_SEND_MESSAGE';
  if (kind === 'context') return false;
  return false;
}

function canLinkKindToPort(kind, toType, toPortClass = '', sourceId = '') {
  const isNumericMathTarget = toType === 'MATH_SUB' || toType === 'MATH_MUL' || toType === 'MATH_DIV';
  const canTypeLink = (kind === 'data-any' && isNumericMathTarget)
    ? canNodeOutputBeNumeric(sourceId)
    : canLinkKindToType(kind, toType);
  if (!canTypeLink) return false;
  const portClass = String(toPortClass || '');
  if (portClass === 'in-exec') return kind === 'exec';
  if (portClass === 'in-player') return kind === 'player';
  if (portClass === 'in-data' || portClass === 'in-data-a' || portClass === 'in-data-b') return isDataKind(kind);
  if (toType === 'EVENT_ON_PLAYER_CHAT' && toPortClass === 'in-cancel') {
    return kind === 'data-bool';
  }
  return true;
}

function canNodeOutputBeNumeric(nodeId, depth = 0, seen = new Set()) {
  if (!nodeId || depth > 24 || seen.has(nodeId)) return false;
  seen.add(nodeId);
  const node = state.blocks.find((it) => it.id === nodeId);
  if (!node) return false;
  if (node.type === 'VAR_INTEGER' || node.type === 'VAR_DECIMAL' || node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV') return true;
  if (node.type !== 'MATH_ADD') return false;

  const inputA = state.links.find((it) => it.to === nodeId && (it.toPortClass || '') === 'in-data-a' && isDataKind(it.kind || 'exec'));
  const inputB = state.links.find((it) => it.to === nodeId && (it.toPortClass || '') === 'in-data-b' && isDataKind(it.kind || 'exec'));
  if (!inputA || !inputB) return false;

  const kindA = inputA.kind || 'exec';
  const kindB = inputB.kind || 'exec';
  const numericA = isNumericDataKind(kindA) || (kindA === 'data-any' && canNodeOutputBeNumeric(inputA.from, depth + 1, seen));
  const numericB = isNumericDataKind(kindB) || (kindB === 'data-any' && canNodeOutputBeNumeric(inputB.from, depth + 1, seen));
  return numericA && numericB;
}

function canLinkFromSourceToTarget(sourceId, sourceKind, targetType) {
  if (sourceKind === 'data-any' && (targetType === 'MATH_SUB' || targetType === 'MATH_MUL' || targetType === 'MATH_DIV')) {
    return canNodeOutputBeNumeric(sourceId);
  }
  return canLinkKindToType(sourceKind, targetType);
}

function isRuntimeLinkCompatible(fromNode, toNode, kind, toPortClass) {
  return !runtimeLinkIncompatibilityReason(fromNode, toNode, kind, toPortClass);
}

function runtimeLinkIncompatibilityReason(fromNode, toNode, kind, toPortClass) {
  if (!fromNode || !toNode) return '존재하지 않는 노드와 연결됨';
  if (!canLinkKindToPort(kind, toNode.type, toPortClass, fromNode.id)) return '포트 타입이 호환되지 않음';

  // Generated code consumes player input only for sendMessage from join/chat event player output.
  if (kind === 'player') {
    const sourceType = String(fromNode.type || '');
    const ok = toNode.type === 'FUNCTION_SEND_MESSAGE'
      && toPortClass === 'in-player'
      && (sourceType === 'EVENT_ON_PLAYER_JOIN' || sourceType === 'EVENT_ON_PLAYER_CHAT');
    return ok ? '' : '이 player 연결은 실행 코드에서 사용되지 않음';
  }

  return '';
}

function buildLinkErrorDiagnostics() {
  const invalidIndexes = new Set();
  const reasonByIndex = new Map();
  const causeNodeIdByIndex = new Map();
  const familyByIndex = new Map();
  const outgoingByNodeId = new Map();

  for (let i = 0; i < state.links.length; i += 1) {
    const link = state.links[i];
    const fromNode = state.blocks.find((it) => it.id === link.from);
    const toNode = state.blocks.find((it) => it.id === link.to);
    if (!fromNode || !toNode) continue;
    if (!outgoingByNodeId.has(link.from)) outgoingByNodeId.set(link.from, []);
    outgoingByNodeId.get(link.from).push(i);

    const kind = link.kind || linkKindFromType(fromNode.type);
    const toPort = targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
    familyByIndex.set(i, linkFamilyByPortClass(toPort, kind));

    const directReason = runtimeLinkIncompatibilityReason(fromNode, toNode, kind, toPort);
    if (directReason) {
      invalidIndexes.add(i);
      reasonByIndex.set(i, directReason);
      causeNodeIdByIndex.set(i, fromNode.id);
    }
  }

  const queue = Array.from(invalidIndexes).map((idx) => ({
    linkIndex: idx,
    family: familyByIndex.get(idx) || 'exec',
    rootCauseNodeId: causeNodeIdByIndex.get(idx) || null
  }));
  while (queue.length) {
    const entry = queue.shift();
    const brokenLink = state.links[entry.linkIndex];
    if (!brokenLink) continue;
    const brokenTargetNode = state.blocks.find((it) => it.id === brokenLink.to);
    const nextIndexes = outgoingByNodeId.get(brokenLink.to) || [];
    for (const nextIndex of nextIndexes) {
      const nextFamily = familyByIndex.get(nextIndex) || 'exec';
      if (!canPropagateErrorAcrossFamily(entry.family, nextFamily, brokenTargetNode?.type || '')) continue;
      if (invalidIndexes.has(nextIndex)) continue;
      invalidIndexes.add(nextIndex);
      reasonByIndex.set(nextIndex, `${titleOf(brokenTargetNode?.type || '')} 노드 입력 오류가 연쇄 전파됨`);
      causeNodeIdByIndex.set(nextIndex, entry.rootCauseNodeId || brokenTargetNode?.id || null);
      queue.push({
        linkIndex: nextIndex,
        family: nextFamily,
        rootCauseNodeId: entry.rootCauseNodeId || brokenTargetNode?.id || null
      });
    }
  }

  return { invalidIndexes, reasonByIndex, causeNodeIdByIndex };
}

function canPropagateErrorAcrossFamily(sourceFamily, nextFamily, targetNodeType) {
  // Same-family propagation is the default.
  if (sourceFamily === nextFamily) {
    // EVENT_ON_PLAYER_JOIN의 입력(message) 오류는 message 출력(data)까지 오염시키지 않고
    // 실행(exec) 흐름에만 영향을 주도록 순서 기반으로 제한한다.
    if (sourceFamily === 'data' && targetNodeType === 'EVENT_ON_PLAYER_JOIN') return false;
    return true;
  }

  // Node-level execution dependencies: data/player 쪽 오류가 실행 단계로 번질 수 있는 노드들.
  const bridgeToExec = nextFamily === 'exec' && sourceFamily !== 'exec';
  if (bridgeToExec) {
    return isActionType(targetNodeType) || targetNodeType === 'EVENT_ON_PLAYER_JOIN';
  }
  return false;
}

function hasIncomingToResolvedPort(nodeId, portClass) {
  for (const link of state.links) {
    if (String(link?.to || '') !== nodeId) continue;
    const fromNode = state.blocks.find((n) => n.id === link.from);
    const toNode = state.blocks.find((n) => n.id === link.to);
    if (!fromNode || !toNode) continue;
    const kind = link.kind || linkKindFromType(fromNode.type);
    const resolvedPort = targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
    if (resolvedPort === portClass) return true;
  }
  return false;
}

function missingRequiredInputLabels(node) {
  if (!node) return [];
  if (node.type === 'FUNCTION_SEND_MESSAGE' || node.type === 'FUNCTION_BROADCAST_MESSAGE') {
    const missing = [];
    if (!hasIncomingToResolvedPort(node.id, 'in-data')) missing.push('message');
    if (node.type === 'FUNCTION_SEND_MESSAGE' && !hasIncomingToResolvedPort(node.id, 'in-player')) missing.push('player');
    return missing;
  }
  if (node.type === 'MATH_ADD' || node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV') {
    const missing = [];
    if (!hasIncomingToResolvedPort(node.id, 'in-data-a')) missing.push('A');
    if (!hasIncomingToResolvedPort(node.id, 'in-data-b')) missing.push('B');
    return missing;
  }
  return [];
}

function buildNodeAiRiskMap() {
  const map = new Map();
  const linkDiagnostics = buildLinkErrorDiagnostics();
  const globalFailProb = clamp01(Number(graphDecisionState?.ml?.failProbability ?? 0));
  const globalFactor = 0.6 + (0.8 * globalFailProb);

  const hasInvalidLinkByNode = new Map();
  const hasPropagatedLinkByNode = new Map();
  for (let index = 0; index < state.links.length; index += 1) {
    if (!linkDiagnostics.invalidIndexes.has(index)) continue;
    const link = state.links[index];
    const reason = String(linkDiagnostics.reasonByIndex.get(index) || '');
    const propagated = reason.includes('연쇄 전파');
    const fromId = String(link?.from || '');
    const toId = String(link?.to || '');
    if (fromId) {
      hasInvalidLinkByNode.set(fromId, true);
      if (propagated) hasPropagatedLinkByNode.set(fromId, true);
    }
    if (toId) {
      hasInvalidLinkByNode.set(toId, true);
      if (propagated) hasPropagatedLinkByNode.set(toId, true);
    }
  }

  for (const node of state.blocks) {
    const nodeId = String(node?.id || '');
    if (!nodeId) continue;
    const incomingExec = state.links.some((it) => it.to === nodeId && ((it.kind || 'exec') === 'exec'));
    const linkedAny = state.links.some((it) => it.from === nodeId || it.to === nodeId);
    const missingRequired = missingRequiredInputLabels(node);
    let score = 0;
    if (hasInvalidLinkByNode.get(nodeId)) score += 0.75;
    if (hasPropagatedLinkByNode.get(nodeId)) score += 0.25;
    if (isActionType(node.type) && !incomingExec) score += 0.95;
    if (!linkedAny) score += 0.18;
    if (missingRequired.length > 0) score += 0.95;
    score = clamp01(score * globalFactor);
    if (score < 0.55) continue;
    const level = score >= 0.75 ? 'high' : 'warn';
    const label = level === 'high' ? '높음' : '보통';
    const reason = missingRequired.length > 0
      ? `필요한 입력이 없음: ${missingRequired.join(', ')}`
      : (hasInvalidLinkByNode.get(nodeId)
        ? '연결 오류 또는 연쇄 영향이 감지됨'
        : (isActionType(node.type) && !incomingExec ? '실행 연결 없음' : '구조가 불안정함'));
    map.set(nodeId, { score, level, label, reason });
  }

  return map;
}

function nodeDistanceSq(a, b) {
  const dx = (a?.x || 0) - (b?.x || 0);
  const dy = (a?.y || 0) - (b?.y || 0);
  return (dx * dx) + (dy * dy);
}

async function applyAiAutoFixForNode(nodeId) {
  const node = state.blocks.find((it) => it.id === nodeId);
  if (!node) {
    showActionBar('AI 자동 수정 실패: 대상 노드를 찾지 못했습니다.', 'error', 2600);
    return 0;
  }
  let applied = 0;

  const sourcePortClassFromKind = (kind) => {
    if (kind === 'exec') return 'out-exec';
    if (kind === 'player') return 'out-player';
    if (kind === 'data-text') return 'out-data-text';
    if (kind === 'data-int') return 'out-data-int';
    if (kind === 'data-decimal') return 'out-data-decimal';
    if (kind === 'data-bool') return 'out-data-bool';
    if (kind === 'data-any') return 'out-data-any';
    if (kind === 'data-number') return 'out-data-number';
    return '';
  };

  const tryConnectFromChosenNode = (sourceNodeId, targetPortClass, expectedFamily = '') => {
    const sourceId = String(sourceNodeId || '').trim();
    const targetPort = String(targetPortClass || '').trim();
    if (!sourceId || !targetPort) return false;
    const source = state.blocks.find((it) => it.id === sourceId);
    if (!source || source.id === node.id) return false;
    const kind = linkKindFromType(source.type);
    if (expectedFamily === 'exec' && kind !== 'exec') return false;
    if (expectedFamily === 'player' && kind !== 'player') return false;
    if (expectedFamily === 'data' && !isDataKind(kind)) return false;
    const fromPortClass = sourcePortClassFromKind(kind);
    if (!fromPortClass) return false;
    if (!canLinkKindToPort(kind, node.type, targetPort, source.id)) return false;
    const before = state.links.length;
    addLink(source.id, node.id, kind, fromPortClass, targetPort);
    return state.links.length > before;
  };

  const ensureExecConnection = (sourceNodeId = '') => {
    const hasExec = state.links.some((it) => it.to === node.id && ((it.kind || 'exec') === 'exec'));
    if (hasExec || !isActionType(node.type)) return false;
    if (tryConnectFromChosenNode(sourceNodeId, 'in-exec', 'exec')) {
      applied += 1;
      return true;
    }
    return false;
  };

  const ensureMessageInput = (sourceNodeId = '') => {
    if (!(node.type === 'FUNCTION_SEND_MESSAGE' || node.type === 'FUNCTION_BROADCAST_MESSAGE')) return;
    if (hasIncomingToResolvedPort(node.id, 'in-data')) return;
    if (tryConnectFromChosenNode(sourceNodeId, 'in-data', 'data')) {
      applied += 1;
      return true;
    }
    return false;
  };

  const ensurePlayerInput = (sourceNodeId = '') => {
    if (node.type !== 'FUNCTION_SEND_MESSAGE') return;
    if (hasIncomingToResolvedPort(node.id, 'in-player')) return;
    if (tryConnectFromChosenNode(sourceNodeId, 'in-player', 'player')) {
      applied += 1;
      return true;
    }
    return false;
  };

  const ensureMathInput = (sourceNodeId = '', targetPortClass = '') => {
    if (!(node.type === 'MATH_ADD' || node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV')) return;
    const port = String(targetPortClass || '');
    if (port !== 'in-data-a' && port !== 'in-data-b') return false;
    if (hasIncomingToResolvedPort(node.id, port)) return false;
    if (tryConnectFromChosenNode(sourceNodeId, port, 'data')) {
      applied += 1;
      return true;
    }
    return false;
  };

  const allActions = ['connect_exec', 'connect_message', 'connect_player', 'connect_math_a', 'connect_math_b'];
  const missing = missingRequiredInputLabels(node);
  showActionBar('AI 자동 수정: 그래프 분석 중...', '', 0, { keepProgress: true });
  await waitForNextUiTick();
  const existingContext = buildExistingAutoFixContext(node.id, 20, 36);
  const availableSourceNodeIds = (Array.isArray(existingContext?.nodes) ? existingContext.nodes : [])
    .map((n) => String(n?.id || '').trim())
    .filter(Boolean)
    .slice(0, 32);

  const runAction = (step) => {
    const action = String(step?.type || step || '');
    const commonSourceNodeId = String(
      step?.sourceNodeId
      || step?.sourceNode
      || step?.sourceId
      || step?.fromNodeId
      || ''
    );
    const sourceNodeIdA = String(step?.sourceNodeIdA || step?.sourceA || commonSourceNodeId || '');
    const sourceNodeIdB = String(step?.sourceNodeIdB || step?.sourceB || commonSourceNodeId || '');
    if (action === 'connect_exec') {
      const ok = ensureExecConnection(commonSourceNodeId);
      return { ok, reason: ok ? '' : `connect_exec 실패(sourceNodeId=${commonSourceNodeId || 'empty'})` };
    }
    if (action === 'connect_message') {
      const ok = ensureMessageInput(commonSourceNodeId);
      return { ok, reason: ok ? '' : `connect_message 실패(sourceNodeId=${commonSourceNodeId || 'empty'})` };
    }
    if (action === 'connect_player') {
      const ok = ensurePlayerInput(commonSourceNodeId);
      return { ok, reason: ok ? '' : `connect_player 실패(sourceNodeId=${commonSourceNodeId || 'empty'})` };
    }
    if (action === 'connect_math_a') {
      const ok = ensureMathInput(sourceNodeIdA, 'in-data-a');
      return { ok, reason: ok ? '' : `connect_math_a 실패(sourceNodeId=${sourceNodeIdA || 'empty'})` };
    }
    if (action === 'connect_math_b') {
      const ok = ensureMathInput(sourceNodeIdB, 'in-data-b');
      return { ok, reason: ok ? '' : `connect_math_b 실패(sourceNodeId=${sourceNodeIdB || 'empty'})` };
    }
    return { ok: false, reason: `unknown action: ${action}` };
  };

  let plannedActions = [];
  try {
    showActionBar('AI 자동 수정: 모델 추론 중...', '', 0, { keepProgress: true });
    await waitForNextUiTick();
    const prompt = [
      'You are an auto-fix planner for a visual block editor.',
      'Return JSON only.',
      `Allowed actions: ${allActions.join(', ')}`,
      'You must pick sourceNodeId from Existing graph context JSON. Do not infer nearest nodes.',
      'Every connect_* action must include sourceNodeId.',
      'Input port order rule: ports.inputs order means visual top-to-bottom order.',
      `Target node type: ${node.type}`,
      `Target input ports (top->bottom): ${inputPortOrderText(node.type)}`,
      `Missing required inputs: ${missing.join(', ') || 'none'}`,
      `Has incoming exec link: ${state.links.some((it) => it.to === node.id && ((it.kind || 'exec') === 'exec')) ? 'yes' : 'no'}`,
      `Valid sourceNodeId candidates: ${availableSourceNodeIds.join(', ') || 'none'}`,
      `Existing graph context JSON: ${JSON.stringify(existingContext)}`,
      'Example: {"actions":[{"type":"connect_exec","sourceNodeId":"n12"},{"type":"connect_message","sourceNodeId":"n31"}]}',
      'No prose, no markdown, JSON only.'
    ].join('\n');
    const generated = await generateWithLocalModel(prompt, 96);
    showActionBar('AI 자동 수정: 출력 해석 중...', '', 0, { keepProgress: true });
    await waitForNextUiTick();
    plannedActions = extractAiActionPlan(generated, allActions);
  } catch (error) {
    showActionBar(`AI 자동 수정 실패: ${error?.message || '모델 출력 실패'}`, 'error', 3000);
    return 0;
  }

  if (plannedActions.length === 0) {
    showActionBar('AI 자동 수정 실패: 유효한 액션을 생성하지 못했습니다.', 'error', 3000);
    return 0;
  }

  showActionBar('AI 자동 수정: 변경 적용 중...', '', 0, { keepProgress: true });
  await waitForNextUiTick();
  let appliedByAction = 0;
  const failedReasons = [];
  for (const step of plannedActions) {
    const result = runAction(step);
    if (result?.ok) appliedByAction += 1;
    else if (result?.reason) failedReasons.push(result.reason);
  }

  if (applied > 0) {
    scheduleCollaborativePublish(true);
    schedulePresencePublish(true);
    showActionBar(`AI 자동 수정 적용: ${applied}개`, 'ok', 1800);
  } else {
    const reasonText = failedReasons.slice(0, 2).join(' / ');
    showActionBar(`AI 자동 수정 실패: 적용 가능한 수정이 없습니다. (액션 ${plannedActions.length}개, 적용 ${appliedByAction}개${reasonText ? `, 원인: ${reasonText}` : ''})`, 'error', 3800);
  }
  return applied;
}

function getLinkIndexesForPort(nodeId, portType, portClass) {
  if (!nodeId || !portType || !portClass) return [];
  const indexes = [];
  for (let index = 0; index < state.links.length; index += 1) {
    const link = state.links[index];
    if (portType === 'out') {
      const resolvedFromPort = link.fromPortClass || '';
      if (link.from === nodeId && resolvedFromPort === portClass) indexes.push(index);
      continue;
    }
    if (link.to !== nodeId) continue;
    const fromNode = state.blocks.find((n) => n.id === link.from);
    const toNode = state.blocks.find((n) => n.id === link.to);
    if (!fromNode || !toNode) continue;
    const kind = link.kind || linkKindFromType(fromNode.type);
    const toPort = targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
    if (toPort === portClass) indexes.push(index);
  }
  return indexes;
}

function moveViewportToNode(nodeId) {
  const node = state.blocks.find((it) => it.id === nodeId);
  if (!node) return false;
  const nodeHeight = measureNodeHeightFromDom(nodeId) || estimateNodeHeight(node);
  const worldX = node.x + (BLOCK_NODE_WIDTH / 2);
  const worldY = node.y + (nodeHeight / 2);
  state.panX = (el.workspace.clientWidth / 2) - (worldX * state.scale);
  state.panY = (el.workspace.clientHeight / 2) - (worldY * state.scale);
  state.selectedId = nodeId;
  state.selectedCommentId = null;
  renderCanvas();
  schedulePresencePublish(true);
  return true;
}

function canLinkTypes(fromType, toType) {
  return canLinkKindToType(linkKindFromType(fromType), toType);
}

function supportsMultiOutgoing(type) {
  return true;
}

function titleOf(type) {
  return typeTitleMap.get(type) || type;
}

function typeLabelOf(type) {
  if (type === 'EVENT_ON_ENABLE') return '플러그인이 활성화될 때';
  if (type === 'EVENT_ON_PLAYER_JOIN') return '플레이어가 접속할 때';
  if (type === 'EVENT_ON_PLAYER_CHAT') return '플레이어가 채팅할 때';
  if (type === 'VAR_TEXT') return '텍스트 값을 저장해 두는 칸';
  if (type === 'VAR_INTEGER') return '소수점 없는 숫자를 저장해 두는 칸';
  if (type === 'VAR_DECIMAL') return '소수점 있는 숫자를 저장해 두는 칸';
  if (type === 'VAR_BOOLEAN') return '참/거짓 값을 저장해 두는 칸';
  if (type === 'FUNCTION_SEND_MESSAGE') return '플레이어에게 메시지를 보내는 함수';
  if (type === 'FUNCTION_BROADCAST_MESSAGE') return '모든 플레이어에게 메시지를 보내는 함수';
  if (type === 'MATH_ADD') return '두 값을 더하거나 이어 붙이는 연산';
  if (type === 'MATH_SUB') return '두 숫자를 빼는 연산';
  if (type === 'MATH_MUL') return '두 숫자를 곱하는 연산';
  if (type === 'MATH_DIV') return '두 숫자를 나누는 연산';
  return type;
}

function buildDefaultNodeParams(type) {
  const functionDef = NODE_LIBRARY.functions.find((it) => it.type === type);
  const variableDef = NODE_LIBRARY.variables.find((it) => it.type === type);
  const params = {};
  if (functionDef) params.message = functionDef.defaultMessage;
  if (type === 'EVENT_ON_PLAYER_JOIN' || type === 'EVENT_ON_PLAYER_CHAT') params.message = '';
  if (variableDef) {
    params.value = variableDef.defaultValue;
  }
  return params;
}

function createNode(type, x, y, paramsOverride = null) {
  const params = buildDefaultNodeParams(type);
  if (paramsOverride && typeof paramsOverride === 'object') {
    Object.assign(params, paramsOverride);
  }
  const node = {
    id: `n${state.nextId++}`,
    type,
    x: Math.round(x),
    y: Math.round(y),
    params
  };
  state.blocks.push(node);
  state.selectedId = node.id;
  commitHistoryState();
  renderCanvas();
  return node;
}

function createComment(x, y, width, height) {
  const comment = {
    id: `c${state.nextCommentId++}`,
    x: Math.round(x),
    y: Math.round(y),
    width: Math.max(180, Math.round(width)),
    height: Math.max(120, Math.round(height)),
    title: '그룹 제목',
    description: '설명'
  };
  state.comments.push(comment);
  state.selectedCommentId = comment.id;
  state.selectedId = null;
  commitHistoryState();
  return comment;
}

function removeComment(commentId) {
  const prevSize = state.comments.length;
  state.comments = state.comments.filter((it) => it.id !== commentId);
  if (state.selectedCommentId === commentId) state.selectedCommentId = null;
  if (state.comments.length !== prevSize) {
    commitHistoryState();
  }
}

function snapGrid(value) {
  return Math.round(value / GRID_SIZE) * GRID_SIZE;
}

function linkPath(from, to) {
  const dx = Math.max(60, Math.abs(to.x - from.x) * 0.5);
  const c1x = from.x + dx;
  const c2x = to.x - dx;
  return `M ${from.x} ${from.y} C ${c1x} ${from.y}, ${c2x} ${to.y}, ${to.x} ${to.y}`;
}

function colorForClient(clientId) {
  let hash = 0;
  for (let i = 0; i < clientId.length; i += 1) {
    hash = ((hash << 5) - hash) + clientId.charCodeAt(i);
    hash |= 0;
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue} 72% 62%)`;
}

function buildGraphDecisionSnapshot() {
  return {
    workspaceId: state.currentWorkspaceId || '',
    nodeCount: state.blocks.length,
    linkCount: state.links.length,
    blocks: state.blocks.map((node) => ({
      id: node.id,
      type: node.type,
      params: node.params || {}
    })),
    links: state.links.map((link) => ({
      from: link.from,
      to: link.to,
      kind: link.kind || 'exec',
      fromPortClass: link.fromPortClass || '',
      toPortClass: link.toPortClass || ''
    }))
  };
}

function evaluateGraphRules() {
  const issues = [];
  const warnings = [];
  const blockById = new Map();
  const duplicateIds = new Set();
  for (const block of state.blocks) {
    if (!block?.id) continue;
    if (blockById.has(block.id)) {
      duplicateIds.add(block.id);
      continue;
    }
    blockById.set(block.id, block);
  }
  if (duplicateIds.size > 0) {
    issues.push({
      code: 'duplicate_node_id',
      severity: 'error',
      message: `중복 노드 ID: ${Array.from(duplicateIds).join(', ')}`
    });
  }

  const incomingExecCount = new Map();
  for (const link of state.links) {
    const fromNode = blockById.get(link.from);
    const toNode = blockById.get(link.to);
    if (!fromNode || !toNode) {
      issues.push({
        code: 'dangling_link',
        severity: 'error',
        message: `존재하지 않는 노드로 연결됨 (${link.from} -> ${link.to})`
      });
      continue;
    }
    const kind = link.kind || linkKindFromType(fromNode.type);
    const toPortClass = resolveLinkToPortClass(link, toNode);
    const runtimeIssue = runtimeLinkIncompatibilityReason(fromNode, toNode, kind, toPortClass);
    if (runtimeIssue) {
      issues.push({
        code: 'invalid_link_type',
        severity: 'error',
        message: `실행 불가 연결 (${titleOf(fromNode.type)} -> ${titleOf(toNode.type)}): ${runtimeIssue}`
      });
      continue;
    }
    if (kind === 'exec') {
      incomingExecCount.set(link.to, (incomingExecCount.get(link.to) || 0) + 1);
    }
  }

  const hasEventNode = state.blocks.some((node) => isEventType(node.type));
  if (!hasEventNode && state.blocks.length > 0) {
    warnings.push({
      code: 'missing_event_root',
      severity: 'warn',
      message: '이벤트 시작 노드가 없어 실행 체인이 시작되지 않을 수 있습니다.'
    });
  }

  for (const node of state.blocks) {
    if (isActionType(node.type) && !incomingExecCount.get(node.id)) {
      warnings.push({
        code: 'unreachable_action',
        severity: 'warn',
        message: `${titleOf(node.type)} 노드가 실행 체인에 연결되지 않았습니다.`
      });
    }
  }

  return {
    version: 'rule-v1',
    canRun: issues.length === 0,
    issues,
    warnings,
    checkedAt: Date.now()
  };
}

function clamp01(v) {
  if (!Number.isFinite(v)) return 0;
  return Math.max(0, Math.min(1, v));
}

function sigmoid(x) {
  const t = Number.isFinite(x) ? x : 0;
  return 1 / (1 + Math.exp(-t));
}

function buildMlGraphFeatures(snapshot, ruleResult) {
  const blocks = Array.isArray(snapshot?.blocks) ? snapshot.blocks : [];
  const links = Array.isArray(snapshot?.links) ? snapshot.links : [];
  const nodeCount = Math.max(1, Number(snapshot?.nodeCount || blocks.length || 0));
  const linkCount = Math.max(0, Number(snapshot?.linkCount || links.length || 0));
  const issues = Array.isArray(ruleResult?.issues) ? ruleResult.issues : [];
  const warnings = Array.isArray(ruleResult?.warnings) ? ruleResult.warnings : [];

  const eventIds = new Set();
  const actionIds = new Set();
  for (const node of blocks) {
    const t = String(node?.type || '');
    if (isEventType(t)) eventIds.add(String(node.id || ''));
    if (isActionType(t)) actionIds.add(String(node.id || ''));
  }

  const outgoingExec = new Map();
  const incomingExec = new Map();
  for (const link of links) {
    const from = String(link?.from || '');
    const to = String(link?.to || '');
    const kind = String(link?.kind || 'exec');
    if (!from || !to || kind !== 'exec') continue;
    if (!outgoingExec.has(from)) outgoingExec.set(from, []);
    outgoingExec.get(from).push(to);
    incomingExec.set(to, (incomingExec.get(to) || 0) + 1);
  }

  const reached = new Set();
  const queue = Array.from(eventIds);
  while (queue.length) {
    const nodeId = queue.shift();
    if (!nodeId || reached.has(nodeId)) continue;
    reached.add(nodeId);
    const next = outgoingExec.get(nodeId) || [];
    for (const nextId of next) {
      if (!reached.has(nextId)) queue.push(nextId);
    }
  }

  let reachedAction = 0;
  for (const actionId of actionIds) {
    if (reached.has(actionId)) reachedAction += 1;
  }

  const invalidLinkIssues = issues.filter((it) => String(it?.code || '') === 'invalid_link_type').length;
  const danglingIssues = issues.filter((it) => String(it?.code || '') === 'dangling_link').length;
  const unreachableWarn = warnings.filter((it) => String(it?.code || '') === 'unreachable_action').length;
  const missingRootWarn = warnings.filter((it) => String(it?.code || '') === 'missing_event_root').length;
  const orphanAction = Math.max(0, actionIds.size - reachedAction);

  return {
    issueRatio: clamp01(issues.length / Math.max(1, nodeCount)),
    warningRatio: clamp01(warnings.length / Math.max(1, nodeCount)),
    invalidLinkRatio: clamp01(invalidLinkIssues / Math.max(1, linkCount)),
    danglingRatio: clamp01(danglingIssues / Math.max(1, linkCount)),
    sparseRatio: clamp01((nodeCount - linkCount) / Math.max(1, nodeCount)),
    orphanActionRatio: clamp01(orphanAction / Math.max(1, actionIds.size || 1)),
    unreachableWarnRatio: clamp01(unreachableWarn / Math.max(1, actionIds.size || 1)),
    missingRoot: missingRootWarn > 0 ? 1 : 0,
    reachability: clamp01(reachedAction / Math.max(1, actionIds.size || 1)),
    hasRuleError: issues.length > 0 ? 1 : 0,
    nodeScale: clamp01(nodeCount / 40),
    linkDensity: clamp01(linkCount / Math.max(1, nodeCount * 2))
  };
}

async function runMlAssistHeuristic(snapshot, ruleResult) {
  const f = buildMlGraphFeatures(snapshot, ruleResult);
  // Tiny local classifier (logistic inference) tuned for conservative failure prediction.
  const z = (
    -0.35
    + (2.35 * f.issueRatio)
    + (1.8 * f.invalidLinkRatio)
    + (1.2 * f.danglingRatio)
    + (0.95 * f.warningRatio)
    + (0.75 * f.orphanActionRatio)
    + (0.65 * f.unreachableWarnRatio)
    + (0.6 * f.sparseRatio)
    + (0.45 * f.missingRoot)
    - (1.25 * f.reachability)
    - (0.35 * f.linkDensity)
    + (0.2 * f.nodeScale)
  );
  const failProb = clamp01(sigmoid(z));
  const score = Math.round((1 - failProb) * 100);
  const confidenceBand = Math.abs(0.5 - failProb);
  const confidence = confidenceBand >= 0.3 ? 'high' : (confidenceBand >= 0.16 ? 'medium' : 'low');
  const reasons = [];
  if (f.invalidLinkRatio > 0.08) reasons.push('타입 불일치 링크 비율이 높음');
  if (f.orphanActionRatio > 0.3) reasons.push('실행 체인에 연결되지 않은 액션이 많음');
  if (f.missingRoot > 0) reasons.push('이벤트 시작 노드가 부족함');
  if (f.reachability < 0.45) reasons.push('이벤트 기준 도달 가능한 실행 경로가 적음');

  let localModel = null;
  try {
    localModel = await runLocalModelRiskAssist(snapshot, ruleResult);
  } catch (error) {
  }
  const mergedFail = localModel
    ? clamp01((failProb * 0.58) + (localModel.failProbability * 0.42))
    : failProb;
  const mergedScore = Math.round((1 - mergedFail) * 100);
  const mergedBand = Math.abs(0.5 - mergedFail);
  const mergedConfidence = mergedBand >= 0.3 ? 'high' : (mergedBand >= 0.16 ? 'medium' : 'low');
  if (localModel?.reason) reasons.push(localModel.reason);

  return {
    version: 'ml-local-v2',
    source: localModel ? 'local-logistic+onnx-embed' : 'local-logistic',
    confidence: mergedConfidence,
    score: mergedScore,
    failProbability: Number(mergedFail.toFixed(4)),
    reasons,
    features: f,
    localModel,
    checkedAt: Date.now()
  };
}

function publishGraphDecision() {
  const payload = {
    workspaceId: state.currentWorkspaceId || '',
    rule: graphDecisionState.rule,
    ml: graphDecisionState.ml,
    updatedAt: Date.now()
  };
  graphDecisionState.lastPublished = payload;
  window.__AEROGEL_GRAPH_DECISION__ = payload;
  window.dispatchEvent(new CustomEvent('aerogel:graph-decision', { detail: payload }));
}

async function runGraphDecisionNow() {
  const snapshot = buildGraphDecisionSnapshot();
  const fingerprint = JSON.stringify(snapshot);
  graphDecisionState.lastFingerprint = fingerprint;
  graphDecisionState.rule = evaluateGraphRules();
  publishGraphDecision();

  if (graphDecisionState.mlInFlight) {
    graphDecisionState.mlQueued = true;
    return;
  }
  graphDecisionState.mlInFlight = true;
  try {
    const mlResult = await runMlAssistHeuristic(snapshot, graphDecisionState.rule);
    if (graphDecisionState.lastFingerprint !== fingerprint) return;
    graphDecisionState.ml = mlResult;
    publishGraphDecision();
  } finally {
    graphDecisionState.mlInFlight = false;
    if (graphDecisionState.mlQueued) {
      graphDecisionState.mlQueued = false;
      runGraphDecisionNow();
    }
  }
}

function scheduleGraphDecision(immediate = false) {
  if (immediate) {
    if (graphDecisionState.timer) {
      clearTimeout(graphDecisionState.timer);
      graphDecisionState.timer = null;
    }
    runGraphDecisionNow();
    return;
  }
  if (graphDecisionState.timer) return;
  graphDecisionState.timer = setTimeout(() => {
    graphDecisionState.timer = null;
    runGraphDecisionNow();
  }, 40);
}

function removeNode(nodeId) {
  state.blocks = state.blocks.filter((it) => it.id !== nodeId);
  state.links = state.links.filter((it) => it.from !== nodeId && it.to !== nodeId);
  if (state.selectedId === nodeId) state.selectedId = null;
  commitHistoryState();
}

function resolveLinkToPortClass(link, toNode) {
  if (!toNode) return link.toPortClass || '';
  const kind = link.kind || linkKindFromType(state.blocks.find((it) => it.id === link.from)?.type || '');
  return targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
}

function disconnectLinksByPort(nodeId, portType, portClass) {
  if (!nodeId || !portType || !portClass) return;
  if (portType === 'out') {
    state.links = state.links.filter((it) => !(it.from === nodeId && (it.fromPortClass || 'out') === portClass));
    commitHistoryState();
    return;
  }
  state.links = state.links.filter((it) => {
    if (it.to !== nodeId) return true;
    const toNode = state.blocks.find((n) => n.id === it.to);
    return resolveLinkToPortClass(it, toNode) !== portClass;
  });
  commitHistoryState();
}

function disconnectAllLinks(nodeId) {
  if (!nodeId) return;
  state.links = state.links.filter((it) => it.from !== nodeId && it.to !== nodeId);
  commitHistoryState();
}

function duplicateNode(nodeId) {
  const source = state.blocks.find((it) => it.id === nodeId);
  if (!source) return null;
  const duplicated = {
    id: `n${state.nextId++}`,
    type: source.type,
    x: Math.round(source.x + 24),
    y: Math.round(source.y + 24),
    params: cloneJson(source.params || {})
  };
  state.blocks.push(duplicated);
  state.selectedId = duplicated.id;
  commitHistoryState();
  return duplicated;
}

function copySelectedNodeToClipboard() {
  const source = state.blocks.find((it) => it.id === state.selectedId);
  if (!source) return false;
  state.nodeClipboard = {
    type: source.type,
    params: cloneJson(source.params || {}),
    x: source.x,
    y: source.y
  };
  state.clipboardPasteCount = 0;
  return true;
}

function pasteNodeFromClipboard() {
  if (!state.nodeClipboard) return false;
  const pasted = {
    id: `n${state.nextId++}`,
    type: state.nodeClipboard.type,
    x: Math.round(state.mouseWorld.x),
    y: Math.round(state.mouseWorld.y),
    params: cloneJson(state.nodeClipboard.params || {})
  };
  state.blocks.push(pasted);
  state.selectedId = pasted.id;
  state.clipboardPasteCount = 0;
  commitHistoryState();
  return true;
}

function hasDuplicateLink(fromId, toId, kind, fromPortClass, toPortClass) {
  return state.links.some((it) => (
    it.from === fromId
    && it.to === toId
    && (it.kind || 'exec') === (kind || 'exec')
    && (it.fromPortClass || 'out') === (fromPortClass || 'out')
    && (it.toPortClass || '') === (toPortClass || '')
  ));
}

function addLink(fromId, toId, kind, fromPortClass, toPortClass) {
  if (!fromId || !toId || fromId === toId) return;
  const fromNode = state.blocks.find((it) => it.id === fromId);
  const toNode = state.blocks.find((it) => it.id === toId);
  if (!fromNode || !toNode) return;
  const resolvedKind = kind || linkKindFromType(fromNode.type);
  if (!canLinkFromSourceToTarget(fromId, resolvedKind, toNode.type)) return;
  const resolvedToPortClass = toPortClass || targetPortClassForLink(resolvedKind, toNode.type, '');
  if (!canLinkKindToPort(resolvedKind, toNode.type, resolvedToPortClass, fromId)) return;
  if (hasDuplicateLink(fromId, toId, resolvedKind, fromPortClass, resolvedToPortClass)) return;
  state.links.push({
    from: fromId,
    to: toId,
    kind: resolvedKind,
    fromPortClass: fromPortClass || 'out',
    toPortClass: resolvedToPortClass
  });
  commitHistoryState();
}

function getPortCenterWorld(nodeId, portType) {
  const nodeEl = el.canvas.querySelector(`.block-node[data-id="${nodeId}"]`);
  if (!(nodeEl instanceof HTMLElement)) return null;
  const portEl = nodeEl.querySelector(`.port.${portType}`);
  if (!(portEl instanceof HTMLElement)) return null;

  const portRect = portEl.getBoundingClientRect();
  const workspaceRect = el.workspace.getBoundingClientRect();
  return {
    x: (portRect.left + (portRect.width / 2) - workspaceRect.left - state.panX) / state.scale,
    y: (portRect.top + (portRect.height / 2) - workspaceRect.top - state.panY) / state.scale
  };
}

function resolveLinkTargetFromClient(clientX, clientY) {
  const sourceId = state.connectionDraft?.from;
  const sourceNode = sourceId ? state.blocks.find((it) => it.id === sourceId) : null;
  if (!sourceNode || !canStartLink(sourceNode.type)) return null;
  const sourceKind = state.connectionDraft?.kind || linkKindFromType(sourceNode.type);

  const hovered = document.elementFromPoint(clientX, clientY);
  const inputPort = hovered instanceof Element ? hovered.closest('.port.in') : null;
  if (inputPort?.dataset.nodeId) {
    const targetId = inputPort.dataset.nodeId;
    const targetNode = state.blocks.find((it) => it.id === targetId);
    const accept = inputPort.dataset.accept || 'exec';
    const targetPortClass = inputPort.dataset.portClass || (
      accept === 'data' ? 'in-data' : (accept === 'player' ? 'in-player' : 'in-exec')
    );
    const acceptOk = (sourceKind === 'exec' && accept === 'exec')
      || (isDataKind(sourceKind) && accept === 'data')
      || (sourceKind === 'data-bool' && accept === 'data-bool')
      || (sourceKind === 'player' && accept === 'player');
    if (!acceptOk) return null;
    if (!(targetNode && canLinkFromSourceToTarget(sourceId, sourceKind, targetNode.type))) return null;
    if (!canLinkKindToPort(sourceKind, targetNode.type, targetPortClass, sourceId)) return null;
    return { id: targetId, port: targetPortClass };
  }

  const nodeElement = hovered instanceof Element ? hovered.closest('.block-node') : null;
  if (nodeElement?.dataset.id) {
    const targetNodeId = nodeElement.dataset.id;
    const targetNode = state.blocks.find((it) => it.id === targetNodeId);
    if (targetNode && canLinkFromSourceToTarget(sourceId, sourceKind, targetNode.type)) {
      const port = targetPortClassForLink(sourceKind, targetNode.type);
      if (!canLinkKindToPort(sourceKind, targetNode.type, port, sourceId)) return null;
      return { id: targetNodeId, port };
    }
  }
  return null;
}

function getDraftTargetWorld() {
  const snapTargetId = state.connectionDraft?.snapTargetId;
  if (!snapTargetId) return state.mouseWorld;
  const portClass = state.connectionDraft?.snapTargetPort || 'in-exec';
  return getPortCenterWorld(snapTargetId, portClass) || state.mouseWorld;
}

function updateSnapInputHighlightOnly() {
  const highlighted = el.canvas.querySelectorAll('.port.in.linking');
  for (const item of highlighted) {
    item.classList.remove('linking');
  }

  const snapTargetId = state.connectionDraft?.snapTargetId;
  const snapTargetPort = state.connectionDraft?.snapTargetPort || 'in-exec';
  if (!snapTargetId) return;

  const nodeEl = el.canvas.querySelector(`.block-node[data-id="${snapTargetId}"]`);
  if (!(nodeEl instanceof HTMLElement)) return;
  const inputPort = nodeEl.querySelector(`.port.in.${snapTargetPort}`);
  if (inputPort instanceof HTMLElement) {
    inputPort.classList.add('linking');
  }
}

function renderLinksLayer() {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('class', 'links-layer');
  svg.setAttribute('width', '100%');
  svg.setAttribute('height', '100%');
  const linkDiagnostics = buildLinkErrorDiagnostics();

  for (let index = 0; index < state.links.length; index += 1) {
    const link = state.links[index];
    const draftPortClass = state.connectionDraft?.fromPortClass || '';
    const linkKind = link.kind || 'exec';
    const linkPortClass = link.fromPortClass || `out-${linkKind}`;
    const isDraftSourceLink = !!(
      state.connectionDraft
      && state.linkingActive
      && link.from === state.connectionDraft.from
      && draftPortClass
      && linkPortClass === draftPortClass
    );
    const fromNode = state.blocks.find((it) => it.id === link.from);
    const toNode = state.blocks.find((it) => it.id === link.to);
    if (!fromNode || !toNode) continue;
    const kind = link.kind || linkKindFromType(fromNode.type);
    const fromPort = link.fromPortClass || `out-${kind}`;
    const toPort = targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
    const isInvalidLink = linkDiagnostics.invalidIndexes.has(index);
    const from = getPortCenterWorld(link.from, fromPort);
    const to = getPortCenterWorld(link.to, toPort);
    if (!from || !to) continue;
    const d = linkPath(from, to);

    const back = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    back.setAttribute('class', isDraftSourceLink ? 'link-outline-back' : (isInvalidLink ? 'link-invalid-back' : 'link-back'));
    back.setAttribute('d', d);
    svg.appendChild(back);

    const front = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    front.setAttribute('class', isDraftSourceLink ? 'link-outline-front' : (isInvalidLink ? 'link-invalid-front' : 'link-front'));
    front.setAttribute('d', d);
    svg.appendChild(front);
  }

  if (state.connectionDraft) {
    const fromAnchor = getPortCenterWorld(state.connectionDraft.from, state.connectionDraft.fromPortClass || 'out');
    if (fromAnchor) {
      const draftD = linkPath(fromAnchor, getDraftTargetWorld());

      const draftBack = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      draftBack.setAttribute('class', 'draft-back');
      draftBack.setAttribute('d', draftD);
      svg.appendChild(draftBack);

      const draftFront = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      draftFront.setAttribute('class', 'draft-front');
      draftFront.setAttribute('d', draftD);
      svg.appendChild(draftFront);
    }
  }

  el.canvas.appendChild(svg);
}

function updateDraftPathOnly() {
  if (!state.connectionDraft || !state.linkingActive) return;
  const svg = el.canvas.querySelector('.links-layer');
  if (!(svg instanceof SVGElement)) return;

  const fromAnchor = getPortCenterWorld(state.connectionDraft.from, state.connectionDraft.fromPortClass || 'out');
  if (!fromAnchor) return;
  const d = linkPath(fromAnchor, getDraftTargetWorld());

  const draftBack = svg.querySelector('path.draft-back');
  const draftFront = svg.querySelector('path.draft-front');
  if (draftBack instanceof SVGPathElement) draftBack.setAttribute('d', d);
  if (draftFront instanceof SVGPathElement) draftFront.setAttribute('d', d);
}

function updateRenderedLinkPathsOnly() {
  const svg = el.canvas.querySelector('.links-layer');
  if (!(svg instanceof SVGElement)) return;

  const backPaths = Array.from(svg.querySelectorAll('path.link-back, path.link-outline-back'));
  const frontPaths = Array.from(svg.querySelectorAll('path.link-front, path.link-outline-front'));
  let index = 0;

  for (const link of state.links) {
    const fromNode = state.blocks.find((it) => it.id === link.from);
    const toNode = state.blocks.find((it) => it.id === link.to);
    if (!fromNode || !toNode) continue;
    const kind = link.kind || linkKindFromType(fromNode.type);
    const fromPort = link.fromPortClass || `out-${kind}`;
    const toPort = targetPortClassForLink(kind, toNode.type, link.toPortClass || '');
    const from = getPortCenterWorld(link.from, fromPort);
    const to = getPortCenterWorld(link.to, toPort);
    if (!from || !to) continue;
    const d = linkPath(from, to);

    const back = backPaths[index];
    const front = frontPaths[index];
    if (back instanceof SVGPathElement) back.setAttribute('d', d);
    if (front instanceof SVGPathElement) front.setAttribute('d', d);
    index += 1;
  }

  updateDraftPathOnly();
}

function renderComment(comment) {
  const box = document.createElement('div');
  box.className = `comment-box${state.selectedCommentId === comment.id ? ' selected' : ''}`;
  const rectOverride = renderPresenceOverrides.commentRects.get(comment.id);
  box.style.left = `${rectOverride ? rectOverride.x : comment.x}px`;
  box.style.top = `${rectOverride ? rectOverride.y : comment.y}px`;
  box.style.width = `${rectOverride ? rectOverride.width : comment.width}px`;
  box.style.height = `${rectOverride ? rectOverride.height : comment.height}px`;
  box.dataset.id = comment.id;

  const head = document.createElement('div');
  head.className = 'comment-head';

  const title = document.createElement('input');
  title.type = 'text';
  title.className = 'comment-title-input';
  title.value = comment.title || '';
  title.placeholder = '그룹 제목';
  title.addEventListener('input', () => {
    comment.title = title.value;
    scheduleCollaborativePublish(true);
  });
  title.addEventListener('blur', () => commitHistoryState());
  head.appendChild(title);

  const description = document.createElement('textarea');
  description.className = 'comment-description-input';
  description.value = comment.description || '';
  description.placeholder = '설명';
  description.style.overflow = 'hidden';
  description.style.resize = 'none';
  const resizeDescription = () => {
    description.style.height = 'auto';
    description.style.height = `${description.scrollHeight}px`;
  };
  requestAnimationFrame(resizeDescription);
  description.addEventListener('input', () => {
    comment.description = description.value;
    resizeDescription();
    scheduleCollaborativePublish(true);
  });
  description.addEventListener('blur', () => commitHistoryState());
  head.appendChild(description);

  box.appendChild(head);

  box.addEventListener('mousedown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest('input, textarea')) return;
    state.selectedCommentId = comment.id;
    state.selectedId = null;
    renderCanvas();
    schedulePresencePublish(true);
  });

  const beginCommentDrag = (event) => {
    if (event.button !== 0) return;
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest('input, textarea')) return;
    event.preventDefault();
    event.stopPropagation();
    state.selectedCommentId = comment.id;
    state.selectedId = null;
    state.draggingComment = {
      id: comment.id,
      startX: event.clientX,
      startY: event.clientY,
      commentX: comment.x,
      commentY: comment.y,
      moved: false
    };
    document.body.style.userSelect = 'none';
    renderCanvas();
    schedulePresencePublish(true);
  };
  head.addEventListener('mousedown', beginCommentDrag);

  const resizeEdges = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw'];
  for (const edge of resizeEdges) {
    const handle = document.createElement('div');
    handle.className = `comment-resize-handle ${edge}`;
    handle.dataset.edge = edge;
    handle.addEventListener('mousedown', (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      state.selectedCommentId = comment.id;
      state.selectedId = null;
      state.resizingComment = {
        id: comment.id,
        edge,
        startX: event.clientX,
        startY: event.clientY,
        x: comment.x,
        y: comment.y,
        width: comment.width,
        height: comment.height,
        moved: false
      };
      document.body.style.userSelect = 'none';
      renderCanvas();
      schedulePresencePublish(true);
    });
    box.appendChild(handle);
  }

  el.canvas.appendChild(box);
}

function renderNode(node, nodeAiRiskMap = new Map()) {
  const hasIncomingExec = state.links.some((it) => it.to === node.id && ((it.kind || 'exec') === 'exec'));
  const hasIncomingData = state.links.some((it) => it.to === node.id && isDataKind(it.kind || 'exec'));
  const hasIncomingPlayer = state.links.some((it) => it.to === node.id && ((it.kind || 'exec') === 'player'));
  const hasOutgoingByPort = (portClass, kind) => state.links.some((it) => {
    if (it.from !== node.id) return false;
    const linkKind = it.kind || 'exec';
    const linkPortClass = it.fromPortClass || `out-${linkKind}`;
    if (portClass) return linkPortClass === portClass;
    return linkKind === kind;
  });
  const nodeEl = document.createElement('div');
  const roleClass = isVariableType(node.type)
    ? 'variable'
    : (isEventType(node.type) ? 'event' : (isOperationType(node.type) ? 'operation' : 'function'));
  const sourceId = state.connectionDraft?.from || '';
  const sourceType = sourceId ? (state.blocks.find((it) => it.id === sourceId)?.type || '') : '';
  const sourceKind = state.connectionDraft?.kind || linkKindFromType(sourceType);
  const isLinking = !!(state.connectionDraft && state.linkingActive);
  const canBeTarget = sourceId && node.id !== sourceId ? canLinkFromSourceToTarget(sourceId, sourceKind, node.type) : false;
  const isCandidateTarget = isLinking && canBeTarget;
  const isUnavailableTarget = isLinking && node.id !== sourceId && !canBeTarget;
  nodeEl.className = `block-node ${roleClass}${state.selectedId === node.id ? ' selected' : ''}${isUnavailableTarget ? ' non-link-target' : ''}`;
  const posOverride = renderPresenceOverrides.nodePositions.get(node.id);
  nodeEl.style.left = `${posOverride ? posOverride.x : node.x}px`;
  nodeEl.style.top = `${posOverride ? posOverride.y : node.y}px`;
  nodeEl.dataset.id = node.id;

  const head = document.createElement('div');
  head.className = 'block-head';
  head.innerHTML = `
    <div class="block-head-meta">
      <div class="block-title">${titleOf(node.type)}</div>
      <div class="block-type">${typeLabelOf(node.type)}</div>
    </div>
  `;
  const badgeBox = document.createElement('div');
  badgeBox.className = 'block-head-badges';
  if (isEventType(node.type)) {
    const orderBadge = document.createElement('div');
    orderBadge.className = 'exec-order-badge';
    orderBadge.innerHTML = `
      <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
        <path d="M4 2.2a.8.8 0 0 1 1.6 0v.35h5.15c.63 0 .96.75.53 1.2L9.7 5.42l1.57 1.65c.43.45.1 1.2-.53 1.2H5.6V13a.8.8 0 1 1-1.6 0z" fill="currentColor" transform="translate(1.05 0.7)"></path>
      </svg>
    `;
    orderBadge.dataset.tooltip = '시작점';
    badgeBox.appendChild(orderBadge);
  }
  const aiRisk = nodeAiRiskMap.get(node.id);
  if (aiRisk) {
    const aiBadge = document.createElement('div');
    aiBadge.className = `ai-risk-badge ${aiRisk.level}`;
    aiBadge.innerHTML = `
      <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
        <path d="M8 3.3v6.2" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"></path>
        <circle cx="8" cy="12.6" r="1.25" fill="currentColor"></circle>
      </svg>
    `;
    aiBadge.dataset.tooltip = `판단 리스크: ${aiRisk.label}`;
    aiBadge.dataset.tooltipReason = `이유: ${aiRisk.reason}`;
    badgeBox.appendChild(aiBadge);
  }
  if (badgeBox.childElementCount > 0) {
    head.appendChild(badgeBox);
  }

  const pinRow = document.createElement('div');
  pinRow.className = 'pin-row';
  const leftPinGroup = document.createElement('div');
  leftPinGroup.className = 'pin-group left';
  const rightPinGroup = document.createElement('div');
  rightPinGroup.className = 'pin-group right';

  const body = document.createElement('div');
  body.className = 'block-body';

  if (isVariableType(node.type)) {
    const valueLabel = document.createElement('label');
    valueLabel.textContent = '값';
    valueLabel.style.marginTop = '0';

    const isTextVariable = node.type === 'VAR_TEXT';
    const isBooleanVariable = node.type === 'VAR_BOOLEAN';
    let valueInput = null;
    if (isBooleanVariable) {
      const raw = String(node.params?.value || '').trim().toLowerCase();
      const getNormalized = () => (raw === 'true' ? 'true' : 'false');
      const picker = document.createElement('div');
      picker.className = 'boolean-variable-picker';

      const trigger = document.createElement('button');
      trigger.type = 'button';
      trigger.className = 'boolean-variable-trigger';

      const menu = document.createElement('div');
      menu.className = 'boolean-variable-menu hidden';

      const options = [
        { value: 'true', label: '참' },
        { value: 'false', label: '거짓' }
      ];
      let selectedValue = getNormalized();

      const refreshTriggerLabel = () => {
        trigger.textContent = selectedValue === 'true' ? '참' : '거짓';
      };
      const closeMenu = () => {
        picker.classList.remove('open');
        menu.classList.add('hidden');
      };
      const openMenu = () => {
        picker.classList.add('open');
        menu.classList.remove('hidden');
      };

      for (const option of options) {
        const optionButton = document.createElement('button');
        optionButton.type = 'button';
        optionButton.className = 'boolean-variable-option';
        optionButton.textContent = option.label;
        if (option.value === selectedValue) optionButton.classList.add('active');
        optionButton.addEventListener('click', (event) => {
          event.preventDefault();
          selectedValue = option.value;
          node.params = node.params || {};
          node.params.value = selectedValue;
          for (const button of menu.querySelectorAll('.boolean-variable-option')) {
            button.classList.toggle('active', button === optionButton);
          }
          refreshTriggerLabel();
          closeMenu();
          scheduleCollaborativePublish(true);
        });
        menu.appendChild(optionButton);
      }

      trigger.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (picker.classList.contains('open')) {
          closeMenu();
        } else {
          openMenu();
        }
      });
      picker.addEventListener('focusout', () => {
        requestAnimationFrame(() => {
          if (!picker.contains(document.activeElement)) closeMenu();
        });
      });

      refreshTriggerLabel();
      picker.appendChild(trigger);
      picker.appendChild(menu);
      body.appendChild(valueLabel);
      body.appendChild(picker);
    } else {
      valueInput = document.createElement(isTextVariable ? 'textarea' : 'input');
      valueInput.value = node.params?.value || '';
      if (isTextVariable) {
        valueInput.classList.add('text-variable-input');
        valueInput.rows = 1;
        valueInput.style.overflow = 'hidden';
        valueInput.style.resize = 'none';
        const resizeTextArea = () => {
          valueInput.style.height = 'auto';
          valueInput.style.height = `${valueInput.scrollHeight}px`;
        };
        requestAnimationFrame(resizeTextArea);
        valueInput.addEventListener('input', resizeTextArea);
      }
      valueInput.addEventListener('input', () => {
        node.params = node.params || {};
        node.params.value = valueInput.value;
        scheduleCollaborativePublish(true);
      });
      body.appendChild(valueLabel);
      body.appendChild(valueInput);
    }
  }

  nodeEl.appendChild(head);
  nodeEl.appendChild(pinRow);
  if (body.childElementCount > 0) {
    nodeEl.appendChild(body);
  }

  if (isVariableType(node.type)) {
    const leftSpacer = document.createElement('div');
    leftSpacer.className = 'pin-spacer';
    leftPinGroup.appendChild(leftSpacer);
  } else if (node.type === 'EVENT_ON_PLAYER_JOIN') {
    leftPinGroup.classList.add('stack');
    const inItem = document.createElement('div');
    inItem.className = 'pin-in-item';

    const inData = document.createElement('div');
    inData.className = `port in in-data${hasIncomingData ? ' connected' : ''}`;
    inData.dataset.tooltip = 'message 입력';
    inData.dataset.portType = 'in';
    inData.dataset.accept = 'data';
    inData.dataset.portClass = 'in-data';
    inData.dataset.nodeId = node.id;
    if (isCandidateTarget && isDataKind(sourceKind)) inData.classList.add('candidate');

    const inLabel = document.createElement('div');
    inLabel.className = 'pin-label';
    inLabel.textContent = 'message';

    inItem.appendChild(inData);
    inItem.appendChild(inLabel);
    leftPinGroup.appendChild(inItem);
  } else if (node.type === 'EVENT_ON_PLAYER_CHAT') {
    leftPinGroup.classList.add('stack');
    const appendChatInput = (label, portClass) => {
      const connected = state.links.some((it) => it.to === node.id && isDataKind(it.kind || 'exec') && (it.toPortClass || '') === portClass);
      const inItem = document.createElement('div');
      inItem.className = 'pin-in-item';

      const inPort = document.createElement('div');
      inPort.className = `port in ${portClass}${connected ? ' connected' : ''}`;
      inPort.dataset.tooltip = `${label} 입력`;
      inPort.dataset.portType = 'in';
      inPort.dataset.accept = portClass === 'in-cancel' ? 'data-bool' : 'data';
      inPort.dataset.portClass = portClass;
      inPort.dataset.nodeId = node.id;
      const candidateAllowed = portClass === 'in-cancel' ? sourceKind === 'data-bool' : isDataKind(sourceKind);
      if (isCandidateTarget && candidateAllowed) inPort.classList.add('candidate');

      const inLabel = document.createElement('div');
      inLabel.className = 'pin-label';
      inLabel.textContent = label;

      inItem.appendChild(inPort);
      inItem.appendChild(inLabel);
      leftPinGroup.appendChild(inItem);
    };
    appendChatInput('message', 'in-data');
    appendChatInput('취소', 'in-cancel');
  } else if (node.type === 'FUNCTION_SEND_MESSAGE' || node.type === 'FUNCTION_BROADCAST_MESSAGE') {
    leftPinGroup.classList.add('stack');

    const appendInPort = (label, accept, extraClass, connected) => {
      const inItem = document.createElement('div');
      inItem.className = 'pin-in-item';

      const inPort = document.createElement('div');
      inPort.className = `port in ${extraClass}${connected ? ' connected' : ''}`;
      inPort.dataset.tooltip = `${label} 입력`;
      inPort.dataset.portType = 'in';
      inPort.dataset.accept = accept;
      inPort.dataset.portClass = extraClass;
      inPort.dataset.nodeId = node.id;
      if (isCandidateTarget) {
        if (accept === 'exec' && sourceKind === 'exec') inPort.classList.add('candidate');
        if (accept === 'data' && isDataKind(sourceKind)) inPort.classList.add('candidate');
        if (accept === 'player' && sourceKind === 'player') inPort.classList.add('candidate');
      }

      const inLabel = document.createElement('div');
      inLabel.className = 'pin-label';
      inLabel.textContent = label;

      inItem.appendChild(inPort);
      inItem.appendChild(inLabel);
      leftPinGroup.appendChild(inItem);
    };

    appendInPort('실행', 'exec', 'in-exec', hasIncomingExec);
    if (node.type === 'FUNCTION_SEND_MESSAGE') {
      appendInPort('player', 'player', 'in-player', hasIncomingPlayer);
    }
    appendInPort('message', 'data', 'in-data', hasIncomingData);
  } else if (node.type === 'MATH_ADD' || node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV') {
    leftPinGroup.classList.add('stack');
    const appendAddInput = (label, portClass) => {
      const connected = state.links.some((it) => it.to === node.id && isDataKind(it.kind || 'exec') && (it.toPortClass || '') === portClass);
      const inItem = document.createElement('div');
      inItem.className = 'pin-in-item';
      const inPort = document.createElement('div');
      inPort.className = `port in ${portClass}${connected ? ' connected' : ''}`;
      inPort.dataset.tooltip = `${label} 입력`;
      inPort.dataset.portType = 'in';
      inPort.dataset.accept = 'data';
      inPort.dataset.portClass = portClass;
      inPort.dataset.nodeId = node.id;
      const candidateAllowed = (node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV')
        ? (isNumericDataKind(sourceKind) || (sourceKind === 'data-any' && canNodeOutputBeNumeric(sourceId)))
        : isDataKind(sourceKind);
      if (isCandidateTarget && candidateAllowed) inPort.classList.add('candidate');
      const inLabel = document.createElement('div');
      inLabel.className = 'pin-label';
      inLabel.textContent = label;
      inItem.appendChild(inPort);
      inItem.appendChild(inLabel);
      leftPinGroup.appendChild(inItem);
    };
    appendAddInput('A', 'in-data-a');
    appendAddInput('B', 'in-data-b');
  } else if (!isEventType(node.type)) {
    const inPort = document.createElement('div');
    const outlinedInput = state.connectionDraft && state.linkingActive && (state.connectionDraft.previousTargets || []).includes(node.id);
    inPort.className = `port in in-exec${hasIncomingExec ? ' connected' : ''}${outlinedInput ? ' outline' : ''}`;
    inPort.dataset.tooltip = '실행 입력';
    inPort.dataset.portType = 'in';
    inPort.dataset.accept = 'exec';
    inPort.dataset.portClass = 'in-exec';
    inPort.dataset.nodeId = node.id;
    if (isCandidateTarget && sourceKind === 'exec') inPort.classList.add('candidate');
    leftPinGroup.appendChild(inPort);

    const inLabel = document.createElement('div');
    inLabel.className = 'pin-label';
    inLabel.textContent = '실행';
    leftPinGroup.appendChild(inLabel);
  } else {
    const spacer = document.createElement('div');
    spacer.className = 'pin-spacer';
    leftPinGroup.appendChild(spacer);
  }

  const appendOutPort = (label, kind) => {
    const outPortClass = `out-${kind}`;
    const outItem = document.createElement('div');
    outItem.className = 'pin-out-item';

    const outPort = document.createElement('div');
    const linkingFromThis = state.connectionDraft?.from === node.id
      && state.linkingActive
      && state.connectionDraft?.fromPortClass === outPortClass;
    const connectedOnThisPort = hasOutgoingByPort(outPortClass, kind);
    outPort.className = `port out ${outPortClass}${connectedOnThisPort ? ' connected' : ''}${linkingFromThis ? ' linking outline' : ''}`;
    outPort.dataset.tooltip = isDataKind(kind)
      ? '데이터 출력'
      : (kind === 'player' ? 'player 출력' : (kind === 'context' ? '컨텍스트 출력' : '실행 출력'));
    outPort.dataset.portType = 'out';
    outPort.dataset.nodeId = node.id;
    outPort.dataset.kind = kind;
    outPort.dataset.portClass = outPortClass;
    outPort.addEventListener('mousedown', (event) => {
      if (event.button !== 0) return;
      event.stopPropagation();
      event.preventDefault();
      state.connectionDraft = {
        from: node.id,
        kind,
        fromPortClass: outPortClass,
        previousTargets: state.links.filter((it) => it.from === node.id).map((it) => it.to),
        snapTargetId: null,
        snapTargetPort: null
      };
      state.linkingActive = true;
      renderCanvas();
      schedulePresencePublish(true);
    });
    const outLabel = document.createElement('div');
    outLabel.className = 'pin-label';
    outLabel.textContent = label;

    outItem.appendChild(outPort);
    outItem.appendChild(outLabel);
    rightPinGroup.appendChild(outItem);
  };

  if (node.type === 'EVENT_ON_ENABLE') {
    rightPinGroup.classList.add('stack');
    appendOutPort('다음', 'exec');
    appendOutPort('PluginContext', 'context');
  } else if (node.type === 'EVENT_ON_PLAYER_JOIN') {
    rightPinGroup.classList.add('stack');
    appendOutPort('다음', 'exec');
    appendOutPort('player', 'player');
    appendOutPort('message', 'data-text');
  } else if (node.type === 'EVENT_ON_PLAYER_CHAT') {
    rightPinGroup.classList.add('stack');
    appendOutPort('다음', 'exec');
    appendOutPort('player', 'player');
    appendOutPort('message', 'data-text');
  } else if (node.type === 'VAR_TEXT') {
    appendOutPort('값', 'data-text');
  } else if (node.type === 'VAR_INTEGER') {
    appendOutPort('값', 'data-int');
  } else if (node.type === 'VAR_DECIMAL') {
    appendOutPort('값', 'data-decimal');
  } else if (node.type === 'VAR_BOOLEAN') {
    appendOutPort('값', 'data-bool');
  } else if (node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV') {
    appendOutPort('값', 'data-number');
  } else if (node.type === 'MATH_ADD') {
    appendOutPort('값', 'data-any');
  } else {
    appendOutPort('다음', 'exec');
  }

  pinRow.appendChild(leftPinGroup);
  pinRow.appendChild(rightPinGroup);

  nodeEl.addEventListener('mousedown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest('input, textarea, select, button')) return;
    state.selectedId = node.id;
    state.selectedCommentId = null;
    hideContextMenu();
    renderCanvas();
    schedulePresencePublish(true);
  });

  head.addEventListener('mousedown', (event) => {
    if (event.button !== 0) return;
    event.stopPropagation();
    state.selectedId = node.id;
    state.selectedCommentId = null;
    hideContextMenu();
    renderCanvas();
    schedulePresencePublish(true);
    state.draggingNode = {
      id: node.id,
      startX: event.clientX,
      startY: event.clientY,
      nodeX: node.x,
      nodeY: node.y,
      moved: false
    };
    document.body.style.userSelect = 'none';
  });

  el.canvas.appendChild(nodeEl);
}

function computeBlockBounds() {
  if (!state.blocks.length) return null;
  let minX = Number.POSITIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;

  for (const block of state.blocks) {
    minX = Math.min(minX, block.x);
    minY = Math.min(minY, block.y);
    maxX = Math.max(maxX, block.x + BLOCK_NODE_WIDTH);
    maxY = Math.max(maxY, block.y + BLOCK_NODE_ESTIMATED_HEIGHT);
  }
  return { minX, minY, maxX, maxY, width: maxX - minX, height: maxY - minY };
}

function getViewportWorldRect() {
  const width = Math.max(1, el.workspace.clientWidth / Math.max(0.0001, state.scale));
  const height = Math.max(1, el.workspace.clientHeight / Math.max(0.0001, state.scale));
  const x = -state.panX / Math.max(0.0001, state.scale);
  const y = -state.panY / Math.max(0.0001, state.scale);
  return { x, y, width, height };
}

function measureNodeSizeInWorld(nodeId) {
  const nodeEl = el.canvas.querySelector(`.block-node[data-id="${nodeId}"]`);
  if (!(nodeEl instanceof HTMLElement)) {
    return { width: BLOCK_NODE_WIDTH, height: BLOCK_NODE_ESTIMATED_HEIGHT };
  }
  const rect = nodeEl.getBoundingClientRect();
  const worldW = rect.width / Math.max(0.0001, state.scale);
  const worldH = rect.height / Math.max(0.0001, state.scale);
  return {
    width: Number.isFinite(worldW) && worldW > 0 ? worldW : BLOCK_NODE_WIDTH,
    height: Number.isFinite(worldH) && worldH > 0 ? worldH : BLOCK_NODE_ESTIMATED_HEIGHT
  };
}

function rectEdgeAnchorToward(centerX, centerY, width, height, targetX, targetY) {
  const halfW = Math.max(0.0001, width / 2);
  const halfH = Math.max(0.0001, height / 2);
  const dx = targetX - centerX;
  const dy = targetY - centerY;
  if (Math.abs(dx) < 1e-6 && Math.abs(dy) < 1e-6) {
    return { x: centerX, y: centerY };
  }
  const t = Math.max(Math.abs(dx) / halfW, Math.abs(dy) / halfH);
  return {
    x: centerX + (dx / t),
    y: centerY + (dy / t)
  };
}

function computeSceneBoundsForMinimap() {
  const viewport = getViewportWorldRect();
  let minX = viewport.x;
  let minY = viewport.y;
  let maxX = viewport.x + viewport.width;
  let maxY = viewport.y + viewport.height;

  for (const block of state.blocks) {
    const pos = renderPresenceOverrides.nodePositions.get(block.id);
    const size = measureNodeSizeInWorld(block.id);
    const x = pos ? pos.x : block.x;
    const y = pos ? pos.y : block.y;
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + size.width);
    maxY = Math.max(maxY, y + size.height);
  }
  for (const comment of state.comments) {
    const rect = renderPresenceOverrides.commentRects.get(comment.id);
    const x = rect ? rect.x : comment.x;
    const y = rect ? rect.y : comment.y;
    const w = rect ? rect.width : comment.width;
    const h = rect ? rect.height : comment.height;
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x + Math.max(1, w));
    maxY = Math.max(maxY, y + Math.max(1, h));
  }

  const rawWidth = Math.max(1, maxX - minX);
  const rawHeight = Math.max(1, maxY - minY);
  const pad = Math.max(80, Math.max(rawWidth, rawHeight) * 0.08);
  return {
    minX: minX - pad,
    minY: minY - pad,
    maxX: maxX + pad,
    maxY: maxY + pad,
    width: rawWidth + (pad * 2),
    height: rawHeight + (pad * 2)
  };
}

function renderMinimap(activePeers = collectSmoothedActivePeers(performance.now()), now = performance.now()) {
  if (!(el.minimapCanvas instanceof HTMLElement)) return;
  const width = Math.max(1, el.minimapCanvas.clientWidth);
  const height = Math.max(1, el.minimapCanvas.clientHeight);
  const bounds = computeSceneBoundsForMinimap();
  const scale = Math.max(0.0001, Math.min(width / bounds.width, height / bounds.height));
  const offsetX = (width - (bounds.width * scale)) / 2;
  const offsetY = (height - (bounds.height * scale)) / 2;
  minimapState.bounds = bounds;
  minimapState.scale = scale;
  minimapState.offsetX = offsetX;
  minimapState.offsetY = offsetY;

  const worldToMapX = (x) => offsetX + ((x - bounds.minX) * scale);
  const worldToMapY = (y) => offsetY + ((y - bounds.minY) * scale);
  const worldToMapW = (w) => Math.max(1, w * scale);
  const worldToMapH = (h) => Math.max(1, h * scale);
  const preserveAspectMinSize = (w, h, minSize = 1) => {
    let nextW = Math.max(0.0001, w);
    let nextH = Math.max(0.0001, h);
    if (nextW >= minSize && nextH >= minSize) return { width: nextW, height: nextH };
    const factor = Math.max(minSize / nextW, minSize / nextH);
    nextW *= factor;
    nextH *= factor;
    return { width: nextW, height: nextH };
  };

  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('class', 'minimap-svg');
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);

  for (const comment of state.comments) {
    const rect = renderPresenceOverrides.commentRects.get(comment.id);
    const x = rect ? rect.x : comment.x;
    const y = rect ? rect.y : comment.y;
    const w = rect ? rect.width : comment.width;
    const h = rect ? rect.height : comment.height;
    const cell = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    cell.setAttribute('class', 'minimap-comment');
    cell.setAttribute('x', String(worldToMapX(x)));
    cell.setAttribute('y', String(worldToMapY(y)));
    cell.setAttribute('width', String(worldToMapW(Math.max(1, w))));
    cell.setAttribute('height', String(worldToMapH(Math.max(1, h))));
    cell.setAttribute('rx', '2');
    cell.setAttribute('ry', '2');
    svg.appendChild(cell);
  }

  const linkGroupIndex = new Map();
  const linkGroupTotals = new Map();
  for (const link of state.links) {
    const key = `${link.from}->${link.to}`;
    linkGroupTotals.set(key, (linkGroupTotals.get(key) || 0) + 1);
  }

  for (const link of state.links) {
    const fromNode = state.blocks.find((it) => it.id === link.from);
    const toNode = state.blocks.find((it) => it.id === link.to);
    if (!fromNode || !toNode) continue;
    const groupKey = `${link.from}->${link.to}`;
    const nth = linkGroupIndex.get(groupKey) || 0;
    const total = linkGroupTotals.get(groupKey) || 1;
    linkGroupIndex.set(groupKey, nth + 1);
    const fromPos = renderPresenceOverrides.nodePositions.get(fromNode.id);
    const toPos = renderPresenceOverrides.nodePositions.get(toNode.id);
    const fromSize = measureNodeSizeInWorld(fromNode.id);
    const toSize = measureNodeSizeInWorld(toNode.id);
    const fromBaseX = fromPos ? fromPos.x : fromNode.x;
    const fromBaseY = fromPos ? fromPos.y : fromNode.y;
    const toBaseX = toPos ? toPos.x : toNode.x;
    const toBaseY = toPos ? toPos.y : toNode.y;
    const fromCenterX = fromBaseX + (fromSize.width / 2);
    const fromCenterY = fromBaseY + (fromSize.height / 2);
    const toCenterX = toBaseX + (toSize.width / 2);
    const toCenterY = toBaseY + (toSize.height / 2);
    const fromAnchor = rectEdgeAnchorToward(fromCenterX, fromCenterY, fromSize.width, fromSize.height, toCenterX, toCenterY);
    const toAnchor = rectEdgeAnchorToward(toCenterX, toCenterY, toSize.width, toSize.height, fromCenterX, fromCenterY);
    const fromMapX = worldToMapX(fromAnchor.x);
    const fromMapY = worldToMapY(fromAnchor.y);
    const toMapX = worldToMapX(toAnchor.x);
    const toMapY = worldToMapY(toAnchor.y);
    const dx = toMapX - fromMapX;
    const dy = toMapY - fromMapY;
    const len = Math.hypot(dx, dy);
    const unitNx = len > 1e-6 ? (-dy / len) : 0;
    const unitNy = len > 1e-6 ? (dx / len) : 0;
    const spacing = 2;
    const offset = (nth - ((total - 1) / 2)) * spacing;
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    path.setAttribute('class', 'minimap-link');
    path.setAttribute('x1', String(fromMapX + (unitNx * offset)));
    path.setAttribute('y1', String(fromMapY + (unitNy * offset)));
    path.setAttribute('x2', String(toMapX + (unitNx * offset)));
    path.setAttribute('y2', String(toMapY + (unitNy * offset)));
    svg.appendChild(path);
  }

  for (const block of state.blocks) {
    const pos = renderPresenceOverrides.nodePositions.get(block.id);
    const size = measureNodeSizeInWorld(block.id);
    const x = pos ? pos.x : block.x;
    const y = pos ? pos.y : block.y;
    const roleClass = isVariableType(block.type)
      ? 'variable'
      : (isEventType(block.type) ? 'event' : (isOperationType(block.type) ? 'operation' : 'function'));
    const cell = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    cell.setAttribute('class', `minimap-node ${roleClass}`);
    cell.setAttribute('x', String(worldToMapX(x)));
    cell.setAttribute('y', String(worldToMapY(y)));
    cell.setAttribute('width', String(worldToMapW(size.width)));
    cell.setAttribute('height', String(worldToMapH(size.height)));
    cell.setAttribute('rx', '1.5');
    cell.setAttribute('ry', '1.5');
    svg.appendChild(cell);
  }

  for (const peer of activePeers) {
    const p = smoothPeerPresence(peer, now) || {};
    const color = peer.color || '#d4d8e0';
    if (p.linking?.from && p.linking?.to) {
      const peerLink = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      peerLink.setAttribute('class', 'minimap-peer-link');
      peerLink.setAttribute('stroke', color);
      peerLink.setAttribute('x1', String(worldToMapX(p.linking.from.x || 0)));
      peerLink.setAttribute('y1', String(worldToMapY(p.linking.from.y || 0)));
      peerLink.setAttribute('x2', String(worldToMapX(p.linking.to.x || 0)));
      peerLink.setAttribute('y2', String(worldToMapY(p.linking.to.y || 0)));
      svg.appendChild(peerLink);
    }
    if (p.pointer) {
      const cursor = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      cursor.setAttribute('class', 'minimap-peer-cursor');
      cursor.setAttribute('fill', color);
      cursor.setAttribute('cx', String(worldToMapX(p.pointer.x || 0)));
      cursor.setAttribute('cy', String(worldToMapY(p.pointer.y || 0)));
      cursor.setAttribute('r', '3');
      svg.appendChild(cursor);
    }
  }

  const viewport = getViewportWorldRect();
  const viewportLeft = worldToMapX(viewport.x);
  const viewportTop = worldToMapY(viewport.y);
  const viewportRight = worldToMapX(viewport.x + viewport.width);
  const viewportBottom = worldToMapY(viewport.y + viewport.height);
  const centerX = (viewportLeft + viewportRight) / 2;
  const centerY = (viewportTop + viewportBottom) / 2;
  const viewportRawW = Math.max(0.0001, viewportRight - viewportLeft);
  const viewportRawH = Math.max(0.0001, viewportBottom - viewportTop);
  const workspaceRatio = Math.max(0.0001, el.workspace.clientWidth / Math.max(1, el.workspace.clientHeight));
  let ratioW = viewportRawW;
  let ratioH = viewportRawH;
  const currentRatio = ratioW / ratioH;
  if (currentRatio > workspaceRatio) {
    ratioH = ratioW / workspaceRatio;
  } else {
    ratioW = ratioH * workspaceRatio;
  }
  const viewportSize = preserveAspectMinSize(ratioW, ratioH, 1);
  const viewportRect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
  viewportRect.setAttribute('class', 'minimap-viewport');
  viewportRect.setAttribute('x', String(centerX - (viewportSize.width / 2)));
  viewportRect.setAttribute('y', String(centerY - (viewportSize.height / 2)));
  viewportRect.setAttribute('width', String(viewportSize.width));
  viewportRect.setAttribute('height', String(viewportSize.height));
  viewportRect.setAttribute('rx', '2');
  viewportRect.setAttribute('ry', '2');
  svg.appendChild(viewportRect);

  el.minimapCanvas.innerHTML = '';
  el.minimapCanvas.appendChild(svg);
}

function focusViewportFromMinimapClient(clientX, clientY) {
  if (!(el.minimapCanvas instanceof HTMLElement) || !minimapState.bounds) return;
  const rect = el.minimapCanvas.getBoundingClientRect();
  const localX = clientX - rect.left;
  const localY = clientY - rect.top;
  const worldX = ((localX - minimapState.offsetX) / minimapState.scale) + minimapState.bounds.minX;
  const worldY = ((localY - minimapState.offsetY) / minimapState.scale) + minimapState.bounds.minY;
  state.panX = (el.workspace.clientWidth / 2) - (worldX * state.scale);
  state.panY = (el.workspace.clientHeight / 2) - (worldY * state.scale);
  renderCanvas();
  schedulePresencePublish(true);
}

function getScaleBounds() {
  const max = 3;
  const bounds = computeBlockBounds();
  if (!bounds) return { min: 1, max };

  const padding = 80;
  const viewportW = Math.max(1, el.workspace.clientWidth);
  const viewportH = Math.max(1, el.workspace.clientHeight);
  const fitX = viewportW / (bounds.width + padding);
  const fitY = viewportH / (bounds.height + padding);
  const dynamicMin = Math.max(0.0001, Math.min(1, Math.min(fitX, fitY)));
  return { min: dynamicMin, max };
}

function lerpNumber(current, target, alpha) {
  const c = Number.isFinite(current) ? current : 0;
  const t = Number.isFinite(target) ? target : 0;
  return c + ((t - c) * alpha);
}

function blendPoint(current, target, alpha) {
  if (!target || typeof target !== 'object') return null;
  const c = current && typeof current === 'object' ? current : { x: target.x, y: target.y };
  return {
    x: lerpNumber(c.x, target.x, alpha),
    y: lerpNumber(c.y, target.y, alpha)
  };
}

function blendRect(current, target, alpha) {
  if (!target || typeof target !== 'object') return null;
  const c = current && typeof current === 'object' ? current : target;
  return {
    startX: lerpNumber(c.startX, target.startX, alpha),
    startY: lerpNumber(c.startY, target.startY, alpha),
    currentX: lerpNumber(c.currentX, target.currentX, alpha),
    currentY: lerpNumber(c.currentY, target.currentY, alpha)
  };
}

function blendSizedBox(current, target, alpha) {
  if (!target || typeof target !== 'object') return null;
  const c = current && typeof current === 'object' ? current : target;
  return {
    id: target.id || c.id || '',
    x: lerpNumber(c.x, target.x, alpha),
    y: lerpNumber(c.y, target.y, alpha),
    width: lerpNumber(c.width, target.width, alpha),
    height: lerpNumber(c.height, target.height, alpha)
  };
}

function blendLinking(current, target, alpha) {
  if (!target || typeof target !== 'object') return null;
  const c = current && typeof current === 'object' ? current : target;
  return {
    from: blendPoint(c.from, target.from, alpha),
    to: blendPoint(c.to, target.to, alpha),
    kind: target.kind || c.kind || 'exec'
  };
}

function blendPresence(current, target, alpha) {
  if (!target || typeof target !== 'object') return {};
  return {
    pointer: blendPoint(current?.pointer, target.pointer, alpha),
    linking: blendLinking(current?.linking, target.linking, alpha),
    commentDraft: blendRect(current?.commentDraft, target.commentDraft, alpha),
    draggingNode: blendSizedBox(
      current?.draggingNode,
      target.draggingNode
        ? { id: target.draggingNode.id, x: target.draggingNode.x, y: target.draggingNode.y, width: BLOCK_NODE_WIDTH, height: BLOCK_NODE_ESTIMATED_HEIGHT }
        : null,
      alpha
    ),
    draggingComment: blendSizedBox(current?.draggingComment, target.draggingComment, alpha),
    resizingComment: blendSizedBox(current?.resizingComment, target.resizingComment, alpha),
    selectedNodeId: target.selectedNodeId ?? null,
    selectedCommentId: target.selectedCommentId ?? null
  };
}

function smoothPeerPresence(peer, now) {
  const target = peer?.presence || {};
  const stored = realtimeState.smoothedPeers[peer.clientId];
  if (!stored) {
    const initial = cloneJson(target);
    realtimeState.smoothedPeers[peer.clientId] = { presence: initial, at: now };
    return initial;
  }
  const elapsedMs = Math.max(0, now - (stored.at || now));
  stored.at = now;
  const alpha = 1 - Math.exp(-elapsedMs / 70);
  stored.presence = blendPresence(stored.presence, target, alpha);
  return stored.presence;
}

function isTextEditingActive() {
  const active = document.activeElement;
  if (!(active instanceof Element)) return false;
  return !!active.closest('input, textarea, [contenteditable="true"]');
}

function ensurePresenceAnimationLoop() {
  if (realtimeState.presenceAnimationFrame) return;
  const tick = (now) => {
    realtimeState.presenceAnimationFrame = null;
    const activePeers = collectSmoothedActivePeers(now);
    refreshPresenceOverrides(activePeers, now);
    applyPresenceOverridesToCanvasDom();
    renderRemotePresenceLayer(now, activePeers);
    const staleBefore = Date.now() - 6000;
    const hasActivePeers = Object.values(realtimeState.peers).some((peer) => peer && peer.lastSeen >= staleBefore);
    const hasOverrideTail = renderPresenceOverrides.nodePositions.size > 0 || renderPresenceOverrides.commentRects.size > 0;
    if (hasActivePeers || hasOverrideTail || SHOW_SELF_PRESENCE_DEBUG) {
      realtimeState.presenceAnimationFrame = requestAnimationFrame(tick);
    }
  };
  realtimeState.presenceAnimationFrame = requestAnimationFrame(tick);
}

function collectSmoothedActivePeers(now = performance.now()) {
  const staleBefore = Date.now() - 6000;
  const activePeers = Object.values(realtimeState.peers).filter((peer) => peer && peer.lastSeen >= staleBefore);
  for (const [clientId, peer] of Object.entries(realtimeState.peers)) {
    if (!peer || peer.lastSeen < staleBefore) delete realtimeState.peers[clientId];
  }
  for (const clientId of Object.keys(realtimeState.smoothedPeers)) {
    if (!realtimeState.peers[clientId] || realtimeState.peers[clientId].lastSeen < staleBefore) {
      delete realtimeState.smoothedPeers[clientId];
    }
  }
  return activePeers;
}

function refreshPresenceOverrides(activePeers, now) {
  const frameDt = realtimeState.lastPresenceFrameAt > 0
    ? Math.max(1, Math.min(64, now - realtimeState.lastPresenceFrameAt))
    : 16;
  realtimeState.lastPresenceFrameAt = now;
  const alpha = 1 - Math.exp(-frameDt / 42);
  const nearly = (a, b) => Math.abs((a ?? 0) - (b ?? 0)) <= 0.6;
  const activeNodeIds = new Set();
  const activeCommentIds = new Set();
  for (const peer of activePeers) {
    const p = smoothPeerPresence(peer, now) || {};
    if (p.draggingNode?.id) {
      activeNodeIds.add(p.draggingNode.id);
      renderPresenceOverrides.nodePositions.set(p.draggingNode.id, {
        x: Number.isFinite(p.draggingNode.x) ? p.draggingNode.x : 0,
        y: Number.isFinite(p.draggingNode.y) ? p.draggingNode.y : 0
      });
    }
    const activeGroupDrag = p.resizingComment || p.draggingComment;
    if (activeGroupDrag?.id) {
      activeCommentIds.add(activeGroupDrag.id);
      renderPresenceOverrides.commentRects.set(activeGroupDrag.id, {
        x: Number.isFinite(activeGroupDrag.x) ? activeGroupDrag.x : 0,
        y: Number.isFinite(activeGroupDrag.y) ? activeGroupDrag.y : 0,
        width: Number.isFinite(activeGroupDrag.width) ? activeGroupDrag.width : 0,
        height: Number.isFinite(activeGroupDrag.height) ? activeGroupDrag.height : 0
      });
    }
  }
  for (const [nodeId, info] of renderPresenceOverrides.nodePositions.entries()) {
    if (activeNodeIds.has(nodeId)) continue;
    const node = state.blocks.find((it) => it.id === nodeId);
    if (!node || !info) {
      renderPresenceOverrides.nodePositions.delete(nodeId);
      continue;
    }
    info.x = lerpNumber(info.x, node.x, alpha);
    info.y = lerpNumber(info.y, node.y, alpha);
    if (nearly(info.x, node.x) && nearly(info.y, node.y)) {
      renderPresenceOverrides.nodePositions.delete(nodeId);
    }
  }
  for (const [commentId, info] of renderPresenceOverrides.commentRects.entries()) {
    if (activeCommentIds.has(commentId)) continue;
    const comment = state.comments.find((it) => it.id === commentId);
    if (!comment || !info) {
      renderPresenceOverrides.commentRects.delete(commentId);
      continue;
    }
    info.x = lerpNumber(info.x, comment.x, alpha);
    info.y = lerpNumber(info.y, comment.y, alpha);
    info.width = lerpNumber(info.width, comment.width, alpha);
    info.height = lerpNumber(info.height, comment.height, alpha);
    if (
      nearly(info.x, comment.x)
      && nearly(info.y, comment.y)
      && nearly(info.width, comment.width)
      && nearly(info.height, comment.height)
    ) {
      renderPresenceOverrides.commentRects.delete(commentId);
    }
  }
}

function applyPresenceOverridesToCanvasDom() {
  const nodeEls = el.canvas.querySelectorAll('.block-node[data-id]');
  for (const nodeEl of nodeEls) {
    if (!(nodeEl instanceof HTMLElement)) continue;
    const nodeId = nodeEl.dataset.id || '';
    const node = state.blocks.find((it) => it.id === nodeId);
    if (!node) continue;
    const pos = renderPresenceOverrides.nodePositions.get(nodeId);
    nodeEl.style.left = `${Math.round((pos ? pos.x : node.x) || 0)}px`;
    nodeEl.style.top = `${Math.round((pos ? pos.y : node.y) || 0)}px`;
  }

  const commentEls = el.canvas.querySelectorAll('.comment-box[data-id]');
  for (const commentEl of commentEls) {
    if (!(commentEl instanceof HTMLElement)) continue;
    const commentId = commentEl.dataset.id || '';
    const comment = state.comments.find((it) => it.id === commentId);
    if (!comment) continue;
    const rect = renderPresenceOverrides.commentRects.get(commentId);
    commentEl.style.left = `${Math.round((rect ? rect.x : comment.x) || 0)}px`;
    commentEl.style.top = `${Math.round((rect ? rect.y : comment.y) || 0)}px`;
    commentEl.style.width = `${Math.max(1, Math.round((rect ? rect.width : comment.width) || 0))}px`;
    commentEl.style.height = `${Math.max(1, Math.round((rect ? rect.height : comment.height) || 0))}px`;
  }

  // Keep wire endpoints synced with smoothed remote drag positions without full re-render.
  updateRenderedLinkPathsOnly();
}

function renderRemotePresenceLayer(now = performance.now(), activePeers = collectSmoothedActivePeers(now)) {
  if (SHOW_SELF_PRESENCE_DEBUG) {
    activePeers.push({
      clientId: realtimeState.clientId,
      color: '#ffffff',
      presence: buildLocalPresence(),
      lastSeen: Date.now()
    });
  }
  const oldLayer = el.canvas.querySelector('.remote-presence-layer');
  if (oldLayer) oldLayer.remove();
  if (!activePeers.length) return;

  const layer = document.createElement('div');
  layer.className = 'remote-presence-layer';

  for (const peer of activePeers) {
    const p = smoothPeerPresence(peer, now) || {};
    const color = peer.color || '#d4d8e0';
    const pointer = p.pointer;

    if (p.linking && p.linking.from) {
      const from = p.linking.from;
      const to = p.linking.to || pointer;
      if (from && to) {
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('class', 'remote-link-preview');
        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '100%');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', linkPath(from, to));
        path.setAttribute('stroke', color);
        svg.appendChild(path);
        layer.appendChild(svg);
      }
    }
    if (p.commentDraft) {
      const draft = p.commentDraft;
      const x = Math.min(draft.startX || 0, draft.currentX || 0);
      const y = Math.min(draft.startY || 0, draft.currentY || 0);
      const width = Math.abs((draft.currentX || 0) - (draft.startX || 0));
      const height = Math.abs((draft.currentY || 0) - (draft.startY || 0));
      if (width > 1 && height > 1) {
        const box = document.createElement('div');
        box.className = 'remote-comment-draft';
        box.style.left = `${Math.round(x)}px`;
        box.style.top = `${Math.round(y)}px`;
        box.style.width = `${Math.round(width)}px`;
        box.style.height = `${Math.round(height)}px`;
        box.style.setProperty('--remote-color', color);
        layer.appendChild(box);
      }
    }
    const activeGroupDrag = p.resizingComment || p.draggingComment;
    if (activeGroupDrag) {
      const box = document.createElement('div');
      box.className = 'remote-group-drag';
      box.style.left = `${Math.round(activeGroupDrag.x || 0)}px`;
      box.style.top = `${Math.round(activeGroupDrag.y || 0)}px`;
      box.style.width = `${Math.max(1, Math.round(activeGroupDrag.width || 0))}px`;
      box.style.height = `${Math.max(1, Math.round(activeGroupDrag.height || 0))}px`;
      box.style.setProperty('--remote-color', color);
      layer.appendChild(box);
    }
    if (p.draggingNode) {
      const node = p.draggingNode;
      const box = document.createElement('div');
      box.className = 'remote-node-drag';
      box.style.left = `${Math.round(node.x || 0)}px`;
      box.style.top = `${Math.round(node.y || 0)}px`;
      box.style.width = `${Math.max(1, Math.round(node.width || BLOCK_NODE_WIDTH))}px`;
      box.style.height = `${Math.max(1, Math.round(node.height || BLOCK_NODE_ESTIMATED_HEIGHT))}px`;
      box.style.setProperty('--remote-color', color);
      layer.appendChild(box);
    }
    if (pointer) {
      const cursor = document.createElement('div');
      cursor.className = 'remote-cursor';
      cursor.style.left = `${Math.round(pointer.x)}px`;
      cursor.style.top = `${Math.round(pointer.y)}px`;
      cursor.style.setProperty('--cursor-color', color);

      const icon = document.createElement('div');
      icon.className = 'remote-cursor-icon';
      icon.innerHTML = `
        <svg viewBox="-2 -2 28 28" aria-hidden="true">
          <path d="M6.2 2.8c-.8-.5-1.8.1-1.8 1.1v16.6c0 1 .9 1.6 1.7 1.1l4.6-2.8 2.3 4.8c.3.7 1.1 1 1.8.7l1.2-.5c.7-.3 1.1-1.1.8-1.8l-2.3-4.8h5.4c1 0 1.4-1.3.6-1.9L6.2 2.8z"></path>
        </svg>
      `;
      cursor.appendChild(icon);
      layer.appendChild(cursor);
    }
  }

  el.canvas.appendChild(layer);
}

function renderCanvas() {
  const now = performance.now();
  const activePeers = collectSmoothedActivePeers(now);
  refreshPresenceOverrides(activePeers, now);
  const bounds = getScaleBounds();
  if (state.scale > bounds.max) state.scale = bounds.max;

  el.canvas.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
  el.workspace.style.backgroundPosition = `${state.panX}px ${state.panY}px, ${state.panX}px ${state.panY}px, 0 0`;
  el.workspace.style.backgroundSize = `${24 * state.scale}px ${24 * state.scale}px, ${24 * state.scale}px ${24 * state.scale}px, auto`;

  el.canvas.innerHTML = '';
  const nodeAiRiskMap = buildNodeAiRiskMap();
  for (const comment of state.comments) {
    renderComment(comment);
  }
  for (const block of state.blocks) {
    renderNode(block, nodeAiRiskMap);
  }
  if (state.commentDraft) {
    const draft = state.commentDraft;
    const x = Math.min(draft.startX, draft.currentX);
    const y = Math.min(draft.startY, draft.currentY);
    const width = Math.abs(draft.currentX - draft.startX);
    const height = Math.abs(draft.currentY - draft.startY);
    const draftEl = document.createElement('div');
    draftEl.className = 'comment-box draft';
    draftEl.style.left = `${x}px`;
    draftEl.style.top = `${y}px`;
    draftEl.style.width = `${Math.max(1, width)}px`;
    draftEl.style.height = `${Math.max(1, height)}px`;
    el.canvas.appendChild(draftEl);
  }
  renderLinksLayer();
  renderRemotePresenceLayer(now, activePeers);
  renderMinimap(activePeers, now);

  el.worldInfo.textContent = `x: ${Math.round(state.panX)}, y: ${Math.round(state.panY)}, 배율: ${(state.scale * 100).toFixed(0)}%`;
}

let relayoutRenderScheduled = false;
function scheduleRelayoutRender() {
  if (relayoutRenderScheduled) return;
  relayoutRenderScheduled = true;
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      relayoutRenderScheduled = false;
      renderCanvas();
    });
  });
}

function beginPan(event) {
  state.panning = {
    startX: event.clientX,
    startY: event.clientY,
    panX: state.panX,
    panY: state.panY
  };
  document.body.style.userSelect = 'none';
}

function toWorldPosition(clientX, clientY) {
  const rect = el.workspace.getBoundingClientRect();
  return {
    x: (clientX - rect.left - state.panX) / state.scale,
    y: (clientY - rect.top - state.panY) / state.scale
  };
}

function hideContextMenu() {
  el.contextMenu.classList.add('hidden');
}

function ensureHoverTooltipElement() {
  if (hoverTooltipState.el instanceof HTMLElement) return hoverTooltipState.el;
  const tip = document.createElement('div');
  tip.className = 'hover-tooltip hidden';
  tip.innerHTML = '<div class="hover-tooltip-title"></div><div class="hover-tooltip-reason empty"></div>';
  document.body.appendChild(tip);
  hoverTooltipState.el = tip;
  return tip;
}

function showHoverTooltip(text, clientX, clientY, target = null, reason = '') {
  const message = String(text || '').trim();
  if (!message) {
    hideHoverTooltip();
    return;
  }
  const tip = ensureHoverTooltipElement();
  const titleEl = tip.querySelector('.hover-tooltip-title');
  const reasonEl = tip.querySelector('.hover-tooltip-reason');
  if (titleEl instanceof HTMLElement) {
    titleEl.textContent = message;
  } else {
    tip.textContent = message;
  }
  const normalizedReason = String(reason || '').trim();
  if (reasonEl instanceof HTMLElement) {
    if (normalizedReason) {
      reasonEl.textContent = normalizedReason;
      reasonEl.classList.remove('empty');
    } else {
      reasonEl.textContent = '';
      reasonEl.classList.add('empty');
    }
  }
  tip.classList.remove('hidden');
  hoverTooltipState.target = target;

  const offset = 14;
  const margin = 8;
  const tipWidth = Math.max(1, tip.offsetWidth || 160);
  const tipHeight = Math.max(1, tip.offsetHeight || 28);
  let anchorX = clientX;
  let anchorY = clientY;
  if (target instanceof Element) {
    const rect = target.getBoundingClientRect();
    anchorX = rect.left + (rect.width / 2);
    anchorY = rect.bottom;
  }
  let left = anchorX - (tipWidth / 2);
  let top = anchorY + offset;
  if (left < margin) left = margin;
  if (left + tipWidth + margin > window.innerWidth) {
    left = Math.max(margin, window.innerWidth - tipWidth - margin);
  }
  if (top + tipHeight + margin > window.innerHeight) {
    if (target instanceof Element) {
      const rect = target.getBoundingClientRect();
      top = Math.max(margin, rect.top - tipHeight - offset);
    } else {
      top = Math.max(margin, anchorY - tipHeight - offset);
    }
  }
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

function hideHoverTooltip() {
  const tip = hoverTooltipState.el;
  if (!(tip instanceof HTMLElement)) return;
  tip.classList.add('hidden');
  hoverTooltipState.target = null;
}

function migrateNativeTitleAttributes(root = document) {
  if (!(root instanceof Document || root instanceof Element)) return;
  const nodes = root.querySelectorAll('[title]');
  for (const node of nodes) {
    if (!(node instanceof HTMLElement)) continue;
    const nativeTitle = node.getAttribute('title');
    if (nativeTitle && !node.dataset.tooltip) {
      node.dataset.tooltip = nativeTitle;
    }
    node.removeAttribute('title');
  }
}

function describePortIssue(target) {
  if (!(target instanceof HTMLElement)) return '';
  const portType = target.dataset.portType || '';
  const portClass = target.dataset.portClass || '';
  const nodeId = target.dataset.nodeId || '';
  if (!portType || !portClass || !nodeId) return '';
  const node = state.blocks.find((it) => it.id === nodeId);
  if (!node) return '';

  const linkDiagnostics = buildLinkErrorDiagnostics();
  const checkLinkByIndex = (index) => {
    if (!linkDiagnostics.invalidIndexes.has(index)) return '';
    const reason = String(linkDiagnostics.reasonByIndex.get(index) || '').trim();
    return reason || '실행 불가 연결';
  };

  for (const index of getLinkIndexesForPort(nodeId, portType, portClass)) {
    const reason = checkLinkByIndex(index);
    if (reason) return `이유: ${reason}`;
  }

  if (state.connectionDraft && state.linkingActive && portType === 'in') {
    const fromId = state.connectionDraft.from || '';
    const fromNode = state.blocks.find((it) => it.id === fromId);
    if (!fromNode) return '';
    const kind = state.connectionDraft.kind || linkKindFromType(fromNode.type);
    if (fromId === nodeId) return '이유: 같은 노드에는 연결할 수 없음';
    if (!canLinkFromSourceToTarget(fromId, kind, node.type)) return '이유: 노드 타입이 호환되지 않음';
    if (!canLinkKindToPort(kind, node.type, portClass, fromId)) {
      if (portClass === 'in-cancel') return '이유: 취소 입력은 논리형만 연결 가능';
      return '이유: 포트 타입이 호환되지 않음';
    }
    if (!isRuntimeLinkCompatible(fromNode, node, kind, portClass)) {
      return '이유: 이 연결은 실행 코드에서 무시됨';
    }
  }

  return '';
}

function placeContextMenu(clientX, clientY, margin = 8) {
  const rect = el.workspace.getBoundingClientRect();
  el.contextMenu.classList.remove('hidden');
  el.contextMenu.style.visibility = 'hidden';
  el.contextMenu.style.left = '0px';
  el.contextMenu.style.top = '0px';

  const menuWidth = Math.max(1, el.contextMenu.offsetWidth || 236);
  const menuHeight = Math.max(1, el.contextMenu.offsetHeight || 180);
  const localX = clientX - rect.left;
  const localY = clientY - rect.top;
  const left = Math.max(margin, Math.min(rect.width - menuWidth - margin, localX));
  const top = Math.max(margin, Math.min(rect.height - menuHeight - margin, localY));

  el.contextMenu.style.left = `${left}px`;
  el.contextMenu.style.top = `${top}px`;
  el.contextMenu.style.visibility = '';
}

function showContextMenu(clientX, clientY, options = {}) {
  const worldPos = toWorldPosition(clientX, clientY);
  state.contextMenuWorld = worldPos;
  const connectFromId = options.connectFromId || null;
  const connectKind = options.connectKind || null;
  const connectPortClass = options.connectPortClass || 'out';
  const connectFromType = connectFromId
    ? (state.blocks.find((it) => it.id === connectFromId)?.type || '')
    : '';
  const extraActions = Array.isArray(options.extraActions) ? options.extraActions : [];

  el.contextMenu.innerHTML = '';
  const search = document.createElement('input');
  search.type = 'text';
  search.placeholder = '노드 검색...';
  search.className = 'context-search';
  el.contextMenu.appendChild(search);

  const body = document.createElement('div');
  el.contextMenu.appendChild(body);

  const allItems = connectFromId
    ? [
      ...NODE_LIBRARY.events.map((it) => ({ ...it, group: '이벤트', groupKey: 'event' })),
      ...NODE_LIBRARY.functions.map((it) => ({ ...it, group: '함수', groupKey: 'function' })),
      ...NODE_LIBRARY.operations.map((it) => ({ ...it, group: '연산', groupKey: 'operation' })),
      ...NODE_LIBRARY.variables.map((it) => ({ ...it, group: '변수', groupKey: 'variable' }))
    ].filter((it) => canLinkKindToType(connectKind || linkKindFromType(connectFromType), it.type))
    : [
      ...NODE_LIBRARY.events.map((it) => ({ ...it, group: '이벤트', groupKey: 'event' })),
      ...NODE_LIBRARY.functions.map((it) => ({ ...it, group: '함수', groupKey: 'function' })),
      ...NODE_LIBRARY.operations.map((it) => ({ ...it, group: '연산', groupKey: 'operation' })),
      ...NODE_LIBRARY.variables.map((it) => ({ ...it, group: '변수', groupKey: 'variable' }))
    ];

  const renderMenuItems = (keyword) => {
    const q = keyword.trim().toLowerCase();
    body.innerHTML = '';
    const filtered = !q ? allItems : allItems.filter((it) => {
      return it.title.toLowerCase().includes(q)
        || it.type.toLowerCase().includes(q)
        || it.group.toLowerCase().includes(q)
        || it.groupKey.toLowerCase().includes(q);
    });

    const groups = ['이벤트', '함수', '연산', '변수'];
    for (const group of groups) {
      const items = filtered.filter((it) => it.group === group);
      if (!items.length) continue;
      const title = document.createElement('div');
      title.className = 'context-title';
      title.textContent = group;
      body.appendChild(title);

      for (const item of items) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'context-item';
        button.dataset.groupKey = item.groupKey;
        button.textContent = item.title;
        button.addEventListener('click', () => {
          const created = createNode(item.type, state.contextMenuWorld.x, state.contextMenuWorld.y);
          if (connectFromId && created) {
            addLink(
              connectFromId,
              created.id,
              connectKind || linkKindFromType(connectFromType),
              connectPortClass,
              targetPortClassForLink(connectKind || linkKindFromType(connectFromType), created.type)
            );
            renderCanvas();
          }
          hideContextMenu();
        });
        body.appendChild(button);
      }
    }
    if (!filtered.length) {
      const empty = document.createElement('div');
      empty.className = 'context-title';
      empty.textContent = connectFromId ? '연결 가능한 노드가 없습니다.' : '검색 결과가 없습니다.';
      body.appendChild(empty);
    }

  };

  search.addEventListener('input', () => renderMenuItems(search.value));
  renderMenuItems('');

  if (extraActions.length) {
    const actionNodes = [];
    const divider = document.createElement('div');
    divider.className = 'context-title';
    divider.textContent = '그룹';
    actionNodes.push(divider);

    for (const action of extraActions) {
      if (!action || typeof action.label !== 'string' || typeof action.onClick !== 'function') continue;
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'context-item';
      button.textContent = action.label;
      button.addEventListener('click', () => {
        action.onClick();
        renderCanvas();
        scheduleCollaborativePublish(true);
        schedulePresencePublish(true);
        hideContextMenu();
      });
      actionNodes.push(button);
    }
    if (actionNodes.length) {
      const separator = document.createElement('div');
      separator.className = 'context-title';
      separator.textContent = '';
      actionNodes.push(separator);
      for (let i = actionNodes.length - 1; i >= 0; i -= 1) {
        body.prepend(actionNodes[i]);
      }
    }
  }

  placeContextMenu(clientX, clientY);
  search.focus();
}

function showNodeContextMenu(clientX, clientY, nodeId) {
  el.contextMenu.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'context-title';
  title.textContent = '노드';
  el.contextMenu.appendChild(title);

  const disconnectAllBtn = document.createElement('button');
  disconnectAllBtn.type = 'button';
  disconnectAllBtn.className = 'context-item';
  disconnectAllBtn.textContent = '모든 연결 끊기';
  disconnectAllBtn.addEventListener('click', () => {
    disconnectAllLinks(nodeId);
    hideContextMenu();
    renderCanvas();
  });
  el.contextMenu.appendChild(disconnectAllBtn);

  const copyBtn = document.createElement('button');
  copyBtn.type = 'button';
  copyBtn.className = 'context-item';
  copyBtn.textContent = '복사';
  copyBtn.addEventListener('click', () => {
    duplicateNode(nodeId);
    hideContextMenu();
    renderCanvas();
  });
  el.contextMenu.appendChild(copyBtn);

  const deleteBtn = document.createElement('button');
  deleteBtn.type = 'button';
  deleteBtn.className = 'context-item';
  deleteBtn.textContent = '삭제';
  deleteBtn.addEventListener('click', () => {
    removeNode(nodeId);
    hideContextMenu();
    renderCanvas();
  });
  el.contextMenu.appendChild(deleteBtn);

  const aiRiskMap = buildNodeAiRiskMap();
  if (aiRiskMap.has(nodeId)) {
    const toolsTitle = document.createElement('div');
    toolsTitle.className = 'context-title';
    toolsTitle.textContent = '도구';
    el.contextMenu.appendChild(toolsTitle);

    const aiFixBtn = document.createElement('button');
    aiFixBtn.type = 'button';
    aiFixBtn.className = 'context-item';
    aiFixBtn.textContent = '자동 수정 제안';
    aiFixBtn.addEventListener('click', async () => {
      hideContextMenu();
      showActionBar('AI 자동 수정 분석 중...', '', 0, { keepProgress: true });
      await waitForNextUiTick();
      applyAiAutoFixForNode(nodeId)
        .catch((error) => {
          showActionBar(`AI 자동 수정 실패: ${error?.message || 'unknown error'}`, 'error', 3200);
        })
        .finally(() => {
          renderCanvas();
        });
    });
    el.contextMenu.appendChild(aiFixBtn);
  }

  placeContextMenu(clientX, clientY);
}

function showCommentContextMenu(clientX, clientY, commentId) {
  el.contextMenu.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'context-title';
  title.textContent = '그룹';
  el.contextMenu.appendChild(title);

  const deleteBtn = document.createElement('button');
  deleteBtn.type = 'button';
  deleteBtn.className = 'context-item';
  deleteBtn.textContent = '삭제';
  deleteBtn.addEventListener('click', () => {
    removeComment(commentId);
    hideContextMenu();
    renderCanvas();
  });
  el.contextMenu.appendChild(deleteBtn);

  placeContextMenu(clientX, clientY);
}

function showPortContextMenu(clientX, clientY, nodeId, portType, portClass) {
  el.contextMenu.innerHTML = '';

  const title = document.createElement('div');
  title.className = 'context-title';
  title.textContent = '포트';
  el.contextMenu.appendChild(title);

  const disconnectBtn = document.createElement('button');
  disconnectBtn.type = 'button';
  disconnectBtn.className = 'context-item';
  disconnectBtn.textContent = '모든 연결 끊기';
  disconnectBtn.addEventListener('click', () => {
    disconnectLinksByPort(nodeId, portType, portClass);
    hideContextMenu();
    renderCanvas();
  });
  el.contextMenu.appendChild(disconnectBtn);

  const linkDiagnostics = buildLinkErrorDiagnostics();
  const connectedIndexes = getLinkIndexesForPort(nodeId, portType, portClass);
  const invalidConnectedIndexes = connectedIndexes.filter((index) => linkDiagnostics.invalidIndexes.has(index));
  if (invalidConnectedIndexes.length > 0) {
    const causeNodeId = invalidConnectedIndexes
      .map((index) => linkDiagnostics.causeNodeIdByIndex.get(index))
      .find((id) => typeof id === 'string' && id);
    if (causeNodeId) {
      const moveToCauseBtn = document.createElement('button');
      moveToCauseBtn.type = 'button';
      moveToCauseBtn.className = 'context-item';
      moveToCauseBtn.textContent = '원인 노드로 이동';
      moveToCauseBtn.addEventListener('click', () => {
        hideContextMenu();
        moveViewportToNode(causeNodeId);
      });
      el.contextMenu.appendChild(moveToCauseBtn);
    }
  }

  placeContextMenu(clientX, clientY);
}

function closePluginSelectMenu() {
  el.pluginSelectField.classList.remove('open');
  el.pluginSelectMenu.classList.add('hidden');
  el.pluginSelectTrigger.setAttribute('aria-expanded', 'false');
}

function closeWorkspaceTreeMenu() {
  el.workspaceTreeMenu.classList.add('hidden');
  state.workspaceMenuTargetId = null;
}

function showWorkspaceTreeMenu(clientX, clientY, targetId = null) {
  state.workspaceMenuTargetId = targetId;
  el.renameWorkspaceMenuBtn.classList.toggle('hidden', !targetId);
  el.deleteWorkspaceMenuBtn.classList.toggle('hidden', !targetId);
  const hostRect = el.workspaceHub.getBoundingClientRect();
  const menuWidth = 150;
  const menuHeight = targetId ? 188 : 122;
  const left = Math.max(6, Math.min(hostRect.width - menuWidth - 6, clientX - hostRect.left));
  const top = Math.max(6, Math.min(hostRect.height - menuHeight - 6, clientY - hostRect.top));
  el.workspaceTreeMenu.style.left = `${left}px`;
  el.workspaceTreeMenu.style.top = `${top}px`;
  el.workspaceTreeMenu.classList.remove('hidden');
}

function requestNameDialog(title, placeholder, initialValue = '') {
  return new Promise((resolve) => {
    el.nameDialogTitle.textContent = title;
    el.nameDialogInput.value = initialValue;
    el.nameDialogInput.placeholder = placeholder;
    el.nameDialog.classList.remove('hidden');
    el.nameDialogInput.focus();
    el.nameDialogInput.select();

    let closed = false;
    const done = (value) => {
      if (closed) return;
      closed = true;
      el.nameDialog.classList.add('hidden');
      el.nameDialogCancelBtn.removeEventListener('click', onCancel);
      el.nameDialogConfirmBtn.removeEventListener('click', onConfirm);
      el.nameDialog.removeEventListener('mousedown', onBackdrop);
      el.nameDialogInput.removeEventListener('keydown', onKeyDown);
      resolve(value);
    };
    const onCancel = () => done(null);
    const onConfirm = () => done(el.nameDialogInput.value.trim() || null);
    const onBackdrop = (event) => {
      if (event.target === el.nameDialog) done(null);
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') done(null);
      if (event.key === 'Enter') done(el.nameDialogInput.value.trim() || null);
    };

    el.nameDialogCancelBtn.addEventListener('click', onCancel);
    el.nameDialogConfirmBtn.addEventListener('click', onConfirm);
    el.nameDialog.addEventListener('mousedown', onBackdrop);
    el.nameDialogInput.addEventListener('keydown', onKeyDown);
  });
}

function requestConfirmDialog(title, message, confirmLabel = '확인') {
  return new Promise((resolve) => {
    el.confirmDialogTitle.textContent = title;
    el.confirmDialogMessage.textContent = message;
    el.confirmDialogOkBtn.textContent = confirmLabel;
    el.confirmDialog.classList.remove('hidden');
    el.confirmDialogOkBtn.focus();

    let closed = false;
    const done = (value) => {
      if (closed) return;
      closed = true;
      el.confirmDialog.classList.add('hidden');
      el.confirmDialogCancelBtn.removeEventListener('click', onCancel);
      el.confirmDialogOkBtn.removeEventListener('click', onConfirm);
      el.confirmDialog.removeEventListener('mousedown', onBackdrop);
      document.removeEventListener('keydown', onKeyDown);
      resolve(value);
    };
    const onCancel = () => done(false);
    const onConfirm = () => done(true);
    const onBackdrop = (event) => {
      if (event.target === el.confirmDialog) done(false);
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') done(false);
      if (event.key === 'Enter') done(true);
    };

    el.confirmDialogCancelBtn.addEventListener('click', onCancel);
    el.confirmDialogOkBtn.addEventListener('click', onConfirm);
    el.confirmDialog.addEventListener('mousedown', onBackdrop);
    document.addEventListener('keydown', onKeyDown);
  });
}

function setAutoCreateAuthStatus(message, kind = '') {
  el.autoCreateAuthStatus.textContent = message;
  el.autoCreateAuthStatus.classList.remove('ok', 'error');
  if (kind === 'ok' || kind === 'error') {
    el.autoCreateAuthStatus.classList.add(kind);
  }
}

function refreshAutoCreateGenerateEnabled() {
  const ready = (localAiRuntimeState.ready || localAiRuntimeState.loading)
    && !autoCreateState.generateInFlight
    && el.autoCreatePromptInput.value.trim().length > 0;
  el.autoCreateGenerateBtn.disabled = !ready;
}

function resizeAutoCreatePromptInput() {
  const input = el.autoCreatePromptInput;
  if (!(input instanceof HTMLTextAreaElement)) return;
  const card = input.closest('.auto-create-dialog-card');
  if (!(card instanceof HTMLElement)) return;
  const minHeight = 112;
  const viewportGap = 20;
  const maxCardHeight = Math.max(240, Math.floor(window.innerHeight - (viewportGap * 2)));

  // Measure non-textarea area using minimum textarea height to keep sizing linear by viewport px.
  input.style.height = `${minHeight}px`;
  input.style.overflowY = 'hidden';
  const baseCardHeight = card.scrollHeight;
  const nonTextareaHeight = Math.max(0, baseCardHeight - minHeight);

  input.style.height = 'auto';
  const desiredHeight = Math.max(minHeight, input.scrollHeight);
  const maxHeight = Math.max(minHeight, maxCardHeight - nonTextareaHeight);
  const nextHeight = Math.min(maxHeight, desiredHeight);
  input.style.height = `${Math.round(nextHeight)}px`;
  input.style.overflowY = input.scrollHeight > maxHeight ? 'auto' : 'hidden';
}

function closeAutoCreateDialog(resetGenerateState = true) {
  el.autoCreateDialog.classList.add('hidden');
  if (resetGenerateState) {
    autoCreateState.targetWorkspaceId = null;
    autoCreateState.targetOriginWorld = null;
  }
  if (resetGenerateState) autoCreateState.generateInFlight = false;
  refreshAutoCreateGenerateEnabled();
}

function openAutoCreateDialog(targetWorkspaceId = null, targetOriginWorld = null) {
  autoCreateState.targetWorkspaceId = typeof targetWorkspaceId === 'string' ? targetWorkspaceId : state.currentWorkspaceId;
  autoCreateState.targetOriginWorld = targetOriginWorld && Number.isFinite(targetOriginWorld.x) && Number.isFinite(targetOriginWorld.y)
    ? { x: targetOriginWorld.x, y: targetOriginWorld.y }
    : null;
  el.autoCreateDialog.classList.remove('hidden');
  if (localAiRuntimeState.ready) {
    setAutoCreateAuthStatus('로컬 AI 준비 완료', 'ok');
  } else {
    setAutoCreateAuthStatus('로컬 AI 준비 중...');
  }
  refreshAutoCreateGenerateEnabled();
  resizeAutoCreatePromptInput();
  el.autoCreatePromptInput.focus();
}

function extractJsonObjectFromText(text) {
  if (!text) return null;
  const fenced = text.match(/```json\s*([\s\S]*?)```/i) || text.match(/```\s*([\s\S]*?)```/i);
  const candidate = fenced ? fenced[1].trim() : text.trim();
  try {
    return JSON.parse(candidate);
  } catch {
    // continue
  }
  const start = candidate.indexOf('{');
  const end = candidate.lastIndexOf('}');
  if (start < 0 || end <= start) return null;
  const sliced = candidate.slice(start, end + 1);
  try {
    return JSON.parse(sliced);
  } catch {
    return null;
  }
}

function rectsOverlap(a, b, gap = 0) {
  return !(
    (a.x + a.width + gap) <= b.x
    || (b.x + b.width + gap) <= a.x
    || (a.y + a.height + gap) <= b.y
    || (b.y + b.height + gap) <= a.y
  );
}

function estimateNodeHeight(node) {
  if (node.type === 'VAR_TEXT') {
    const value = String(node.params?.value || '');
    const lines = Math.max(1, value.split('\n').length);
    return Math.max(132, 112 + (lines * 18));
  }
  if (node.type === 'FUNCTION_SEND_MESSAGE' || node.type === 'FUNCTION_BROADCAST_MESSAGE') return 150;
  if (node.type === 'EVENT_ON_PLAYER_JOIN') return 144;
  if (node.type === 'EVENT_ON_PLAYER_CHAT') return 168;
  if (node.type === 'EVENT_ON_ENABLE') return 134;
  if (node.type === 'MATH_ADD' || node.type === 'MATH_SUB' || node.type === 'MATH_MUL' || node.type === 'MATH_DIV') return 136;
  return BLOCK_NODE_ESTIMATED_HEIGHT;
}

function measureNodeHeightFromDom(nodeId) {
  const nodeEl = el.canvas?.querySelector(`.block-node[data-id="${nodeId}"]`);
  if (!(nodeEl instanceof HTMLElement)) return null;
  const h = nodeEl.getBoundingClientRect().height;
  return Number.isFinite(h) && h > 0 ? Math.round(h) : null;
}

function nodeRectOf(node) {
  const measuredSize = measureNodeSizeInWorld(node.id);
  const measuredHeight = measureNodeHeightFromDom(node.id);
  return {
    x: node.x,
    y: node.y,
    width: Number.isFinite(measuredSize?.width) && measuredSize.width > 0 ? measuredSize.width : BLOCK_NODE_WIDTH,
    height: measuredHeight || (Number.isFinite(measuredSize?.height) && measuredSize.height > 0 ? measuredSize.height : estimateNodeHeight(node))
  };
}

function measureGroupHeaderHeightFromDom(groupId) {
  const groupEl = el.canvas?.querySelector(`.comment-box[data-id="${groupId}"]`);
  if (!(groupEl instanceof HTMLElement)) return null;
  const headEl = groupEl.querySelector('.comment-head');
  if (!(headEl instanceof HTMLElement)) return null;
  const h = headEl.getBoundingClientRect().height;
  return Number.isFinite(h) && h > 0 ? Math.round(h) : null;
}

function buildSearchOffsets(step = GRID_SIZE, rings = 14) {
  const offsets = [{ x: 0, y: 0 }];
  for (let r = 1; r <= rings; r += 1) {
    const d = r * step;
    for (let x = -d; x <= d; x += step) {
      offsets.push({ x, y: -d });
      offsets.push({ x, y: d });
    }
    for (let y = -d + step; y <= d - step; y += step) {
      offsets.push({ x: -d, y });
      offsets.push({ x: d, y });
    }
  }
  return offsets;
}

function autoArrangeCreatedNodes(session) {
  const createdNodes = state.blocks.filter((it) => session.createdLocalNodeIds.has(it.id));
  if (!createdNodes.length) return;
  const fixedNodes = state.blocks.filter((it) => !session.createdLocalNodeIds.has(it.id));
  const fixedRects = fixedNodes.map((it) => nodeRectOf(it));
  const placedCreated = [];
  const offsets = buildSearchOffsets(GRID_SIZE, 16);
  const minGap = 14;

  createdNodes.sort((a, b) => (a.y - b.y) || (a.x - b.x));
  for (const node of createdNodes) {
    const baseX = node.x;
    const baseY = node.y;
    const nodeHeight = nodeRectOf(node).height;
    let chosenX = baseX;
    let chosenY = baseY;
    for (const off of offsets) {
      const candidate = {
        x: Math.round(baseX + off.x),
        y: Math.round(baseY + off.y),
        width: BLOCK_NODE_WIDTH,
        height: nodeHeight
      };
      let collided = false;
      for (const rect of fixedRects) {
        if (rectsOverlap(candidate, rect, minGap)) {
          collided = true;
          break;
        }
      }
      if (!collided) {
        for (const rect of placedCreated) {
          if (rectsOverlap(candidate, rect, minGap)) {
            collided = true;
            break;
          }
        }
      }
      if (!collided) {
        chosenX = candidate.x;
        chosenY = candidate.y;
        placedCreated.push(candidate);
        break;
      }
    }
    node.x = chosenX;
    node.y = chosenY;
  }
}

function findAvailableNodePosition(type, x, y, paramsOverride = null, minGap = 14) {
  const baseX = snapGrid(x);
  const baseY = snapGrid(y);
  const probeParams = buildDefaultNodeParams(type);
  if (paramsOverride && typeof paramsOverride === 'object') {
    Object.assign(probeParams, paramsOverride);
  }
  const probeNode = { id: '_probe', type, x: baseX, y: baseY, params: probeParams };
  const height = estimateNodeHeight(probeNode);
  const existingRects = state.blocks.map((it) => nodeRectOf(it));
  const overlapArea = (a, b, gap = 0) => {
    const ax1 = a.x - gap;
    const ay1 = a.y - gap;
    const ax2 = a.x + a.width + gap;
    const ay2 = a.y + a.height + gap;
    const bx1 = b.x;
    const by1 = b.y;
    const bx2 = b.x + b.width;
    const by2 = b.y + b.height;
    const w = Math.max(0, Math.min(ax2, bx2) - Math.max(ax1, bx1));
    const h = Math.max(0, Math.min(ay2, by2) - Math.max(ay1, by1));
    return w * h;
  };
  const overlapSum = (candidate) => {
    let sum = 0;
    for (const rect of existingRects) {
      sum += overlapArea(candidate, rect, minGap);
    }
    return sum;
  };
  const candidateScore = (candidate) => {
    const overlap = overlapSum(candidate);
    const dx = Math.abs(candidate.x - baseX);
    const dy = Math.abs(candidate.y - baseY);
    // Stable and readable layout: no overlap first, then shorter move, then axis-aligned placements.
    const upperPenalty = candidate.y < baseY ? 180 : 0;
    const leftPenalty = candidate.x < baseX ? 140 : 0;
    const axisPenalty = (candidate.x !== baseX && candidate.y !== baseY) ? 56 : 0;
    return (overlap * 100000000) + (dx + dy) + upperPenalty + leftPenalty + axisPenalty;
  };
  const origin = { x: baseX, y: baseY, width: BLOCK_NODE_WIDTH, height };
  if (overlapSum(origin) <= 0) return { x: baseX, y: baseY };

  // 1) Prefer nearby placement first (grid-step search around origin).
  const nearOffsets = buildSearchOffsets(GRID_SIZE, 12);
  let nearBest = null;
  let nearBestScore = Number.POSITIVE_INFINITY;
  let nearBestClean = null;
  let nearBestCleanScore = Number.POSITIVE_INFINITY;
  const betterTie = (a, b) => (a.y - b.y) || (a.x - b.x);
  for (const off of nearOffsets) {
    const candidate = {
      x: snapGrid(baseX + off.x),
      y: snapGrid(baseY + off.y),
      width: BLOCK_NODE_WIDTH,
      height
    };
    const score = candidateScore(candidate);
    if (score < nearBestScore || (score === nearBestScore && nearBest && betterTie(candidate, nearBest) < 0)) {
      nearBest = candidate;
      nearBestScore = score;
    }
    if (overlapSum(candidate) <= 0) {
      if (score < nearBestCleanScore || (score === nearBestCleanScore && nearBestClean && betterTie(candidate, nearBestClean) < 0)) {
        nearBestClean = candidate;
        nearBestCleanScore = score;
      }
    }
  }
  if (nearBestClean) return { x: nearBestClean.x, y: nearBestClean.y };

  // Radial placement around origin: left/right/up/down + diagonal expansion.
  const stepX = BLOCK_NODE_WIDTH + minGap + GRID_SIZE;
  const stepY = height + minGap + GRID_SIZE;
  let bestAny = null;
  let bestAnyScore = Number.POSITIVE_INFINITY;
  let bestClean = null;
  let bestCleanScore = Number.POSITIVE_INFINITY;
  const consider = (candidate) => {
    const overlap = overlapSum(candidate);
    const score = candidateScore(candidate);
    if (score < bestAnyScore || (score === bestAnyScore && bestAny && betterTie(candidate, bestAny) < 0)) {
      bestAny = candidate;
      bestAnyScore = score;
    }
    if (overlap <= 0) {
      if (score < bestCleanScore || (score === bestCleanScore && bestClean && betterTie(candidate, bestClean) < 0)) {
        bestClean = candidate;
        bestCleanScore = score;
      }
    }
  };
  for (let ring = 1; ring <= 120; ring += 1) {
    const points = [
      { x: baseX + (ring * stepX), y: baseY },
      { x: baseX - (ring * stepX), y: baseY },
      { x: baseX, y: baseY + (ring * stepY) },
      { x: baseX, y: baseY - (ring * stepY) },
      { x: baseX + (ring * stepX), y: baseY + (ring * stepY) },
      { x: baseX - (ring * stepX), y: baseY + (ring * stepY) },
      { x: baseX + (ring * stepX), y: baseY - (ring * stepY) },
      { x: baseX - (ring * stepX), y: baseY - (ring * stepY) }
    ];
    for (const p of points) {
      const candidate = {
        x: snapGrid(p.x),
        y: snapGrid(p.y),
        width: BLOCK_NODE_WIDTH,
        height
      };
      consider(candidate);
    }
  }
  if (bestClean) return { x: bestClean.x, y: bestClean.y };
  if (nearBest) return { x: nearBest.x, y: nearBest.y };
  if (bestAny) return { x: bestAny.x, y: bestAny.y };
  // Extremely dense fallback.
  return { x: snapGrid(baseX + (stepX * 8)), y: snapGrid(baseY + stepY) };
}

function createNodeAvoidingOverlap(type, x, y, paramsOverride = null) {
  const pos = findAvailableNodePosition(type, x, y, paramsOverride);
  return createNode(type, pos.x, pos.y, paramsOverride);
}

function autoArrangeCreatedGroups(session) {
  const createdGroups = state.comments.filter((it) => session.createdLocalGroupIds.has(it.id));
  if (!createdGroups.length) return;

  const createdNodes = state.blocks.filter((it) => session.createdLocalNodeIds.has(it.id));
  const contentPadding = 88;
  const groupPadding = { left: contentPadding, right: contentPadding, bottom: contentPadding };
  const minWidth = 280;
  const minHeight = 170;

  const estimateGroupHeaderHeight = (group) => {
    const title = String(group?.title || '');
    const description = String(group?.description || '');
    const titleLines = Math.max(1, Math.ceil(title.length / 28));
    const descriptionLines = Math.max(1, description ? description.split('\n').length : 1);
    const base = 44; // paddings + border + default controls
    const titleHeight = titleLines * 18;
    const descriptionHeight = Math.max(34, descriptionLines * 16);
    return base + titleHeight + descriptionHeight + 18;
  };

  for (const group of createdGroups) {
    const remoteGroupId = session.localGroupToRemoteId.get(group.id) || '';
    const explicitMembers = createdNodes.filter((node) => {
      const intendedGroup = session.nodeGroupIntentByLocalNodeId.get(node.id);
      if (remoteGroupId && intendedGroup === remoteGroupId) return true;
      const remoteNodeId = session.localNodeToRemoteId.get(node.id);
      const set = remoteGroupId ? session.groupMemberRemoteNodeIds.get(remoteGroupId) : null;
      return !!(remoteNodeId && set && set.has(remoteNodeId));
    });

    const nodesByCenterInGroup = createdNodes.filter((node) => {
      const rect = nodeRectOf(node);
      const cx = rect.x + (rect.width / 2);
      const cy = rect.y + (rect.height / 2);
      return cx >= group.x && cx <= (group.x + group.width) && cy >= group.y && cy <= (group.y + group.height);
    });
    const nodesInGroup = explicitMembers.length ? explicitMembers : nodesByCenterInGroup;
    if (!nodesInGroup.length) continue;

    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    for (const node of nodesInGroup) {
      const rect = nodeRectOf(node);
      minX = Math.min(minX, rect.x);
      minY = Math.min(minY, rect.y);
      maxX = Math.max(maxX, rect.x + rect.width);
      maxY = Math.max(maxY, rect.y + rect.height);
    }
    const measuredHeaderHeight = measureGroupHeaderHeightFromDom(group.id);
    const headerHeight = measuredHeaderHeight || estimateGroupHeaderHeight(group);
    const topPadding = headerHeight + contentPadding;
    const desiredX = Math.round(minX - groupPadding.left);
    const desiredY = Math.round(minY - topPadding);
    const desiredWidth = Math.max(minWidth, Math.round((maxX - minX) + groupPadding.left + groupPadding.right));
    const desiredHeight = Math.max(minHeight, Math.round((maxY - minY) + topPadding + groupPadding.bottom));

    // Keep AI intent, but never allow header/top intrusion or tight edges.
    group.x = Math.min(group.x, desiredX);
    group.y = Math.min(group.y, desiredY);
    group.width = desiredWidth;
    group.height = desiredHeight;
  }
}

function autoArrangeCreatedContent(session) {
  if (!session) return;
  // Render once so layout-dependent node heights (textarea, dynamic ports) are measurable.
  renderCanvas();
  autoArrangeCreatedNodes(session);
  renderCanvas();
  autoArrangeCreatedGroups(session);
}

function portSchemaForNodeType(type) {
  if (type === 'EVENT_ON_ENABLE') {
    return {
      inputs: [],
      outputs: [
        { portClass: 'out-exec', linkKind: 'exec' },
        { portClass: 'out-context', linkKind: 'context' }
      ]
    };
  }
  if (type === 'EVENT_ON_PLAYER_JOIN') {
    return {
      inputs: [{ portClass: 'in-data', accept: 'data' }],
      outputs: [
        { portClass: 'out-exec', linkKind: 'exec' },
        { portClass: 'out-player', linkKind: 'player' },
        { portClass: 'out-data-text', linkKind: 'data-text' }
      ]
    };
  }
  if (type === 'EVENT_ON_PLAYER_CHAT') {
    return {
      inputs: [
        { portClass: 'in-data', accept: 'data' },
        { portClass: 'in-cancel', accept: 'data-bool' }
      ],
      outputs: [
        { portClass: 'out-exec', linkKind: 'exec' },
        { portClass: 'out-player', linkKind: 'player' },
        { portClass: 'out-data-text', linkKind: 'data-text' }
      ]
    };
  }
  if (type === 'FUNCTION_SEND_MESSAGE') {
    return {
      inputs: [
        { portClass: 'in-exec', accept: 'exec' },
        { portClass: 'in-player', accept: 'player' },
        { portClass: 'in-data', accept: 'data' }
      ],
      outputs: [{ portClass: 'out-exec', linkKind: 'exec' }]
    };
  }
  if (type === 'FUNCTION_BROADCAST_MESSAGE') {
    return {
      inputs: [
        { portClass: 'in-exec', accept: 'exec' },
        { portClass: 'in-data', accept: 'data' }
      ],
      outputs: [{ portClass: 'out-exec', linkKind: 'exec' }]
    };
  }
  if (type === 'MATH_ADD') {
    return {
      inputs: [
        { portClass: 'in-data-a', accept: 'data' },
        { portClass: 'in-data-b', accept: 'data' }
      ],
      outputs: [{ portClass: 'out-data-any', linkKind: 'data-any' }]
    };
  }
  if (type === 'MATH_SUB') {
    return {
      inputs: [
        { portClass: 'in-data-a', accept: 'data-number' },
        { portClass: 'in-data-b', accept: 'data-number' }
      ],
      outputs: [{ portClass: 'out-data-number', linkKind: 'data-number' }]
    };
  }
  if (type === 'MATH_MUL') {
    return {
      inputs: [
        { portClass: 'in-data-a', accept: 'data-number' },
        { portClass: 'in-data-b', accept: 'data-number' }
      ],
      outputs: [{ portClass: 'out-data-number', linkKind: 'data-number' }]
    };
  }
  if (type === 'MATH_DIV') {
    return {
      inputs: [
        { portClass: 'in-data-a', accept: 'data-number' },
        { portClass: 'in-data-b', accept: 'data-number' }
      ],
      outputs: [{ portClass: 'out-data-number', linkKind: 'data-number' }]
    };
  }
  if (type === 'VAR_TEXT') {
    return { inputs: [], outputs: [{ portClass: 'out-data-text', linkKind: 'data-text' }] };
  }
  if (type === 'VAR_INTEGER') {
    return { inputs: [], outputs: [{ portClass: 'out-data-int', linkKind: 'data-int' }] };
  }
  if (type === 'VAR_DECIMAL') {
    return { inputs: [], outputs: [{ portClass: 'out-data-decimal', linkKind: 'data-decimal' }] };
  }
  if (type === 'VAR_BOOLEAN') {
    return { inputs: [], outputs: [{ portClass: 'out-data-bool', linkKind: 'data-bool' }] };
  }
  return { inputs: [], outputs: [] };
}

function inputPortOrderText(type) {
  const schema = portSchemaForNodeType(type);
  const inputs = Array.isArray(schema?.inputs) ? schema.inputs : [];
  if (!inputs.length) return '없음';
  return inputs
    .map((port, index) => `${index + 1}:${String(port?.portClass || '')}`)
    .join(' -> ');
}

function buildExistingAutoFixContext(targetNodeId, maxNodes = 48, maxLinks = 96) {
  const target = state.blocks.find((node) => node.id === targetNodeId) || null;
  const adjacency = new Map();
  for (const link of state.links) {
    const a = String(link.from || '');
    const b = String(link.to || '');
    if (!a || !b) continue;
    if (!adjacency.has(a)) adjacency.set(a, new Set());
    if (!adjacency.has(b)) adjacency.set(b, new Set());
    adjacency.get(a).add(b);
    adjacency.get(b).add(a);
  }

  const selectedNodeIds = new Set();
  const queue = [{ id: String(targetNodeId || ''), depth: 0 }];
  while (queue.length) {
    const item = queue.shift();
    if (!item?.id || selectedNodeIds.has(item.id)) continue;
    selectedNodeIds.add(item.id);
    if (item.depth >= 2) continue;
    const neighbors = adjacency.get(item.id);
    if (!neighbors) continue;
    for (const nextId of neighbors) {
      if (selectedNodeIds.has(nextId)) continue;
      queue.push({ id: nextId, depth: item.depth + 1 });
    }
  }

  const scored = state.blocks.map((node) => ({
    node,
    inNeighborhood: selectedNodeIds.has(node.id) ? 1 : 0,
    dist: target ? nodeDistanceSq(node, target) : 0
  }));
  scored.sort((a, b) => {
    if (a.inNeighborhood !== b.inNeighborhood) return b.inNeighborhood - a.inNeighborhood;
    return a.dist - b.dist;
  });

  const selectedNodes = scored.slice(0, maxNodes).map((item) => item.node);
  const selectedIds = new Set(selectedNodes.map((node) => node.id));
  const nodes = selectedNodes.map((node) => ({
    id: node.id,
    t: node.type,
    x: Math.round(node.x),
    y: Math.round(node.y),
    params: (() => {
      const src = node.params && typeof node.params === 'object' ? node.params : {};
      const entries = Object.entries(src).slice(0, 2);
      const out = {};
      for (const [k, v] of entries) out[k] = String(v ?? '').slice(0, 48);
      return out;
    })(),
    in: portSchemaForNodeType(node.type).inputs.map((p) => p.portClass),
    out: portSchemaForNodeType(node.type).outputs.map((p) => p.portClass)
  }));
  const links = [];
  for (const link of state.links) {
    if (links.length >= maxLinks) break;
    if (!selectedIds.has(link.from) || !selectedIds.has(link.to)) continue;
    links.push({
      f: link.from,
      t: link.to,
      k: String(link.kind || ''),
      fp: String(link.fromPortClass || ''),
      tp: String(link.toPortClass || '')
    });
  }
  return {
    targetNodeId: String(targetNodeId || ''),
    nodes,
    links
  };
}

function normalizeSolutionLines(text) {
  return String(text || '')
    .replace(/\r/g, '')
    .split('\n')
    .map((line) => line.replace(/^\s*[-*]\s*/, '').trim())
    .filter(Boolean)
    .slice(0, 8);
}

function fallbackGenerationSpec(promptText) {
  const text = String(promptText || '');
  const lower = text.toLowerCase();
  const wantsChat = /채팅|chat/.test(text);
  const wantsJoin = /접속|join/.test(text);
  const wantsBroadcast = /브로드캐스트|broadcast|전체/.test(text);
  const wantsMath = /계산|수학|더하|빼|곱|나누|\+|-|×|÷|\//.test(text);
  const cancelTrue = /취소.*(참|true|yes)|막아|차단/.test(lower);
  const cancelFalse = /취소.*(거짓|false|no)|허용/.test(lower);
  return {
    intent: wantsChat ? 'chat' : (wantsJoin ? 'join' : 'enable'),
    useBroadcast: wantsBroadcast || !wantsChat,
    useEventMessage: wantsChat,
    useMath: wantsMath,
    cancel: cancelTrue ? true : (cancelFalse ? false : null),
    message: text.trim().slice(0, 80) || 'Hello from AI'
  };
}

async function inferLocalGenerationSpec(promptText) {
  const fallback = fallbackGenerationSpec(promptText);
  try {
    const renderedAabb = buildRenderedAabbSnapshot(40).map((it) => ({
      id: it.id,
      t: it.type,
      x: it.x,
      y: it.y,
      w: it.width,
      h: it.height
    }));
    const prompt = [
      'You are a block-editor generation planner.',
      'Return JSON only.',
      'intent: one of chat, join, enable.',
      'useBroadcast/useEventMessage/useMath: boolean.',
      'cancel: true, false, or null.',
      'message: short string.',
      'Format:',
      '{"intent":"chat","useBroadcast":false,"useEventMessage":true,"useMath":false,"cancel":null,"message":"..."}',
      `Rendered node AABB (JSON): ${JSON.stringify(renderedAabb)}`,
      `Request: ${promptText}`
    ].join('\n');
    const generated = await generateWithLocalModel(prompt, 120);
    const parsed = extractJsonObjectFromText(generated) || {};
    const intent = ['chat', 'join', 'enable'].includes(String(parsed.intent || '')) ? parsed.intent : fallback.intent;
    const useBroadcast = typeof parsed.useBroadcast === 'boolean' ? parsed.useBroadcast : fallback.useBroadcast;
    const useEventMessage = typeof parsed.useEventMessage === 'boolean' ? parsed.useEventMessage : fallback.useEventMessage;
    const useMath = typeof parsed.useMath === 'boolean' ? parsed.useMath : fallback.useMath;
    const cancel = (parsed.cancel === true || parsed.cancel === false || parsed.cancel === null) ? parsed.cancel : fallback.cancel;
    const message = String(parsed.message || fallback.message || 'Hello from AI').slice(0, 120);
    const spec = { intent, useBroadcast, useEventMessage, useMath, cancel, message };
    return spec;
  } catch (error) {
    return fallback;
  }
}

function resolveAutoCreateOriginWorld() {
  if (autoCreateState.targetOriginWorld && Number.isFinite(autoCreateState.targetOriginWorld.x) && Number.isFinite(autoCreateState.targetOriginWorld.y)) {
    return { x: autoCreateState.targetOriginWorld.x, y: autoCreateState.targetOriginWorld.y };
  }
  const centerX = ((el.workspace?.clientWidth || 0) * 0.45 - state.panX) / Math.max(0.0001, state.scale);
  const centerY = ((el.workspace?.clientHeight || 0) * 0.4 - state.panY) / Math.max(0.0001, state.scale);
  return { x: centerX, y: centerY };
}

function applyLocalGeneratedFlow(spec) {
  const origin = resolveAutoCreateOriginWorld();
  const ox = snapGrid(origin.x);
  const oy = snapGrid(origin.y);
  const createdNodes = [];
  const beforeNodeCount = state.blocks.length;
  const beforeLinkCount = state.links.length;

  const eventType = spec.intent === 'chat'
    ? 'EVENT_ON_PLAYER_CHAT'
    : (spec.intent === 'join' ? 'EVENT_ON_PLAYER_JOIN' : 'EVENT_ON_ENABLE');
  const functionType = (spec.useBroadcast || eventType === 'EVENT_ON_ENABLE')
    ? 'FUNCTION_BROADCAST_MESSAGE'
    : 'FUNCTION_SEND_MESSAGE';
  const eventNode = createNodeAvoidingOverlap(eventType, ox, oy);
  if (!eventNode) return { nodeCount: 0, linkCount: 0 };
  createdNodes.push(eventNode);

  const fnNode = createNodeAvoidingOverlap(functionType, ox + 360, oy);
  if (!fnNode) return { nodeCount: 0, linkCount: 0 };
  createdNodes.push(fnNode);
  addLink(eventNode.id, fnNode.id, 'exec', 'out-exec', 'in-exec');

  if (functionType === 'FUNCTION_SEND_MESSAGE') {
    addLink(eventNode.id, fnNode.id, 'player', 'out-player', 'in-player');
  }

  if (spec.useMath) {
    const a = createNodeAvoidingOverlap('VAR_INTEGER', ox + 60, oy + 126, { value: '1' });
    const b = createNodeAvoidingOverlap('VAR_INTEGER', ox + 60, oy + 198, { value: '2' });
    const op = createNodeAvoidingOverlap('MATH_ADD', ox + 210, oy + 162);
    if (a && b && op) {
      createdNodes.push(a, b, op);
      addLink(a.id, op.id, 'data-int', 'out-data-int', 'in-data-a');
      addLink(b.id, op.id, 'data-int', 'out-data-int', 'in-data-b');
      addLink(op.id, fnNode.id, 'data-any', 'out-data-any', 'in-data');
    }
  } else {
    // Prefer explicit text variable over direct message->message wiring for readability.
    const msgValue = String(spec.message || 'Hello from AI');
    const msg = createNodeAvoidingOverlap('VAR_TEXT', ox + 60, oy + 144, { value: msgValue });
    if (msg) {
      createdNodes.push(msg);
      addLink(msg.id, fnNode.id, 'data-text', 'out-data-text', 'in-data');
    }
  }

  if (eventType === 'EVENT_ON_PLAYER_CHAT' && (spec.cancel === true || spec.cancel === false)) {
    const cancelNode = createNodeAvoidingOverlap('VAR_BOOLEAN', ox + 48, oy - 96, { value: spec.cancel ? 'true' : 'false' });
    if (cancelNode) {
      createdNodes.push(cancelNode);
      addLink(cancelNode.id, eventNode.id, 'data-bool', 'out-data-bool', 'in-cancel');
    }
  }

  const nodeCount = Math.max(0, state.blocks.length - beforeNodeCount);
  const linkCount = Math.max(0, state.links.length - beforeLinkCount);
  if (createdNodes.length > 0) {
    // Ensure generated changes are snapshotted/persisted/realtime-published as one final state.
    commitHistoryState();
    scheduleWorkspacePersist(true);
    scheduleCollaborativePublish(true);
    schedulePresencePublish(true);
  }
  return { nodeCount, linkCount };
}

async function runAutoCreateFromDialog() {
  const promptText = el.autoCreatePromptInput.value.trim();
  if (!promptText) {
    setAutoCreateAuthStatus('설명을 입력하세요.', 'error');
    return;
  }

  autoCreateState.generateInFlight = true;
  refreshAutoCreateGenerateEnabled();
  actionBarState.logs = [];
  renderActionBarLogs();
  if (!aiModelState.prefetchDone) {
    await ensureAiModelAssets({ silent: false });
  }
  showActionBar('AI 생성/수정을 준비 중입니다...', '', 0);
  try {
    const spec = await inferLocalGenerationSpec(promptText);
    const created = applyLocalGeneratedFlow(spec);
    if (!created.nodeCount && !created.linkCount) {
      throw new Error('생성 결과가 비어 있습니다.');
    }
    const localSummary = `노드 ${created.nodeCount}개, 연결 ${created.linkCount}개 생성`;
    actionBarState.logs = [{ message: `- ${localSummary}`, kind: '' }];
    renderActionBarLogs();
    setActionBarExpanded(true);
    setAutoCreateAuthStatus('로컬 생성 완료', 'ok');
    showActionBar(`AI 생성 적용 완료: ${localSummary}`, 'ok', 2600);
    setResult({
      ok: true,
      kind: 'local_generation',
      prompt: promptText,
      created
    });
    closeAutoCreateDialog(false);
  } catch (error) {
    const message = error?.message || 'unknown error';
    setAutoCreateAuthStatus(`AI 생성 실패: ${message}`, 'error');
    showActionBar(`AI 생성 실패: ${message}`, 'error', 3800);
  } finally {
    autoCreateState.generateInFlight = false;
    refreshAutoCreateGenerateEnabled();
  }
}

function syncPluginSelectLabel() {
  const selected = el.pluginSelect.options[el.pluginSelect.selectedIndex];
  el.pluginSelectLabel.textContent = selected?.textContent || '플러그인 선택';
}

function rebuildPluginSelectMenu() {
  el.pluginSelectMenu.innerHTML = '';
  const options = Array.from(el.pluginSelect.options);
  for (const option of options) {
    const item = document.createElement('button');
    item.type = 'button';
    item.className = 'plugin-select-option';
    if (option.value === el.pluginSelect.value) item.classList.add('active');
    item.textContent = option.textContent || '';
    item.addEventListener('click', () => {
      el.pluginSelect.value = option.value;
      el.pluginSelect.dispatchEvent(new Event('change'));
      closePluginSelectMenu();
    });
    el.pluginSelectMenu.appendChild(item);
  }
}

function makeWorkspaceId(prefix) {
  return `${prefix}_${Math.random().toString(36).slice(2, 9)}`;
}

function buildWorkspacePersistencePayload() {
  return {
    items: state.workspaceItems,
    snapshots: state.workspaceSnapshots,
    selectedWorkspaceId: state.selectedWorkspaceId,
    currentWorkspaceId: state.currentWorkspaceId,
    projectMeta: state.projectMeta
  };
}

function saveWorkspaceState() {
  // Local persistence disabled: draft is persisted on server.
}

function persistWorkspaceStateNow() {
  if (historyState.applying) return;
  snapshotCurrentWorkspace();
  saveWorkspaceState();
}

async function saveServerDraftNow() {
  if (autosaveState.serverInFlight || historyState.applying) return;
  const payload = buildWorkspacePersistencePayload();
  const serialized = JSON.stringify(payload);
  if (!serialized || serialized === autosaveState.lastServerSerialized) return;
  autosaveState.serverInFlight = true;
  try {
    const response = await fetch(withPrivateKey('/api/block-editor/draft'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: serialized
    });
    if (response.ok) autosaveState.lastServerSerialized = serialized;
  } catch {
    // ignore transient network errors
  } finally {
    autosaveState.serverInFlight = false;
  }
}

function scheduleServerDraftPersist(immediate = false) {
  if (immediate) {
    if (autosaveState.serverTimer) {
      clearTimeout(autosaveState.serverTimer);
      autosaveState.serverTimer = null;
    }
    saveServerDraftNow();
    return;
  }
  if (autosaveState.serverTimer) return;
  autosaveState.serverTimer = setTimeout(() => {
    autosaveState.serverTimer = null;
    saveServerDraftNow();
  }, 120);
}

function scheduleWorkspacePersist(immediate = false) {
  if (immediate) {
    if (autosaveState.timer) {
      clearTimeout(autosaveState.timer);
      autosaveState.timer = null;
    }
    persistWorkspaceStateNow();
    scheduleServerDraftPersist(true);
    return;
  }
  if (autosaveState.timer) return;
  autosaveState.timer = setTimeout(() => {
    autosaveState.timer = null;
    persistWorkspaceStateNow();
    scheduleServerDraftPersist(false);
  }, 50);
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function captureEditorState() {
  snapshotCurrentWorkspace();
  return {
    projectMeta: cloneJson(state.projectMeta),
    workspaceItems: cloneJson(state.workspaceItems),
    workspaceSnapshots: cloneJson(state.workspaceSnapshots),
    selectedWorkspaceId: state.selectedWorkspaceId,
    currentWorkspaceId: state.currentWorkspaceId,
    nodeClipboard: cloneJson(state.nodeClipboard),
    selectedCommentId: state.selectedCommentId
  };
}

function applyCapturedEditorState(serialized) {
  let parsed;
  try {
    parsed = JSON.parse(serialized);
  } catch {
    return;
  }
  historyState.applying = true;
  try {
    state.projectMeta = parsed?.projectMeta && typeof parsed.projectMeta === 'object'
      ? cloneJson(parsed.projectMeta)
      : { pluginId: 'aerogel-studio-plugin', pluginName: 'Aerogel Studio Plugin', version: '1.0.0' };
    state.workspaceItems = Array.isArray(parsed?.workspaceItems) ? cloneJson(parsed.workspaceItems) : [];
    state.workspaceSnapshots = parsed?.workspaceSnapshots && typeof parsed.workspaceSnapshots === 'object'
      ? cloneJson(parsed.workspaceSnapshots)
      : {};
    state.selectedWorkspaceId = typeof parsed?.selectedWorkspaceId === 'string' ? parsed.selectedWorkspaceId : null;
    state.currentWorkspaceId = typeof parsed?.currentWorkspaceId === 'string' ? parsed.currentWorkspaceId : null;
    state.nodeClipboard = parsed?.nodeClipboard ? cloneJson(parsed.nodeClipboard) : null;
    state.selectedCommentId = typeof parsed?.selectedCommentId === 'string' ? parsed.selectedCommentId : null;
    state.clipboardPasteCount = 0;

    ensureWorkspaceDefaults();
    normalizeAllWorkspaceOrders();
    if (!getWorkspaceById(state.selectedWorkspaceId)) {
      state.selectedWorkspaceId = state.workspaceItems.find((it) => it.kind === 'workspace')?.id || null;
    }
    if (!getWorkspaceById(state.currentWorkspaceId)) {
      state.currentWorkspaceId = state.selectedWorkspaceId;
    }
    applyWorkspaceSnapshot(state.currentWorkspaceId || state.selectedWorkspaceId);
    renderWorkspaceTree();
    showCanvasView();
    saveWorkspaceState();
  } finally {
    historyState.applying = false;
  }
}

function commitHistoryState(clearRedo = true) {
  if (historyState.applying) return;
  const serialized = JSON.stringify(captureEditorState());
  if (serialized === historyState.lastSerialized) return;
  historyState.undo.push(serialized);
  if (historyState.undo.length > HISTORY_LIMIT) historyState.undo.shift();
  historyState.lastSerialized = serialized;
  if (clearRedo) historyState.redo = [];
  scheduleWorkspacePersist(true);
  scheduleCollaborativePublish(true);
}

function undoHistory() {
  if (historyState.undo.length <= 1) return;
  const currentSerialized = historyState.undo.pop();
  if (currentSerialized) historyState.redo.push(currentSerialized);
  const prevSerialized = historyState.undo[historyState.undo.length - 1];
  if (!prevSerialized) return;
  applyCapturedEditorState(prevSerialized);
  historyState.lastSerialized = prevSerialized;
  scheduleWorkspacePersist(true);
  scheduleCollaborativePublish(true);
}

function redoHistory() {
  if (!historyState.redo.length) return;
  const nextSerialized = historyState.redo.pop();
  if (!nextSerialized) return;
  applyCapturedEditorState(nextSerialized);
  historyState.undo.push(nextSerialized);
  if (historyState.undo.length > HISTORY_LIMIT) historyState.undo.shift();
  historyState.lastSerialized = nextSerialized;
  scheduleWorkspacePersist(true);
  scheduleCollaborativePublish(true);
}

function captureCollaborativeState() {
  snapshotCurrentWorkspace();
  const snapshots = {};
  for (const [workspaceId, snapshot] of Object.entries(state.workspaceSnapshots || {})) {
    if (!snapshot || typeof snapshot !== 'object') continue;
    snapshots[workspaceId] = {
      comments: Array.isArray(snapshot.comments) ? cloneJson(snapshot.comments) : [],
      blocks: Array.isArray(snapshot.blocks) ? cloneJson(snapshot.blocks) : [],
      links: Array.isArray(snapshot.links) ? cloneJson(snapshot.links) : [],
      nextId: Number.isFinite(snapshot.nextId) ? snapshot.nextId : 1,
      nextCommentId: Number.isFinite(snapshot.nextCommentId) ? snapshot.nextCommentId : 1
    };
  }
  return {
    projectMeta: cloneJson(state.projectMeta),
    workspaceItems: cloneJson(state.workspaceItems),
    workspaceSnapshots: snapshots
  };
}

function applyCollaborativeStateFromRemote(payload) {
  if (!payload || typeof payload !== 'object') return;
  const incomingItems = Array.isArray(payload.workspaceItems) ? payload.workspaceItems : null;
  const incomingSnapshots = payload.workspaceSnapshots && typeof payload.workspaceSnapshots === 'object'
    ? payload.workspaceSnapshots
    : null;
  if (!incomingItems || !incomingSnapshots) return;

  realtimeState.suspended = true;
  historyState.applying = true;
  try {
    const localCommentDraft = state.commentDraft ? cloneJson(state.commentDraft) : null;
    const localDraggingNode = state.draggingNode ? cloneJson(state.draggingNode) : null;
    const localDraggingComment = state.draggingComment ? cloneJson(state.draggingComment) : null;
    const localResizingComment = state.resizingComment ? cloneJson(state.resizingComment) : null;
    const localPanning = state.panning ? cloneJson(state.panning) : null;
    const localConnectionDraft = state.connectionDraft && state.linkingActive
      ? cloneJson(state.connectionDraft)
      : null;
    const currentWsId = state.currentWorkspaceId;
    const localCurrentSnapshot = currentWsId ? state.workspaceSnapshots[currentWsId] : null;
    const localPanX = Number.isFinite(localCurrentSnapshot?.panX) ? localCurrentSnapshot.panX : state.panX;
    const localPanY = Number.isFinite(localCurrentSnapshot?.panY) ? localCurrentSnapshot.panY : state.panY;
    const localScale = Number.isFinite(localCurrentSnapshot?.scale) ? localCurrentSnapshot.scale : state.scale;

    if (payload.projectMeta && typeof payload.projectMeta === 'object') {
      state.projectMeta = cloneJson(payload.projectMeta);
    }
    state.workspaceItems = cloneJson(incomingItems);

    const nextSnapshots = {};
    for (const [workspaceId, incomingSnapshot] of Object.entries(incomingSnapshots)) {
      const localSnapshot = state.workspaceSnapshots[workspaceId];
      nextSnapshots[workspaceId] = {
        comments: Array.isArray(incomingSnapshot?.comments) ? cloneJson(incomingSnapshot.comments) : [],
        blocks: Array.isArray(incomingSnapshot?.blocks) ? cloneJson(incomingSnapshot.blocks) : [],
        links: Array.isArray(incomingSnapshot?.links) ? cloneJson(incomingSnapshot.links) : [],
        panX: Number.isFinite(localSnapshot?.panX) ? localSnapshot.panX : 0,
        panY: Number.isFinite(localSnapshot?.panY) ? localSnapshot.panY : 0,
        scale: Number.isFinite(localSnapshot?.scale) ? localSnapshot.scale : 1,
        nextId: Number.isFinite(incomingSnapshot?.nextId) ? incomingSnapshot.nextId : 1,
        nextCommentId: Number.isFinite(incomingSnapshot?.nextCommentId) ? incomingSnapshot.nextCommentId : 1
      };
    }
    state.workspaceSnapshots = nextSnapshots;

    ensureWorkspaceDefaults();
    normalizeAllWorkspaceOrders();
    if (!getWorkspaceById(state.selectedWorkspaceId)) {
      state.selectedWorkspaceId = state.workspaceItems.find((it) => it.kind === 'workspace')?.id || null;
    }
    if (!getWorkspaceById(state.currentWorkspaceId)) {
      state.currentWorkspaceId = state.selectedWorkspaceId;
    }

    applyWorkspaceSnapshot(state.currentWorkspaceId || state.selectedWorkspaceId);
    state.panX = localPanX;
    state.panY = localPanY;
    state.scale = localScale;
    state.connectionDraft = null;
    state.linkingActive = false;
    if (localConnectionDraft && typeof localConnectionDraft.from === 'string') {
      const fromNode = state.blocks.find((it) => it.id === localConnectionDraft.from);
      if (fromNode) {
        const restoredKind = localConnectionDraft.kind || linkKindFromType(fromNode.type);
        const restoredFromPortClass = localConnectionDraft.fromPortClass || `out-${restoredKind}`;
        let restoredSnapTargetId = localConnectionDraft.snapTargetId || null;
        let restoredSnapTargetPort = localConnectionDraft.snapTargetPort || null;
        if (restoredSnapTargetId) {
          const targetNode = state.blocks.find((it) => it.id === restoredSnapTargetId);
          if (!targetNode || !canLinkKindToPort(restoredKind, targetNode.type, restoredSnapTargetPort || '', localConnectionDraft.from)) {
            restoredSnapTargetId = null;
            restoredSnapTargetPort = null;
          }
        }
        state.connectionDraft = {
          from: localConnectionDraft.from,
          kind: restoredKind,
          fromPortClass: restoredFromPortClass,
          previousTargets: Array.isArray(localConnectionDraft.previousTargets)
            ? localConnectionDraft.previousTargets.filter((id) => state.blocks.some((it) => it.id === id))
            : [],
          snapTargetId: restoredSnapTargetId,
          snapTargetPort: restoredSnapTargetPort
        };
        state.linkingActive = true;
      }
    }
    state.draggingNode = null;
    if (localDraggingNode && typeof localDraggingNode.id === 'string' && state.blocks.some((it) => it.id === localDraggingNode.id)) {
      state.draggingNode = localDraggingNode;
    }
    state.draggingComment = null;
    if (localDraggingComment && typeof localDraggingComment.id === 'string' && state.comments.some((it) => it.id === localDraggingComment.id)) {
      state.draggingComment = localDraggingComment;
    }
    state.resizingComment = null;
    if (localResizingComment && typeof localResizingComment.id === 'string' && state.comments.some((it) => it.id === localResizingComment.id)) {
      state.resizingComment = localResizingComment;
    }
    state.commentDraft = localCommentDraft;
    state.panning = localPanning;
    renderWorkspaceTree();
    renderCanvas();
    scheduleGraphDecision(true);
    saveWorkspaceState();
    historyState.lastSerialized = JSON.stringify(captureEditorState());
  } finally {
    historyState.applying = false;
    realtimeState.suspended = false;
  }
}

async function publishCollaborativeStateNow() {
  if (realtimeState.suspended || !realtimeState.connected) return;
  if (realtimeState.publishInFlight) {
    realtimeState.publishQueued = true;
    return;
  }
  const collabState = captureCollaborativeState();
  const serialized = JSON.stringify(collabState);
  if (!serialized || serialized === realtimeState.lastPublishedSerialized) return;
  realtimeState.publishInFlight = true;
  try {
    const response = await fetch(withPrivateKey('/api/block-editor/realtime/publish'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId: realtimeState.clientId,
        type: 'state',
        payload: collabState
      })
    });
    if (response.ok) {
      realtimeState.lastPublishedSerialized = serialized;
    }
  } catch {
    // ignore transient realtime failures
  } finally {
    realtimeState.publishInFlight = false;
    if (realtimeState.publishQueued) {
      realtimeState.publishQueued = false;
      publishCollaborativeStateNow();
    }
  }
}

function scheduleCollaborativePublish(immediate = false) {
  scheduleWorkspacePersist(immediate);
  scheduleGraphDecision(immediate);
  if (realtimeState.suspended) return;
  if (immediate) {
    if (realtimeState.publishTimer) {
      clearTimeout(realtimeState.publishTimer);
      realtimeState.publishTimer = null;
    }
    publishCollaborativeStateNow();
    return;
  }
  if (realtimeState.publishTimer) return;
  realtimeState.publishTimer = setTimeout(() => {
    realtimeState.publishTimer = null;
    publishCollaborativeStateNow();
  }, 24);
}

function buildLocalPresence() {
  const pointer = {
    x: Number.isFinite(state.mouseWorld.x) ? state.mouseWorld.x : 0,
    y: Number.isFinite(state.mouseWorld.y) ? state.mouseWorld.y : 0
  };
  const draggingNode = state.draggingNode
    ? (() => {
      const block = state.blocks.find((it) => it.id === state.draggingNode.id);
      if (!block) return null;
      return { id: block.id, x: block.x, y: block.y };
    })()
    : null;
  const draggingComment = state.draggingComment
    ? (() => {
      const comment = state.comments.find((it) => it.id === state.draggingComment.id);
      if (!comment) return null;
      return { id: comment.id, x: comment.x, y: comment.y, width: comment.width, height: comment.height };
    })()
    : null;
  const resizingComment = state.resizingComment
    ? (() => {
      const comment = state.comments.find((it) => it.id === state.resizingComment.id);
      if (!comment) return null;
      return { id: comment.id, x: comment.x, y: comment.y, width: comment.width, height: comment.height };
    })()
    : null;
  const linking = state.connectionDraft && state.linkingActive
    ? (() => {
      const from = getPortCenterWorld(state.connectionDraft.from, state.connectionDraft.fromPortClass || 'out');
      const to = state.connectionDraft.snapTargetId
        ? (getPortCenterWorld(state.connectionDraft.snapTargetId, state.connectionDraft.snapTargetPort || 'in-exec') || state.mouseWorld)
        : state.mouseWorld;
      return {
        from: from || pointer,
        to: to || pointer,
        kind: state.connectionDraft.kind || 'exec'
      };
    })()
    : null;
  const commentDraft = state.commentDraft
    ? {
      startX: state.commentDraft.startX,
      startY: state.commentDraft.startY,
      currentX: state.commentDraft.currentX,
      currentY: state.commentDraft.currentY
    }
    : null;

  return {
    pointer,
    selectedNodeId: state.selectedId || null,
    selectedCommentId: state.selectedCommentId || null,
    draggingNode,
    draggingComment,
    resizingComment,
    linking,
    commentDraft
  };
}

async function publishPresenceNow() {
  if (realtimeState.suspended || !realtimeState.connected) return;
  if (realtimeState.presenceInFlight) {
    realtimeState.presenceQueued = true;
    return;
  }
  const presence = buildLocalPresence();
  const serialized = JSON.stringify(presence);
  if (!serialized || serialized === realtimeState.lastPresenceSerialized) return;
  realtimeState.presenceInFlight = true;
  try {
    const response = await fetch(withPrivateKey('/api/block-editor/realtime/publish'), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientId: realtimeState.clientId,
        type: 'presence',
        payload: presence
      })
    });
    if (response.ok) realtimeState.lastPresenceSerialized = serialized;
  } catch {
    // ignore
  } finally {
    realtimeState.presenceInFlight = false;
    if (realtimeState.presenceQueued) {
      realtimeState.presenceQueued = false;
      publishPresenceNow();
    }
  }
}

function schedulePresencePublish(immediate = false) {
  if (realtimeState.suspended) return;
  if (immediate) {
    if (realtimeState.presenceTimer) {
      clearTimeout(realtimeState.presenceTimer);
      realtimeState.presenceTimer = null;
    }
    publishPresenceNow();
    return;
  }
  if (realtimeState.presenceTimer) return;
  realtimeState.presenceTimer = setTimeout(() => {
    realtimeState.presenceTimer = null;
    publishPresenceNow();
  }, 16);
}

function connectCollaborativeRealtime() {
  if (realtimeState.eventSource) return;
  const streamUrl = withPrivateKey(`/api/block-editor/realtime/stream?clientId=${encodeURIComponent(realtimeState.clientId)}`);
  const es = new EventSource(streamUrl);
  realtimeState.eventSource = es;
  es.onopen = () => {
    realtimeState.connected = true;
    scheduleCollaborativePublish(true);
    schedulePresencePublish(true);
  };
  es.onerror = () => {
    realtimeState.connected = false;
    if (realtimeState.presenceAnimationFrame) {
      cancelAnimationFrame(realtimeState.presenceAnimationFrame);
      realtimeState.presenceAnimationFrame = null;
    }
  };
  es.onmessage = (event) => {
    if (!event?.data) return;
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch {
      return;
    }
    const sender = typeof payload?.clientId === 'string' ? payload.clientId : '';
    const type = typeof payload?.type === 'string' ? payload.type : 'state';
    const body = payload?.payload ?? payload?.state;
    if (!body || sender === realtimeState.clientId) return;
    if (type === 'state') {
      const incomingSerialized = JSON.stringify(body);
      const localSerialized = JSON.stringify(captureCollaborativeState());
      if (incomingSerialized === localSerialized) return;
      applyCollaborativeStateFromRemote(body);
      return;
    }
    if (type === 'presence') {
      realtimeState.peers[sender] = {
        clientId: sender,
        color: colorForClient(sender),
        presence: body,
        lastSeen: Date.now()
      };
      ensurePresenceAnimationLoop();
      renderRemotePresenceLayer();
    }
  };
}

function snapshotCurrentWorkspace() {
  const current = state.currentWorkspaceId ? getWorkspaceById(state.currentWorkspaceId) : null;
  if (!current || current.kind !== 'workspace') return;
  state.workspaceSnapshots[current.id] = {
    comments: cloneJson(state.comments),
    blocks: cloneJson(state.blocks),
    links: cloneJson(state.links),
    panX: state.panX,
    panY: state.panY,
    scale: state.scale,
    nextId: state.nextId,
    nextCommentId: state.nextCommentId
  };
}

function applyWorkspaceSnapshot(workspaceId) {
  const selected = workspaceId ? getWorkspaceById(workspaceId) : null;
  if (!selected || selected.kind !== 'workspace') return;
  state.currentWorkspaceId = workspaceId;
  const snapshot = state.workspaceSnapshots[workspaceId];
  if (!snapshot) {
    state.comments = [];
    state.blocks = [];
    state.links = [];
    state.nextId = 1;
    state.nextCommentId = 1;
    state.selectedId = null;
    state.selectedCommentId = null;
    state.panX = 0;
    state.panY = 0;
    state.scale = 1;
    state.connectionDraft = null;
    renderCanvas();
    scheduleGraphDecision(true);
    return;
  }
  state.comments = Array.isArray(snapshot.comments) ? cloneJson(snapshot.comments) : [];
  state.blocks = Array.isArray(snapshot.blocks) ? cloneJson(snapshot.blocks) : [];
  state.links = Array.isArray(snapshot.links) ? cloneJson(snapshot.links) : [];
  state.nextId = Number.isFinite(snapshot.nextId) ? snapshot.nextId : (state.blocks.reduce((acc, it) => {
    const numeric = Number(String(it.id).replace(/^n/, ''));
    return Number.isFinite(numeric) ? Math.max(acc, numeric + 1) : acc;
  }, 1));
  state.nextCommentId = Number.isFinite(snapshot.nextCommentId)
    ? snapshot.nextCommentId
    : (state.comments.reduce((acc, it) => {
      const numeric = Number(String(it.id).replace(/^c/, ''));
      return Number.isFinite(numeric) ? Math.max(acc, numeric + 1) : acc;
    }, 1));
  state.selectedId = null;
  state.selectedCommentId = null;
  state.panX = Number.isFinite(snapshot.panX) ? snapshot.panX : 0;
  state.panY = Number.isFinite(snapshot.panY) ? snapshot.panY : 0;
  state.scale = Number.isFinite(snapshot.scale) ? snapshot.scale : 1;
  state.connectionDraft = null;
  renderCanvas();
  scheduleGraphDecision(true);
}

function selectWorkspace(workspaceId) {
  if (state.selectedWorkspaceId === workspaceId) return;
  snapshotCurrentWorkspace();
  state.selectedWorkspaceId = workspaceId;
  applyWorkspaceSnapshot(workspaceId);
  saveWorkspaceState();
  renderWorkspaceTree();
}

function showCanvasView() {
  el.workspaceToolbar.classList.remove('hidden');
  el.workspace.classList.remove('hidden');
  renderCanvas();
}

function normalizeWorkspacePluginId(raw, fallbackId) {
  const ascii = String(raw || '')
    .normalize('NFKD')
    .replace(/[^\x00-\x7F]/g, '');
  const normalized = ascii
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .replace(/-+/g, '-');
  if (normalized) return normalized;
  return `workspace-${fallbackId.replace(/[^a-z0-9]/gi, '').toLowerCase()}`;
}

function getWorkspaceById(id) {
  return state.workspaceItems.find((it) => it.id === id) || null;
}

function workspaceOrderValue(item) {
  return Number.isFinite(item?.order) ? item.order : 0;
}

function normalizeWorkspaceOrders(parentId, kind) {
  const siblings = state.workspaceItems
    .filter((it) => {
      if ((it.parentId || null) !== (parentId || null)) return false;
      return it.kind === kind;
    })
    .sort((a, b) => {
      const diff = workspaceOrderValue(a) - workspaceOrderValue(b);
      if (diff !== 0) return diff;
      return a.name.localeCompare(b.name, 'ko');
    });
  siblings.forEach((item, index) => {
    item.order = index + 1;
  });
}

function normalizeAllWorkspaceOrders() {
  const parents = new Set(state.workspaceItems.map((it) => it.parentId || 'root'));
  for (const parentRaw of parents) {
    const parentId = parentRaw === 'root' ? null : parentRaw;
    normalizeWorkspaceOrders(parentId, 'folder');
    normalizeWorkspaceOrders(parentId, 'workspace');
  }
}

function nextWorkspaceOrder(parentId, kind) {
  const siblings = state.workspaceItems.filter((it) => {
    if ((it.parentId || null) !== (parentId || null)) return false;
    return it.kind === kind;
  });
  if (!siblings.length) return 1;
  return Math.max(...siblings.map((it) => workspaceOrderValue(it))) + 1;
}

function getWorkspacePathParts(workspaceId) {
  const parts = [];
  let current = getWorkspaceById(workspaceId);
  while (current) {
    parts.unshift(current.name);
    current = current.parentId ? getWorkspaceById(current.parentId) : null;
  }
  return parts;
}

function applyWorkspaceMetaFromSelection() {
  const selected = state.selectedWorkspaceId ? getWorkspaceById(state.selectedWorkspaceId) : null;
  if (!selected) {
    el.workspaceHint.textContent = '선택된 작업 공간 없음';
    return;
  }
  const pathParts = getWorkspacePathParts(selected.id);
  if (selected.kind !== 'workspace') {
    el.workspaceHint.textContent = `선택됨: ${pathParts.join(' / ')}`;
    return;
  }
  state.projectMeta.pluginName = selected.name;
  state.projectMeta.pluginId = normalizeWorkspacePluginId(pathParts.join('-'), selected.id);
  el.workspaceHint.textContent = `선택됨: ${pathParts.join(' / ')}`;
}

function renderWorkspaceTree() {
  el.workspaceTree.innerHTML = '';
  el.workspaceTree.classList.toggle('root-drop-target', state.workspaceDropFolderId === '__root__');
  if (!state.workspaceItems.length) {
    const empty = document.createElement('div');
    empty.className = 'workspace-hint';
    empty.textContent = '작업 공간을 추가하세요.';
    el.workspaceTree.appendChild(empty);
    applyWorkspaceMetaFromSelection();
    return;
  }

  const childrenMap = new Map();
  for (const item of state.workspaceItems) {
    const key = item.parentId || 'root';
    if (!childrenMap.has(key)) childrenMap.set(key, []);
    childrenMap.get(key).push(item);
  }
  for (const list of childrenMap.values()) {
    list.sort((a, b) => {
      if (a.kind !== b.kind) {
        if (a.kind === 'folder') return -1;
        if (b.kind === 'folder') return 1;
      }
      const diff = workspaceOrderValue(a) - workspaceOrderValue(b);
      if (diff !== 0) return diff;
      return a.name.localeCompare(b.name, 'ko');
    });
  }

  const walk = (parentId, depth) => {
    const list = childrenMap.get(parentId || 'root') || [];
    for (const item of list) {
      const row = document.createElement('div');
      const isFolder = item.kind === 'folder';
      const isFolderDropTarget = state.workspaceDropFolderId === item.id;
      const isInsertTarget = state.workspaceDropItemId === item.id;
      row.className = `workspace-item ${item.kind}${state.selectedWorkspaceId === item.id ? ' active' : ''}${state.currentWorkspaceId === item.id ? ' viewing' : ''}${isFolder && !item.expanded ? ' closed' : ''}${isFolderDropTarget ? ' drop-target' : ''}${state.draggingItemId === item.id ? ' dragging' : ''}${isInsertTarget && state.workspaceDropAfter ? ' insert-after' : ''}${isInsertTarget && !state.workspaceDropAfter ? ' insert-before' : ''}`;
      row.dataset.id = item.id;
      row.dataset.kind = item.kind;
      row.draggable = true;

      const indent = document.createElement('span');
      indent.className = 'workspace-item-indent';
      indent.style.width = `${depth * 14}px`;
      row.appendChild(indent);

      const toggle = document.createElement('span');
      toggle.className = isFolder ? 'workspace-toggle' : 'workspace-dot';
      toggle.dataset.role = isFolder ? 'toggle' : 'dot';
      row.appendChild(toggle);

      const name = document.createElement('span');
      name.textContent = item.name;
      row.appendChild(name);
      el.workspaceTree.appendChild(row);

      if (isFolder && item.expanded !== false) {
        walk(item.id, depth + 1);
      }
    }
  };

  walk(null, 0);
  applyWorkspaceMetaFromSelection();
}

function ensureWorkspaceDefaults() {
  if (!state.workspaceItems.some((it) => it.kind === 'workspace')) {
    const defaultWsId = makeWorkspaceId('ws');
    state.workspaceItems.push({
      id: defaultWsId,
      kind: 'workspace',
      parentId: null,
      name: '기본 작업 공간',
      order: 1
    });
    state.selectedWorkspaceId = defaultWsId;
    state.currentWorkspaceId = defaultWsId;
  }
  if (!state.currentWorkspaceId) {
    state.currentWorkspaceId = state.workspaceItems.find((it) => it.kind === 'workspace')?.id || null;
  }
}

function loadWorkspaceState() {
  state.workspaceItems = [];
  state.workspaceSnapshots = {};
  state.selectedWorkspaceId = null;
  state.currentWorkspaceId = null;
  ensureWorkspaceDefaults();
  normalizeAllWorkspaceOrders();
}

async function loadServerDraft() {
  try {
    const response = await fetch(withPrivateKey('/api/block-editor/draft'));
    if (!response.ok) return false;
    const data = await response.json();
    if (!data?.ok || !data?.draft || typeof data.draft !== 'object') return false;
    autosaveState.lastServerSerialized = JSON.stringify(data.draft);
    const parsed = data.draft;
    if (parsed?.projectMeta && typeof parsed.projectMeta === 'object') {
      state.projectMeta = {
        pluginId: String(parsed.projectMeta.pluginId || state.projectMeta.pluginId || 'aerogel-studio-plugin'),
        pluginName: String(parsed.projectMeta.pluginName || state.projectMeta.pluginName || 'Aerogel Studio Plugin'),
        version: String(parsed.projectMeta.version || state.projectMeta.version || '1.0.0')
      };
    }
    const items = Array.isArray(parsed.items) ? parsed.items : [];
    const snapshots = parsed && typeof parsed.snapshots === 'object' && parsed.snapshots
      ? parsed.snapshots
      : {};
    state.workspaceItems = items
      .filter((it) => it && (it.kind === 'folder' || it.kind === 'workspace') && typeof it.name === 'string')
      .map((it) => ({
        id: typeof it.id === 'string'
          ? it.id
          : makeWorkspaceId(it.kind === 'folder' ? 'fld' : 'ws'),
        kind: it.kind,
        parentId: typeof it.parentId === 'string' ? it.parentId : null,
        name: it.name.trim() || (it.kind === 'folder' ? '새 폴더' : '새 작업 공간'),
        expanded: it.kind === 'folder' ? it.expanded !== false : undefined,
        order: Number.isFinite(it.order) ? it.order : 0
      }));
    state.selectedWorkspaceId = typeof parsed.selectedWorkspaceId === 'string' ? parsed.selectedWorkspaceId : null;
    state.currentWorkspaceId = typeof parsed.currentWorkspaceId === 'string' ? parsed.currentWorkspaceId : null;
    state.workspaceSnapshots = {};
    for (const [id, snapshot] of Object.entries(snapshots)) {
      if (!snapshot || typeof snapshot !== 'object') continue;
      state.workspaceSnapshots[id] = {
        comments: Array.isArray(snapshot.comments) ? snapshot.comments : [],
        blocks: Array.isArray(snapshot.blocks) ? snapshot.blocks : [],
        links: Array.isArray(snapshot.links) ? snapshot.links : [],
        panX: Number.isFinite(snapshot.panX) ? snapshot.panX : 0,
        panY: Number.isFinite(snapshot.panY) ? snapshot.panY : 0,
        scale: Number.isFinite(snapshot.scale) ? snapshot.scale : 1,
        nextId: Number.isFinite(snapshot.nextId) ? snapshot.nextId : 1,
        nextCommentId: Number.isFinite(snapshot.nextCommentId) ? snapshot.nextCommentId : 1
      };
    }
    if (!getWorkspaceById(state.selectedWorkspaceId)) {
      state.selectedWorkspaceId = null;
    }
    if (!getWorkspaceById(state.currentWorkspaceId)) {
      state.currentWorkspaceId = null;
    }
    ensureWorkspaceDefaults();
    normalizeAllWorkspaceOrders();
    const validWorkspaceIds = new Set(state.workspaceItems.filter((it) => it.kind === 'workspace').map((it) => it.id));
    state.workspaceSnapshots = Object.fromEntries(
      Object.entries(state.workspaceSnapshots).filter(([id]) => validWorkspaceIds.has(id))
    );
    return true;
  } catch {
    return false;
  }
}

async function addWorkspaceItem(kind) {
  const selected = state.selectedWorkspaceId ? getWorkspaceById(state.selectedWorkspaceId) : null;
  const parentId = selected?.kind === 'folder' ? selected.id : (selected?.parentId || null);
  const name = await requestNameDialog(
    kind === 'folder' ? '폴더 이름 입력' : '작업 공간 이름 입력',
    kind === 'folder' ? '예: 콘텐츠' : '예: 기본 시스템'
  );
  if (!name || !name.trim()) return;

  const id = makeWorkspaceId(kind === 'folder' ? 'fld' : 'ws');
  const item = {
    id,
    kind,
    parentId,
    name: name.trim(),
    order: nextWorkspaceOrder(parentId, kind)
  };
  if (kind === 'folder') item.expanded = true;
  state.workspaceItems.push(item);
  if (kind === 'workspace') {
    state.workspaceSnapshots[id] = {
      comments: [],
      blocks: [],
      links: [],
      panX: 0,
      panY: 0,
      scale: 1,
      nextId: 1,
      nextCommentId: 1
    };
    selectWorkspace(id);
    commitHistoryState();
    return;
  }
  saveWorkspaceState();
  renderWorkspaceTree();
  commitHistoryState();
}

function isFolderDescendant(folderId, potentialAncestorId) {
  let current = getWorkspaceById(folderId);
  while (current && current.parentId) {
    if (current.parentId === potentialAncestorId) return true;
    current = getWorkspaceById(current.parentId);
  }
  return false;
}

function moveItemToFolder(itemId, folderId) {
  const item = getWorkspaceById(itemId);
  if (!item) return;
  const oldParentId = item.parentId || null;
  if (folderId) {
    const folder = getWorkspaceById(folderId);
    if (!folder || folder.kind !== 'folder') return;
    if (item.kind === 'folder' && (item.id === folderId || isFolderDescendant(folderId, item.id))) return;
    if (folder.expanded === false) folder.expanded = true;
  }
  item.parentId = folderId || null;
  item.order = nextWorkspaceOrder(item.parentId || null, item.kind);
  normalizeWorkspaceOrders(oldParentId, item.kind);
  normalizeWorkspaceOrders(item.parentId || null, item.kind);
  saveWorkspaceState();
  renderWorkspaceTree();
  commitHistoryState();
}

function moveItemRelative(itemId, targetItemId, placeAfter) {
  const item = getWorkspaceById(itemId);
  const target = getWorkspaceById(targetItemId);
  if (!item || !target) return;
  const sameGroup = item.kind === target.kind;
  if (!sameGroup) return;
  if (item.id === target.id) return;
  if (item.kind === 'folder' && isFolderDescendant(target.id, item.id)) return;

  const oldParentId = item.parentId || null;
  const newParentId = target.parentId || null;
  item.parentId = newParentId;
  item.order = workspaceOrderValue(target) + (placeAfter ? 0.5 : -0.5);
  normalizeWorkspaceOrders(oldParentId, item.kind);
  normalizeWorkspaceOrders(newParentId, item.kind);
  saveWorkspaceState();
  renderWorkspaceTree();
  commitHistoryState();
}

async function removeWorkspaceItem(itemId) {
  const item = getWorkspaceById(itemId);
  if (!item) return;
  snapshotCurrentWorkspace();

  if (item.kind === 'folder') {
    const directChildren = state.workspaceItems.filter((it) => it.parentId === item.id);
    if (directChildren.length > 0) {
      const ok = await requestConfirmDialog(
        '폴더 삭제',
        '폴더 안에 항목이 있습니다. 폴더와 하위 항목을 함께 삭제할까요?',
        '삭제'
      );
      if (!ok) return;
    }
    const removeIds = new Set([item.id]);
    let expanded = true;
    while (expanded) {
      expanded = false;
      for (const candidate of state.workspaceItems) {
        if (candidate.parentId && removeIds.has(candidate.parentId) && !removeIds.has(candidate.id)) {
          removeIds.add(candidate.id);
          expanded = true;
        }
      }
    }
    for (const removeId of removeIds) {
      delete state.workspaceSnapshots[removeId];
    }
    state.workspaceItems = state.workspaceItems.filter((it) => !removeIds.has(it.id));
  } else {
    const deletingSelected = state.selectedWorkspaceId === item.id;
    const hasContent = deletingSelected && (state.blocks.length > 0 || state.links.length > 0);
    if (hasContent) {
      const ok = await requestConfirmDialog(
        '작업 공간 삭제',
        '현재 작업 내용이 있습니다. 작업 공간을 삭제할까요?',
        '삭제'
      );
      if (!ok) return;
    }
    delete state.workspaceSnapshots[item.id];
    state.workspaceItems = state.workspaceItems.filter((it) => it.id !== item.id);
  }

  const prevSelected = state.selectedWorkspaceId;
  if (!getWorkspaceById(state.selectedWorkspaceId)) {
    state.selectedWorkspaceId = state.workspaceItems.find((it) => it.kind === 'workspace')?.id
      || state.workspaceItems[0]?.id
      || null;
  }
  if (!getWorkspaceById(state.currentWorkspaceId)) {
    state.currentWorkspaceId = state.workspaceItems.find((it) => it.kind === 'workspace')?.id || null;
  }
  ensureWorkspaceDefaults();
  normalizeAllWorkspaceOrders();
  if (prevSelected !== state.selectedWorkspaceId && state.currentWorkspaceId) {
    applyWorkspaceSnapshot(state.currentWorkspaceId);
  }
  saveWorkspaceState();
  renderWorkspaceTree();
  commitHistoryState();
}

async function renameWorkspaceItem(itemId) {
  const item = getWorkspaceById(itemId);
  if (!item) return;
  const nextName = await requestNameDialog(
    item.kind === 'folder' ? '폴더 이름 변경' : '작업 공간 이름 변경',
    '새 이름 입력',
    item.name
  );
  if (!nextName || !nextName.trim()) return;
  item.name = nextName.trim();
  saveWorkspaceState();
  renderWorkspaceTree();
  commitHistoryState();
}

async function refreshPluginList() {
  const response = await fetch(withPrivateKey('/api/block-editor/plugins'));
  const data = await response.json();
  if (!data.ok) {
    setResult(data);
    return;
  }

  el.pluginSelect.innerHTML = '';
  if (!data.plugins.length) {
    const empty = document.createElement('option');
    empty.value = '';
    empty.textContent = 'plugins 폴더에 JAR 없음';
    el.pluginSelect.appendChild(empty);
    syncPluginSelectLabel();
    rebuildPluginSelectMenu();
    return;
  }

  for (const plugin of data.plugins) {
    const opt = document.createElement('option');
    opt.value = plugin.jar;
    const sourceMark = plugin.hasBlockSource ? ' [block-source]' : '';
    opt.textContent = `${plugin.pluginId} (${plugin.jar})${sourceMark}`;
    el.pluginSelect.appendChild(opt);
  }
  syncPluginSelectLabel();
  rebuildPluginSelectMenu();
}

async function loadSelectedPlugin() {
  snapshotCurrentWorkspace();
  const jar = el.pluginSelect.value;
  if (!jar) {
    setResult('선택된 플러그인이 없습니다.');
    return;
  }

  const response = await fetch(withPrivateKey(`/api/block-editor/load?jar=${encodeURIComponent(jar)}`));
  const data = await response.json();
  if (!data.ok) {
    setResult(data);
    return;
  }

  if (state.selectedWorkspaceId) {
    applyWorkspaceMetaFromSelection();
  } else {
    state.projectMeta.pluginId = data.pluginId || 'aerogel-studio-plugin';
    state.projectMeta.pluginName = data.pluginName || state.projectMeta.pluginId;
  }
  state.projectMeta.version = data.version || '1.0.0';

  const loadedBlocks = Array.isArray(data.blocks) ? data.blocks : [];
  state.blocks = loadedBlocks.map((block, index) => ({
    id: String(block.id || `n${index + 1}`),
    type: block.type,
    x: Number.isFinite(block.x) ? block.x : 40 + (index * 18),
    y: Number.isFinite(block.y) ? block.y : 40 + (index * 12),
    params: Object.fromEntries(
      Object.entries(block.params || {})
        .map(([key, value]) => [key, String(value ?? '')])
    )
  }));

  const loadedLinks = Array.isArray(data.links) ? data.links : [];
  state.links = loadedLinks
    .filter((it) => it && typeof it.from === 'string' && typeof it.to === 'string')
    .map((it) => ({
      from: it.from,
      to: it.to,
      kind: typeof it.kind === 'string' ? it.kind : undefined,
      fromPortClass: typeof it.fromPortClass === 'string' ? it.fromPortClass : undefined,
      toPortClass: typeof it.toPortClass === 'string' ? it.toPortClass : undefined
    }));

  state.nextId = state.blocks.reduce((acc, it) => {
    const numeric = Number(String(it.id).replace(/^n/, ''));
    return Number.isFinite(numeric) ? Math.max(acc, numeric + 1) : acc;
  }, 1);

  state.selectedId = null;
  state.comments = [];
  state.nextCommentId = 1;
  state.selectedCommentId = null;
  state.panX = 0;
  state.panY = 0;
  state.scale = 1;
  state.connectionDraft = null;
  hideContextMenu();
  snapshotCurrentWorkspace();
  saveWorkspaceState();
  renderCanvas();
  commitHistoryState();
  setResult({ ok: true, loadedFrom: jar, blockCount: state.blocks.length, linkCount: state.links.length });
}

async function saveProject() {
  snapshotCurrentWorkspace();
  saveWorkspaceState();
  const payload = {
    pluginId: state.projectMeta.pluginId,
    pluginName: state.projectMeta.pluginName,
    version: state.projectMeta.version,
    blocks: state.blocks.map((block) => ({
      id: block.id,
      type: block.type,
      params: Object.fromEntries(
        Object.entries(block.params || {})
          .map(([key, value]) => [key, String(value ?? '')])
      ),
      x: Math.round(block.x),
      y: Math.round(block.y)
    })),
    links: state.links.map((link) => ({
      from: link.from,
      to: link.to,
      kind: link.kind,
      fromPortClass: link.fromPortClass,
      toPortClass: link.toPortClass
    }))
  };

  setResult('저장 중...');
  const response = await fetch(withPrivateKey('/api/block-editor/save'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  setResult(data);
  await refreshPluginList();
}

function installEvents() {
  let minimapDragging = false;

  const suppressNativeContextMenu = (event) => {
    event.preventDefault();
  };

  window.addEventListener('contextmenu', suppressNativeContextMenu, { capture: true });
  document.addEventListener('contextmenu', suppressNativeContextMenu, { capture: true });
  const suppressByOnContextMenu = (event) => {
    event.preventDefault();
    return false;
  };
  window.oncontextmenu = suppressByOnContextMenu;
  document.oncontextmenu = suppressByOnContextMenu;
  document.body.oncontextmenu = suppressByOnContextMenu;

  el.pluginSelectTrigger.addEventListener('click', async () => {
    const opened = el.pluginSelectField.classList.contains('open');
    if (opened) {
      closePluginSelectMenu();
      return;
    }
    await refreshPluginList().catch((error) => setResult(String(error)));
    rebuildPluginSelectMenu();
    el.pluginSelectField.classList.add('open');
    el.pluginSelectMenu.classList.remove('hidden');
    el.pluginSelectTrigger.setAttribute('aria-expanded', 'true');
  });

  el.pluginSelect.addEventListener('change', async () => {
    syncPluginSelectLabel();
    rebuildPluginSelectMenu();
    await loadSelectedPlugin().catch((error) => setResult(String(error)));
  });

  el.autoCreatePromptInput.addEventListener('input', () => {
    resizeAutoCreatePromptInput();
    refreshAutoCreateGenerateEnabled();
  });
  el.autoCreateGenerateBtn.addEventListener('click', () => {
    runAutoCreateFromDialog().catch((error) => {
      setAutoCreateAuthStatus(`생성 실패: ${error?.message || 'unknown error'}`, 'error');
      autoCreateState.generateInFlight = false;
      refreshAutoCreateGenerateEnabled();
    });
  });
  el.autoCreateCancelBtn.addEventListener('click', () => {
    closeAutoCreateDialog();
  });
  el.autoCreateDialog.addEventListener('mousedown', (event) => {
    if (event.target === el.autoCreateDialog) {
      closeAutoCreateDialog();
    }
  });

  if (el.minimapCanvas instanceof HTMLElement) {
    el.minimapCanvas.addEventListener('mousedown', (event) => {
      if (event.button !== 0) return;
      minimapDragging = true;
      focusViewportFromMinimapClient(event.clientX, event.clientY);
      event.preventDefault();
    });
  }

  el.addFolderMenuBtn.addEventListener('click', async () => {
    closeWorkspaceTreeMenu();
    await addWorkspaceItem('folder');
  });
  el.addWorkspaceMenuBtn.addEventListener('click', async () => {
    closeWorkspaceTreeMenu();
    await addWorkspaceItem('workspace');
  });
  el.renameWorkspaceMenuBtn.addEventListener('click', async () => {
    const targetId = state.workspaceMenuTargetId;
    closeWorkspaceTreeMenu();
    if (!targetId) return;
    await renameWorkspaceItem(targetId);
  });
  el.deleteWorkspaceMenuBtn.addEventListener('click', async () => {
    const targetId = state.workspaceMenuTargetId;
    closeWorkspaceTreeMenu();
    if (!targetId) return;
    await removeWorkspaceItem(targetId);
  });

  el.workspaceTree.addEventListener('click', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    const row = target.closest('.workspace-item');
    if (!row) return;
    const item = getWorkspaceById(row.dataset.id || '');
    if (!item) return;
    if (item.kind === 'folder') {
      snapshotCurrentWorkspace();
      item.expanded = item.expanded === false;
      state.selectedWorkspaceId = item.id;
      saveWorkspaceState();
      renderWorkspaceTree();
      return;
    }
    if (item.kind === 'workspace') {
      selectWorkspace(item.id);
      showCanvasView();
    }
  });
  el.workspaceTree.addEventListener('dragstart', (event) => {
    const target = event.target instanceof Element ? event.target.closest('.workspace-item') : null;
    if (!target) return;
    state.draggingItemId = target.dataset.id || null;
    state.draggingItemKind = target.dataset.kind || null;
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', state.draggingItemId || '');
    }
    requestAnimationFrame(() => renderWorkspaceTree());
  });
  el.workspaceTree.addEventListener('dragover', (event) => {
    if (!state.draggingItemId) return;
    const target = event.target instanceof Element ? event.target.closest('.workspace-item') : null;
    const targetId = target?.dataset.id || null;
    const targetKind = target?.dataset.kind || null;

    let nextFolderId = null;
    let nextItemId = null;
    let nextAfter = false;
    if (targetKind === 'folder' && targetId) {
      if (state.draggingItemKind === 'folder' && targetId !== state.draggingItemId) {
        const rect = target.getBoundingClientRect();
        const y = event.clientY - rect.top;
        const upper = rect.height * 0.3;
        const lower = rect.height * 0.7;
        if (y < upper) {
          nextItemId = targetId;
          nextAfter = false;
        } else if (y > lower) {
          nextItemId = targetId;
          nextAfter = true;
        } else {
          nextFolderId = targetId;
        }
      } else {
        nextFolderId = targetId;
      }
    } else if (
      targetId
      && targetId !== state.draggingItemId
      && targetKind === state.draggingItemKind
    ) {
      nextItemId = targetId;
      const rect = target.getBoundingClientRect();
      nextAfter = event.clientY > rect.top + (rect.height / 2);
    } else if (!target) {
      nextFolderId = '__root__';
    }

    if (
      state.workspaceDropFolderId !== nextFolderId
      || state.workspaceDropItemId !== nextItemId
      || state.workspaceDropAfter !== nextAfter
    ) {
      state.workspaceDropFolderId = nextFolderId;
      state.workspaceDropItemId = nextItemId;
      state.workspaceDropAfter = nextAfter;
      renderWorkspaceTree();
    }
    event.preventDefault();
    if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
  });
  el.workspaceTree.addEventListener('drop', (event) => {
    if (!state.draggingItemId) return;
    event.preventDefault();
    if (state.workspaceDropItemId) {
      moveItemRelative(state.draggingItemId, state.workspaceDropItemId, state.workspaceDropAfter);
    } else {
      const folderId = state.workspaceDropFolderId === '__root__' ? null : state.workspaceDropFolderId;
      moveItemToFolder(state.draggingItemId, folderId);
    }
    state.draggingItemId = null;
    state.draggingItemKind = null;
    state.workspaceDropFolderId = null;
    state.workspaceDropItemId = null;
    state.workspaceDropAfter = false;
    renderWorkspaceTree();
  });
  el.workspaceTree.addEventListener('dragend', () => {
    state.draggingItemId = null;
    state.draggingItemKind = null;
    state.workspaceDropFolderId = null;
    state.workspaceDropItemId = null;
    state.workspaceDropAfter = false;
    renderWorkspaceTree();
  });

  el.workspaceHub.addEventListener('contextmenu', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (!el.workspaceHub.contains(target)) return;
    event.preventDefault();
    const row = target.closest('.workspace-item');
    const targetId = row?.dataset.id || null;
    if (targetId) {
      const item = getWorkspaceById(targetId);
      if (item?.kind === 'workspace') {
        selectWorkspace(targetId);
        showCanvasView();
      }
    }
    showWorkspaceTreeMenu(event.clientX, event.clientY, targetId);
  });

  document.addEventListener('mousedown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (!el.contextMenu.contains(target)) hideContextMenu();
    if (!el.pluginSelectField.contains(target)) closePluginSelectMenu();
    if (!el.workspaceTreeMenu.contains(target)) closeWorkspaceTreeMenu();
  });

  document.addEventListener('keydown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const typing = !!target?.closest('input, textarea, [contenteditable="true"]');
    const selectingText = hasActiveTextSelection();
    const withCtrl = event.ctrlKey || event.metaKey;
    const key = event.key.toLowerCase();

    if (!typing && withCtrl && key === 'z' && !event.shiftKey) {
      event.preventDefault();
      undoHistory();
      return;
    }
    if (!typing && withCtrl && (key === 'y' || (key === 'z' && event.shiftKey))) {
      event.preventDefault();
      redoHistory();
      return;
    }

    if (!typing && !selectingText && withCtrl && key === 'c') {
      if (copySelectedNodeToClipboard()) {
        event.preventDefault();
        setResult('노드 복사됨');
      }
      return;
    }
    if (!typing && !selectingText && withCtrl && key === 'v') {
      if (pasteNodeFromClipboard()) {
        event.preventDefault();
        renderCanvas();
        setResult('노드 붙여넣기됨');
      }
      return;
    }

    if (!typing && event.key === 'Backspace') {
      if (state.selectedId) {
        event.preventDefault();
        removeNode(state.selectedId);
        renderCanvas();
        schedulePresencePublish(true);
        return;
      }
      if (state.selectedCommentId) {
        event.preventDefault();
        removeComment(state.selectedCommentId);
        renderCanvas();
        schedulePresencePublish(true);
        return;
      }
    }

    if (event.code === 'Space') {
      state.spaceDown = true;
      el.workspace.style.cursor = 'grab';
    }
    if (event.key === 'Escape') {
      state.connectionDraft = null;
      hideContextMenu();
      closePluginSelectMenu();
      closeWorkspaceTreeMenu();
      closeAutoCreateDialog();
      renderCanvas();
      schedulePresencePublish(true);
      return;
    }

    if (!typing && !selectingText) {
      const isArrowPan =
        event.key === 'ArrowLeft'
        || event.key === 'ArrowRight'
        || event.key === 'ArrowUp'
        || event.key === 'ArrowDown';
      if (isArrowPan) {
        event.preventDefault();
        const panStep = GRID_SIZE;
        if (event.key === 'ArrowLeft') state.panX += panStep;
        if (event.key === 'ArrowRight') state.panX -= panStep;
        if (event.key === 'ArrowUp') state.panY += panStep;
        if (event.key === 'ArrowDown') state.panY -= panStep;
        renderCanvas();
        schedulePresencePublish(true);
        return;
      }
    }
  });

  document.addEventListener('keyup', (event) => {
    if (event.code === 'Space') {
      state.spaceDown = false;
      if (!state.panning) el.workspace.style.cursor = 'default';
    }
  });

  el.workspace.addEventListener('mousedown', (event) => {
    const isLeftButton = event.button === 0;
    const emptyArea = event.target === el.workspace || event.target === el.canvas;
    if (emptyArea && isLeftButton) {
      event.preventDefault();
      if (document.activeElement instanceof HTMLElement) {
        document.activeElement.blur();
      }
      state.selectedId = null;
      state.selectedCommentId = null;
      hideContextMenu();
      renderCanvas();
      schedulePresencePublish(true);
      if (event.shiftKey) {
        const world = toWorldPosition(event.clientX, event.clientY);
        const startX = (event.ctrlKey ? snapGrid(world.x) : world.x);
        const startY = (event.ctrlKey ? snapGrid(world.y) : world.y);
        state.commentDraft = {
          startX,
          startY,
          currentX: startX,
          currentY: startY
        };
        return;
      }
      beginPan(event);
      el.workspace.style.cursor = 'grabbing';
    }
  });

  document.addEventListener('contextmenu', (event) => {
    const targetElement = event.target instanceof Element ? event.target : null;
    if (!targetElement || !el.workspace.contains(targetElement)) return;

    event.preventDefault();
    const commentEl = targetElement.closest('.comment-box');
    if (commentEl instanceof HTMLElement && commentEl.dataset.id) {
      state.selectedCommentId = commentEl.dataset.id;
      state.selectedId = null;
      const commentId = commentEl.dataset.id;
      showContextMenu(event.clientX, event.clientY, {
        extraActions: [
          {
            label: '그룹 삭제',
            onClick: () => removeComment(commentId)
          }
        ]
      });
      renderCanvas();
      return;
    }
    const portEl = targetElement.closest('.port');
    if (portEl instanceof HTMLElement) {
      const nodeId = portEl.dataset.nodeId || '';
      const portType = portEl.dataset.portType || '';
      const portClass = portEl.dataset.portClass || '';
      if (nodeId && portType && portClass) {
        showPortContextMenu(event.clientX, event.clientY, nodeId, portType, portClass);
        return;
      }
    }
    const nodeEl = targetElement.closest('.block-node');
    const inContextMenu = !!targetElement.closest('.context-menu');
    const emptyArea = !nodeEl && !inContextMenu;
    if (nodeEl && nodeEl.dataset.id) {
      state.selectedId = nodeEl.dataset.id;
      showNodeContextMenu(event.clientX, event.clientY, nodeEl.dataset.id);
      renderCanvas();
    } else if (emptyArea) {
      showContextMenu(event.clientX, event.clientY);
    } else {
      hideContextMenu();
    }
  });

  document.addEventListener('mousemove', (event) => {
    if (minimapDragging) {
      focusViewportFromMinimapClient(event.clientX, event.clientY);
      return;
    }

    state.mouseWorld = toWorldPosition(event.clientX, event.clientY);
    schedulePresencePublish(false);
    renderRemotePresenceLayer();

    if (state.commentDraft) {
      const world = toWorldPosition(event.clientX, event.clientY);
      const snap = event.ctrlKey;
      state.commentDraft.currentX = snap ? snapGrid(world.x) : world.x;
      state.commentDraft.currentY = snap ? snapGrid(world.y) : world.y;
      renderCanvas();
      scheduleCollaborativePublish(false);
      return;
    }

    if (state.resizingComment) {
      const comment = state.comments.find((it) => it.id === state.resizingComment.id);
      if (comment) {
        const r = state.resizingComment;
        const dx = (event.clientX - r.startX) / state.scale;
        const dy = (event.clientY - r.startY) / state.scale;
        let x = r.x;
        let y = r.y;
        let width = r.width;
        let height = r.height;
        const minW = 180;
        const minH = 120;
        const edge = r.edge;

        if (edge.includes('e')) {
          width = Math.max(minW, r.width + dx);
        }
        if (edge.includes('s')) {
          height = Math.max(minH, r.height + dy);
        }
        if (edge.includes('w')) {
          const rawX = r.x + dx;
          const rawW = r.width - dx;
          if (rawW >= minW) {
            x = rawX;
            width = rawW;
          } else {
            x = r.x + (r.width - minW);
            width = minW;
          }
        }
        if (edge.includes('n')) {
          const rawY = r.y + dy;
          const rawH = r.height - dy;
          if (rawH >= minH) {
            y = rawY;
            height = rawH;
          } else {
            y = r.y + (r.height - minH);
            height = minH;
          }
        }

        if (event.ctrlKey) {
          x = snapGrid(x);
          y = snapGrid(y);
          width = Math.max(minW, snapGrid(width));
          height = Math.max(minH, snapGrid(height));
        }

        if (
          Math.round(x) !== Math.round(comment.x)
          || Math.round(y) !== Math.round(comment.y)
          || Math.round(width) !== Math.round(comment.width)
          || Math.round(height) !== Math.round(comment.height)
        ) {
          r.moved = true;
        }
        comment.x = x;
        comment.y = y;
        comment.width = width;
        comment.height = height;
        renderCanvas();
        scheduleCollaborativePublish(false);
      }
      return;
    }

    if (state.draggingComment) {
      const comment = state.comments.find((it) => it.id === state.draggingComment.id);
      if (comment) {
        let nextX = state.draggingComment.commentX + ((event.clientX - state.draggingComment.startX) / state.scale);
        let nextY = state.draggingComment.commentY + ((event.clientY - state.draggingComment.startY) / state.scale);
        if (event.ctrlKey) {
          nextX = snapGrid(nextX);
          nextY = snapGrid(nextY);
        }
        if (Math.round(nextX) !== Math.round(comment.x) || Math.round(nextY) !== Math.round(comment.y)) {
          state.draggingComment.moved = true;
        }
        comment.x = nextX;
        comment.y = nextY;
        renderCanvas();
        scheduleCollaborativePublish(false);
      }
      return;
    }

    if (state.draggingNode) {
      const block = state.blocks.find((it) => it.id === state.draggingNode.id);
      if (block) {
        let nextX = state.draggingNode.nodeX + ((event.clientX - state.draggingNode.startX) / state.scale);
        let nextY = state.draggingNode.nodeY + ((event.clientY - state.draggingNode.startY) / state.scale);
        if (event.ctrlKey) {
          nextX = Math.round(nextX / GRID_SIZE) * GRID_SIZE;
          nextY = Math.round(nextY / GRID_SIZE) * GRID_SIZE;
        }
        if (Math.round(nextX) !== Math.round(block.x) || Math.round(nextY) !== Math.round(block.y)) {
          state.draggingNode.moved = true;
        }
        block.x = nextX;
        block.y = nextY;
        renderCanvas();
        scheduleCollaborativePublish(false);
      }
      return;
    }

    if (state.panning) {
      state.panX = state.panning.panX + (event.clientX - state.panning.startX);
      state.panY = state.panning.panY + (event.clientY - state.panning.startY);
      renderCanvas();
      return;
    }

    if (state.connectionDraft && state.linkingActive) {
      let target = resolveLinkTargetFromClient(event.clientX, event.clientY);
      if (target && target.id === state.connectionDraft.from) target = null;
      state.connectionDraft.snapTargetId = target?.id || null;
      state.connectionDraft.snapTargetPort = target?.port || null;
      updateSnapInputHighlightOnly();
      updateDraftPathOnly();
    }
  });

  document.addEventListener('mouseup', (event) => {
    if (minimapDragging) {
      minimapDragging = false;
      return;
    }

    if (state.commentDraft) {
      const draft = state.commentDraft;
      const x = Math.min(draft.startX, draft.currentX);
      const y = Math.min(draft.startY, draft.currentY);
      const width = Math.abs(draft.currentX - draft.startX);
      const height = Math.abs(draft.currentY - draft.startY);
      state.commentDraft = null;
      if (width >= 16 && height >= 16) {
        createComment(x, y, width, height);
      }
      renderCanvas();
    }

    if (state.connectionDraft && state.linkingActive) {
      const hovered = document.elementFromPoint(event.clientX, event.clientY);
      const resolved = resolveLinkTargetFromClient(event.clientX, event.clientY);
      let nextTargetId = state.connectionDraft.snapTargetId || resolved?.id || null;
      if (nextTargetId && nextTargetId === state.connectionDraft.from) nextTargetId = null;

      if (nextTargetId) {
        addLink(
          state.connectionDraft.from,
          nextTargetId,
          state.connectionDraft.kind || 'exec',
          state.connectionDraft.fromPortClass || 'out',
          state.connectionDraft.snapTargetPort || ''
        );
      } else {
        const hoveredElement = hovered instanceof Element ? hovered : null;
        const inWorkspace = !!hoveredElement && el.workspace.contains(hoveredElement);
        const blockedSurface = !!hoveredElement?.closest('.block-node, .port, .context-menu');
        if (inWorkspace && !blockedSurface) {
          showContextMenu(event.clientX, event.clientY, {
            connectFromId: state.connectionDraft.from,
            connectKind: state.connectionDraft.kind || 'exec',
            connectPortClass: state.connectionDraft.fromPortClass || 'out'
          });
        }
      }

      state.connectionDraft = null;
      state.linkingActive = false;
      renderCanvas();
    }
    const movedComment = !!state.draggingComment?.moved;
    const resizedComment = !!state.resizingComment?.moved;
    state.resizingComment = null;
    state.draggingComment = null;
    const movedNode = !!state.draggingNode?.moved;
    state.draggingNode = null;
    state.panning = null;
    document.body.style.userSelect = '';
    el.workspace.style.cursor = state.spaceDown ? 'grab' : 'default';
    if (movedComment) commitHistoryState();
    if (resizedComment) commitHistoryState();
    if (movedNode) commitHistoryState();
    schedulePresencePublish(true);
  });

  el.workspace.addEventListener('wheel', (event) => {
    event.preventDefault();
    hideContextMenu();
    const rect = el.workspace.getBoundingClientRect();
    const pointerX = event.clientX - rect.left;
    const pointerY = event.clientY - rect.top;
    const worldX = (pointerX - state.panX) / state.scale;
    const worldY = (pointerY - state.panY) / state.scale;

    const factor = event.deltaY < 0 ? 1.08 : 0.92;
    const scaleBounds = getScaleBounds();
    const lowerBound = Math.min(scaleBounds.min, state.scale);
    const nextScale = Math.min(scaleBounds.max, Math.max(lowerBound, state.scale * factor));
    if (nextScale === state.scale) return;

    state.scale = nextScale;
    state.panX = pointerX - (worldX * state.scale);
    state.panY = pointerY - (worldY * state.scale);
    renderCanvas();
  }, { passive: false });

  document.addEventListener('click', (event) => {
    if (!el.contextMenu.contains(event.target) && event.target !== el.workspace && event.target !== el.canvas) return;
    if (event.target === el.workspace || event.target === el.canvas) hideContextMenu();
  });
  document.addEventListener('mouseover', (event) => {
    const target = event.target instanceof Element ? event.target.closest('[data-tooltip]') : null;
    if (!(target instanceof HTMLElement)) return;
    const message = target.dataset.tooltip || '';
    if (!message.trim()) return;
    const detail = (target.dataset.tooltipReason || '').trim() || describePortIssue(target);
    showHoverTooltip(message, event.clientX, event.clientY, target, detail);
  });
  document.addEventListener('mousemove', (event) => {
    const target = event.target instanceof Element ? event.target.closest('[data-tooltip]') : null;
    if (!(target instanceof HTMLElement)) {
      hideHoverTooltip();
      return;
    }
    const message = target.dataset.tooltip || '';
    if (!message.trim()) {
      hideHoverTooltip();
      return;
    }
    const detail = (target.dataset.tooltipReason || '').trim() || describePortIssue(target);
    showHoverTooltip(message, event.clientX, event.clientY, target, detail);
  });
  document.addEventListener('mousedown', () => hideHoverTooltip());
  window.addEventListener('blur', () => hideHoverTooltip());
  ensureActionBarStructure();
  window.addEventListener('resize', () => {
    scheduleRelayoutRender();
    if (!el.autoCreateDialog.classList.contains('hidden')) {
      resizeAutoCreatePromptInput();
    }
  });

  window.addEventListener('beforeunload', () => {
    if (realtimeState.eventSource) {
      realtimeState.eventSource.close();
      realtimeState.eventSource = null;
    }
    if (realtimeState.presenceAnimationFrame) {
      cancelAnimationFrame(realtimeState.presenceAnimationFrame);
      realtimeState.presenceAnimationFrame = null;
    }
  });

  el.saveBtn.addEventListener('click', () => saveProject().catch((error) => setResult(String(error))));
}

async function init() {
  ensureActionBarStructure();
  migrateNativeTitleAttributes(document);
  loadWorkspaceState();
  await loadServerDraft();
  applyWorkspaceSnapshot(state.currentWorkspaceId || state.selectedWorkspaceId);
  renderWorkspaceTree();
  showCanvasView();
  saveWorkspaceState();
  commitHistoryState(false);
  installEvents();
  // Keep UI responsive: warm up AI in the background instead of blocking init.
  setTimeout(() => {
    ensureAiModelAssets({ silent: false })
      .then(() => ensureLocalAiRuntime({ silent: false }))
      .catch(() => {});
  }, 0);
  connectCollaborativeRealtime();
  if (document.fonts?.ready) {
    document.fonts.ready.then(() => scheduleRelayoutRender()).catch(() => {});
  }
  window.addEventListener('load', scheduleRelayoutRender, { once: true });
  await refreshPluginList();
  scheduleRelayoutRender();
  setActionBarExpanded(false);
  renderActionBarLogs();
  resizeAutoCreatePromptInput();
  requestAnimationFrame(() => {
    if (document.activeElement instanceof HTMLElement && document.activeElement !== document.body) {
      document.activeElement.blur();
    }
  });
  setResult('우클릭으로 이벤트/함수/연산/변수 노드를 추가하고 포트를 눌러 연결하세요.');
}

init().catch((error) => {
  setResult(String(error));
});
