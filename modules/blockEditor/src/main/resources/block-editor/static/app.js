const NODE_LIBRARY = {
  events: [
    { type: 'EVENT_ON_ENABLE', title: 'PluginEnableEvent' },
    { type: 'EVENT_ON_PLAYER_JOIN', title: 'PlayerJoinEvent' }
  ],
  functions: [
    { type: 'FUNCTION_SEND_MESSAGE', title: 'Player.sendMessage', defaultMessage: 'Hello from function' }
  ],
  operations: [
    { type: 'MATH_ADD', title: '+' }
  ],
  variables: [
    { type: 'VAR_TEXT', title: '문자열 변수', defaultValue: 'hello' },
    { type: 'VAR_INTEGER', title: '정수 변수', defaultValue: '0' },
    { type: 'VAR_DECIMAL', title: '실수 변수', defaultValue: '0.0' }
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
  presenceInFlight: false,
  peers: {},
  suspended: false
};
const SHOW_SELF_PRESENCE_DEBUG = false;

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
  saveBtn: document.getElementById('saveBtn'),
  workspace: document.getElementById('workspace'),
  workspaceToolbar: document.getElementById('workspaceToolbar'),
  canvas: document.getElementById('canvas'),
  worldInfo: document.getElementById('worldInfo'),
  contextMenu: document.getElementById('contextMenu')
};

function setResult(value) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  console.info(`[AerogelStudio] ${text}`);
}

function withPrivateKey(path) {
  const url = new URL(path, window.location.origin);
  if (PRIVATE_KEY) {
    url.searchParams.set('privatekey', PRIVATE_KEY);
  }
  return `${url.pathname}${url.search}`;
}

function isEventType(type) {
  return type === 'EVENT_ON_ENABLE' || type === 'EVENT_ON_PLAYER_JOIN';
}

function isActionType(type) {
  return type.startsWith('ACTION_') || type.startsWith('FUNCTION_') || type === 'ON_ENABLE_LOG' || type === 'ON_PLAYER_JOIN_MESSAGE' || type === 'ON_PLAYER_JOIN_SET_JOIN_MESSAGE';
}

function isOperationType(type) {
  return type === 'MATH_ADD';
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
  return type === 'VAR_TEXT' || type === 'VAR_INTEGER' || type === 'VAR_DECIMAL' || type === 'MATH_ADD';
}

function isDataInputType(type) {
  return type === 'FUNCTION_SEND_MESSAGE' || type === 'EVENT_ON_PLAYER_JOIN' || type === 'MATH_ADD';
}

function canStartLink(type) {
  return true;
}

function isDataKind(kind) {
  return kind === 'data' || String(kind).startsWith('data-');
}

function linkKindFromType(type) {
  if (type === 'VAR_TEXT') return 'data-text';
  if (type === 'VAR_INTEGER') return 'data-int';
  if (type === 'VAR_DECIMAL') return 'data-decimal';
  if (type === 'MATH_ADD') return 'data-any';
  if (isExecOutputType(type)) return 'exec';
  return 'none';
}

function targetPortClassForLink(kind, toType, preferredPortClass = '') {
  if (preferredPortClass) return preferredPortClass;
  if (isDataKind(kind) && toType === 'MATH_ADD') return 'in-data-a';
  if (isDataKind(kind) && toType === 'FUNCTION_SEND_MESSAGE') return 'in-data';
  if (isDataKind(kind) && toType === 'EVENT_ON_PLAYER_JOIN') return 'in-data';
  if (kind === 'player' && toType === 'FUNCTION_SEND_MESSAGE') return 'in-player';
  return 'in-exec';
}

function canLinkKindToType(kind, toType) {
  if (kind === 'data-text' || kind === 'data-int' || kind === 'data-decimal' || kind === 'data-any' || kind === 'data') {
    return isDataInputType(toType);
  }
  if (kind === 'exec') return isExecInputType(toType);
  if (kind === 'player') return toType === 'FUNCTION_SEND_MESSAGE';
  if (kind === 'context') return false;
  return false;
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
  if (type === 'VAR_TEXT') return '텍스트 값을 저장해 두는 칸';
  if (type === 'VAR_INTEGER') return '소수점 없는 숫자를 저장해 두는 칸';
  if (type === 'VAR_DECIMAL') return '소수점 있는 숫자를 저장해 두는 칸';
  if (type === 'FUNCTION_SEND_MESSAGE') return '플레이어에게 메시지를 보내는 함수';
  if (type === 'MATH_ADD') return '두 값을 더하거나 이어 붙이는 연산';
  return type;
}

function createNode(type, x, y) {
  const functionDef = NODE_LIBRARY.functions.find((it) => it.type === type);
  const variableDef = NODE_LIBRARY.variables.find((it) => it.type === type);
  const params = {};
  if (functionDef) params.message = functionDef.defaultMessage;
  if (type === 'EVENT_ON_PLAYER_JOIN') params.message = '';
  if (variableDef) {
    params.value = variableDef.defaultValue;
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
  if (!canLinkKindToType(resolvedKind, toNode.type)) return;
  if (hasDuplicateLink(fromId, toId, resolvedKind, fromPortClass, toPortClass)) return;
  state.links.push({
    from: fromId,
    to: toId,
    kind: resolvedKind,
    fromPortClass: fromPortClass || 'out',
    toPortClass: toPortClass || ''
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
      || (sourceKind === 'player' && accept === 'player');
    if (!acceptOk) return null;
    if (!(targetNode && canLinkKindToType(sourceKind, targetNode.type))) return null;
    return { id: targetId, port: targetPortClass };
  }

  const nodeElement = hovered instanceof Element ? hovered.closest('.block-node') : null;
  if (nodeElement?.dataset.id) {
    const targetNodeId = nodeElement.dataset.id;
    const targetNode = state.blocks.find((it) => it.id === targetNodeId);
    if (targetNode && canLinkKindToType(sourceKind, targetNode.type)) {
      const port = targetPortClassForLink(sourceKind, targetNode.type);
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

  for (const link of state.links) {
    const isDraftSourceLink = state.connectionDraft && state.linkingActive && link.from === state.connectionDraft.from;
    const fromNode = state.blocks.find((it) => it.id === link.from);
    const toNode = state.blocks.find((it) => it.id === link.to);
    if (!fromNode || !toNode) continue;
    const kind = link.kind || linkKindFromType(fromNode.type);
    const from = getPortCenterWorld(link.from, link.fromPortClass || 'out');
    const to = getPortCenterWorld(link.to, targetPortClassForLink(kind, toNode.type, link.toPortClass || ''));
    if (!from || !to) continue;
    const d = linkPath(from, to);

    const back = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    back.setAttribute('class', isDraftSourceLink ? 'link-outline-back' : 'link-back');
    back.setAttribute('d', d);
    svg.appendChild(back);

    const front = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    front.setAttribute('class', isDraftSourceLink ? 'link-outline-front' : 'link-front');
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

function renderComment(comment) {
  const box = document.createElement('div');
  box.className = `comment-box${state.selectedCommentId === comment.id ? ' selected' : ''}`;
  box.style.left = `${comment.x}px`;
  box.style.top = `${comment.y}px`;
  box.style.width = `${comment.width}px`;
  box.style.height = `${comment.height}px`;
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
    scheduleCollaborativePublish(false);
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
    scheduleCollaborativePublish(false);
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

function renderNode(node) {
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
  const canBeTarget = sourceId && node.id !== sourceId ? canLinkKindToType(sourceKind, node.type) : false;
  const isCandidateTarget = isLinking && canBeTarget;
  const isUnavailableTarget = isLinking && node.id !== sourceId && !canBeTarget;
  nodeEl.className = `block-node ${roleClass}${state.selectedId === node.id ? ' selected' : ''}${isUnavailableTarget ? ' non-link-target' : ''}`;
  nodeEl.style.left = `${node.x}px`;
  nodeEl.style.top = `${node.y}px`;
  nodeEl.dataset.id = node.id;

  const head = document.createElement('div');
  head.className = 'block-head';
  head.innerHTML = `
    <div>
      <div class="block-title">${titleOf(node.type)}</div>
      <div class="block-type">${typeLabelOf(node.type)}</div>
    </div>
  `;

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
    const valueInput = document.createElement(isTextVariable ? 'textarea' : 'input');
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
      scheduleCollaborativePublish(false);
    });

    body.appendChild(valueLabel);
    body.appendChild(valueInput);
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
    inData.title = 'message 입력';
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
  } else if (node.type === 'FUNCTION_SEND_MESSAGE') {
    leftPinGroup.classList.add('stack');

    const appendInPort = (label, accept, extraClass, connected) => {
      const inItem = document.createElement('div');
      inItem.className = 'pin-in-item';

      const inPort = document.createElement('div');
      inPort.className = `port in ${extraClass}${connected ? ' connected' : ''}`;
      inPort.title = `${label} 입력`;
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
    appendInPort('player', 'player', 'in-player', hasIncomingPlayer);
    appendInPort('message', 'data', 'in-data', hasIncomingData);
  } else if (node.type === 'MATH_ADD') {
    leftPinGroup.classList.add('stack');
    const appendAddInput = (label, portClass) => {
      const connected = state.links.some((it) => it.to === node.id && isDataKind(it.kind || 'exec') && (it.toPortClass || '') === portClass);
      const inItem = document.createElement('div');
      inItem.className = 'pin-in-item';
      const inPort = document.createElement('div');
      inPort.className = `port in ${portClass}${connected ? ' connected' : ''}`;
      inPort.title = `${label} 입력`;
      inPort.dataset.portType = 'in';
      inPort.dataset.accept = 'data';
      inPort.dataset.portClass = portClass;
      inPort.dataset.nodeId = node.id;
      if (isCandidateTarget && isDataKind(sourceKind)) inPort.classList.add('candidate');
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
    inPort.title = '실행 입력';
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
    outPort.title = isDataKind(kind)
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
  } else if (node.type === 'VAR_TEXT') {
    appendOutPort('값', 'data-text');
  } else if (node.type === 'VAR_INTEGER') {
    appendOutPort('값', 'data-int');
  } else if (node.type === 'VAR_DECIMAL') {
    appendOutPort('값', 'data-decimal');
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

function renderRemotePresenceLayer() {
  const staleBefore = Date.now() - 6000;
  const activePeers = Object.values(realtimeState.peers).filter((peer) => peer && peer.lastSeen >= staleBefore);
  for (const [clientId, peer] of Object.entries(realtimeState.peers)) {
    if (!peer || peer.lastSeen < staleBefore) delete realtimeState.peers[clientId];
  }
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
    const p = peer.presence || {};
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
  const bounds = getScaleBounds();
  if (state.scale > bounds.max) state.scale = bounds.max;

  el.canvas.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
  el.workspace.style.backgroundPosition = `${state.panX}px ${state.panY}px, ${state.panX}px ${state.panY}px, 0 0`;
  el.workspace.style.backgroundSize = `${24 * state.scale}px ${24 * state.scale}px, ${24 * state.scale}px ${24 * state.scale}px, auto`;

  el.canvas.innerHTML = '';
  for (const comment of state.comments) {
    renderComment(comment);
  }
  for (const block of state.blocks) {
    renderNode(block);
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
  renderRemotePresenceLayer();

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

function showContextMenu(clientX, clientY, options = {}) {
  const worldPos = toWorldPosition(clientX, clientY);
  state.contextMenuWorld = worldPos;
  const connectFromId = options.connectFromId || null;
  const connectKind = options.connectKind || null;
  const connectPortClass = options.connectPortClass || 'out';
  const connectFromType = connectFromId
    ? (state.blocks.find((it) => it.id === connectFromId)?.type || '')
    : '';

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

  const rect = el.workspace.getBoundingClientRect();
  el.contextMenu.style.left = `${Math.max(8, Math.min(rect.width - 236, clientX - rect.left))}px`;
  el.contextMenu.style.top = `${Math.max(8, Math.min(rect.height - 280, clientY - rect.top))}px`;
  el.contextMenu.classList.remove('hidden');
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

  const rect = el.workspace.getBoundingClientRect();
  el.contextMenu.style.left = `${Math.max(8, Math.min(rect.width - 236, clientX - rect.left))}px`;
  el.contextMenu.style.top = `${Math.max(8, Math.min(rect.height - 232, clientY - rect.top))}px`;
  el.contextMenu.classList.remove('hidden');
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

  const rect = el.workspace.getBoundingClientRect();
  el.contextMenu.style.left = `${Math.max(8, Math.min(rect.width - 236, clientX - rect.left))}px`;
  el.contextMenu.style.top = `${Math.max(8, Math.min(rect.height - 140, clientY - rect.top))}px`;
  el.contextMenu.classList.remove('hidden');
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

  const rect = el.workspace.getBoundingClientRect();
  el.contextMenu.style.left = `${Math.max(8, Math.min(rect.width - 236, clientX - rect.left))}px`;
  el.contextMenu.style.top = `${Math.max(8, Math.min(rect.height - 140, clientY - rect.top))}px`;
  el.contextMenu.classList.remove('hidden');
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
}

function redoHistory() {
  if (!historyState.redo.length) return;
  const nextSerialized = historyState.redo.pop();
  if (!nextSerialized) return;
  applyCapturedEditorState(nextSerialized);
  historyState.undo.push(nextSerialized);
  if (historyState.undo.length > HISTORY_LIMIT) historyState.undo.shift();
  historyState.lastSerialized = nextSerialized;
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
          if (!targetNode || !canLinkKindToType(restoredKind, targetNode.type)) {
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
    state.draggingComment = null;
    state.resizingComment = null;
    state.commentDraft = null;
    renderWorkspaceTree();
    renderCanvas();
    saveWorkspaceState();
    historyState.lastSerialized = JSON.stringify(captureEditorState());
  } finally {
    historyState.applying = false;
    realtimeState.suspended = false;
  }
}

async function publishCollaborativeStateNow() {
  if (realtimeState.suspended || !realtimeState.connected || realtimeState.publishInFlight) return;
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
  }
}

function scheduleCollaborativePublish(immediate = false) {
  scheduleWorkspacePersist(immediate);
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
  if (realtimeState.suspended || !realtimeState.connected || realtimeState.presenceInFlight) return;
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

    if (!typing && withCtrl && key === 'c') {
      if (copySelectedNodeToClipboard()) {
        event.preventDefault();
        setResult('노드 복사됨');
      }
      return;
    }
    if (!typing && withCtrl && key === 'v') {
      if (pasteNodeFromClipboard()) {
        event.preventDefault();
        renderCanvas();
        setResult('노드 붙여넣기됨');
      }
      return;
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
      renderCanvas();
      schedulePresencePublish(true);
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
      showCommentContextMenu(event.clientX, event.clientY, commentEl.dataset.id);
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

  window.addEventListener('beforeunload', () => {
    if (realtimeState.eventSource) {
      realtimeState.eventSource.close();
      realtimeState.eventSource = null;
    }
  });

  el.saveBtn.addEventListener('click', () => saveProject().catch((error) => setResult(String(error))));
}

async function init() {
  loadWorkspaceState();
  await loadServerDraft();
  applyWorkspaceSnapshot(state.currentWorkspaceId || state.selectedWorkspaceId);
  renderWorkspaceTree();
  showCanvasView();
  saveWorkspaceState();
  commitHistoryState(false);
  installEvents();
  connectCollaborativeRealtime();
  if (document.fonts?.ready) {
    document.fonts.ready.then(() => scheduleRelayoutRender()).catch(() => {});
  }
  window.addEventListener('load', scheduleRelayoutRender, { once: true });
  await refreshPluginList();
  scheduleRelayoutRender();
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
