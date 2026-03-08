const NODE_LIBRARY = {
  events: [
    { type: 'EVENT_ON_ENABLE', title: 'PluginEnableEvent' },
    { type: 'EVENT_ON_PLAYER_JOIN', title: 'PlayerJoinEvent' }
  ],
  functions: [
    { type: 'FUNCTION_SEND_MESSAGE', title: 'Player.sendMessage', defaultMessage: 'Hello from function' }
  ],
  variables: [
    { type: 'VAR_TEXT', title: '문자열 변수', defaultValue: 'hello' },
    { type: 'VAR_INTEGER', title: '정수 변수', defaultValue: '0' },
    { type: 'VAR_DECIMAL', title: '실수 변수', defaultValue: '0.0' }
  ]
};

const typeTitleMap = new Map([...NODE_LIBRARY.events, ...NODE_LIBRARY.functions, ...NODE_LIBRARY.variables].map((it) => [it.type, it.title]));

const state = {
  blocks: [],
  links: [],
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
  selectedId: null,
  panX: 0,
  panY: 0,
  scale: 1,
  draggingNode: null,
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
  contextMenuWorld: { x: 0, y: 0 }
};

const BLOCK_NODE_WIDTH = 260;
const BLOCK_NODE_ESTIMATED_HEIGHT = 132;
const WORKSPACE_STORAGE_KEY = 'aerogel.blockEditor.workspaces.v1';

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
  addCodeMenuBtn: document.getElementById('addCodeMenuBtn'),
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
  codeEditorPane: document.getElementById('codeEditorPane'),
  standaloneCodeEditor: document.getElementById('standaloneCodeEditor'),
  canvas: document.getElementById('canvas'),
  worldInfo: document.getElementById('worldInfo'),
  contextMenu: document.getElementById('contextMenu')
};

function setResult(value) {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  console.info(`[AerogelStudio] ${text}`);
}

function isEventType(type) {
  return type === 'EVENT_ON_ENABLE' || type === 'EVENT_ON_PLAYER_JOIN';
}

function isActionType(type) {
  return type.startsWith('ACTION_') || type.startsWith('FUNCTION_') || type === 'ON_ENABLE_LOG' || type === 'ON_PLAYER_JOIN_MESSAGE' || type === 'ON_PLAYER_JOIN_SET_JOIN_MESSAGE';
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
  return type === 'VAR_TEXT';
}

function isDataInputType(type) {
  return type === 'FUNCTION_SEND_MESSAGE' || type === 'EVENT_ON_PLAYER_JOIN';
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
  if (isExecOutputType(type)) return 'exec';
  return 'none';
}

function targetPortClassForLink(kind, toType) {
  if (isDataKind(kind) && toType === 'FUNCTION_SEND_MESSAGE') return 'in-data';
  if (isDataKind(kind) && toType === 'EVENT_ON_PLAYER_JOIN') return 'in-data';
  if (kind === 'player' && toType === 'FUNCTION_SEND_MESSAGE') return 'in-player';
  return 'in-exec';
}

function canLinkKindToType(kind, toType) {
  if (kind === 'data-text') return isDataInputType(toType);
  if (kind === 'data-int' || kind === 'data-decimal') return false;
  if (kind === 'data') return isDataInputType(toType); // legacy
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
  renderCanvas();
  return node;
}

function linkPath(from, to) {
  const dx = Math.max(60, Math.abs(to.x - from.x) * 0.5);
  const c1x = from.x + dx;
  const c2x = to.x - dx;
  return `M ${from.x} ${from.y} C ${c1x} ${from.y}, ${c2x} ${to.y}, ${to.x} ${to.y}`;
}

function removeNode(nodeId) {
  state.blocks = state.blocks.filter((it) => it.id !== nodeId);
  state.links = state.links.filter((it) => it.from !== nodeId && it.to !== nodeId);
  if (state.selectedId === nodeId) state.selectedId = null;
}

function hasDuplicateLink(fromId, toId, kind) {
  return state.links.some((it) => it.from === fromId && it.to === toId && (it.kind || 'exec') === (kind || 'exec'));
}

function addLink(fromId, toId, kind, fromPortClass) {
  if (!fromId || !toId || fromId === toId) return;
  const fromNode = state.blocks.find((it) => it.id === fromId);
  const toNode = state.blocks.find((it) => it.id === toId);
  if (!fromNode || !toNode) return;
  const resolvedKind = kind || linkKindFromType(fromNode.type);
  if (!canLinkKindToType(resolvedKind, toNode.type)) return;
  if (hasDuplicateLink(fromId, toId, resolvedKind)) return;
  state.links.push({ from: fromId, to: toId, kind: resolvedKind, fromPortClass: fromPortClass || 'out' });
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
    const acceptOk = (sourceKind === 'exec' && accept === 'exec')
      || (isDataKind(sourceKind) && accept === 'data')
      || (sourceKind === 'player' && accept === 'player');
    if (!acceptOk) return null;
    if (!(targetNode && canLinkKindToType(sourceKind, targetNode.type))) return null;
    if (accept === 'data') return { id: targetId, port: 'in-data' };
    if (accept === 'player') return { id: targetId, port: 'in-player' };
    return { id: targetId, port: 'in-exec' };
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
    const to = getPortCenterWorld(link.to, targetPortClassForLink(kind, toNode.type));
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
  const roleClass = isEventType(node.type) ? 'event' : 'action';
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
  } else if (!isEventType(node.type)) {
    const inPort = document.createElement('div');
    const outlinedInput = state.connectionDraft && state.linkingActive && (state.connectionDraft.previousTargets || []).includes(node.id);
    inPort.className = `port in in-exec${hasIncomingExec ? ' connected' : ''}${outlinedInput ? ' outline' : ''}`;
    inPort.title = '실행 입력';
    inPort.dataset.portType = 'in';
    inPort.dataset.accept = 'exec';
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
  } else {
    appendOutPort('다음', 'exec');
  }

  pinRow.appendChild(leftPinGroup);
  pinRow.appendChild(rightPinGroup);

  nodeEl.addEventListener('mousedown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (target?.closest('input, textarea, select, button')) return;
    state.selectedId = node.id;
    hideContextMenu();
    renderCanvas();
  });

  head.addEventListener('mousedown', (event) => {
    if (event.button !== 0) return;
    event.stopPropagation();
    state.draggingNode = {
      id: node.id,
      startX: event.clientX,
      startY: event.clientY,
      nodeX: node.x,
      nodeY: node.y
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

function renderCanvas() {
  const bounds = getScaleBounds();
  if (state.scale > bounds.max) state.scale = bounds.max;

  el.canvas.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
  el.workspace.style.backgroundPosition = `${state.panX}px ${state.panY}px, ${state.panX}px ${state.panY}px, 0 0`;
  el.workspace.style.backgroundSize = `${24 * state.scale}px ${24 * state.scale}px, ${24 * state.scale}px ${24 * state.scale}px, auto`;

  el.canvas.innerHTML = '';
  for (const block of state.blocks) {
    renderNode(block);
  }
  renderLinksLayer();

  el.worldInfo.textContent = `x: ${Math.round(state.panX)}, y: ${Math.round(state.panY)}, 배율: ${(state.scale * 100).toFixed(0)}%`;
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
      ...NODE_LIBRARY.variables.map((it) => ({ ...it, group: '변수', groupKey: 'variable' }))
    ].filter((it) => canLinkKindToType(connectKind || linkKindFromType(connectFromType), it.type))
    : [
      ...NODE_LIBRARY.events.map((it) => ({ ...it, group: '이벤트', groupKey: 'event' })),
      ...NODE_LIBRARY.functions.map((it) => ({ ...it, group: '함수', groupKey: 'function' })),
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

    const groups = ['이벤트', '함수', '변수'];
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
            addLink(connectFromId, created.id, connectKind || linkKindFromType(connectFromType), connectPortClass);
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
  title.textContent = 'Node';
  el.contextMenu.appendChild(title);

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

function saveWorkspaceState() {
  const payload = {
    items: state.workspaceItems,
    snapshots: state.workspaceSnapshots,
    selectedWorkspaceId: state.selectedWorkspaceId,
    currentWorkspaceId: state.currentWorkspaceId
  };
  localStorage.setItem(WORKSPACE_STORAGE_KEY, JSON.stringify(payload));
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function snapshotCurrentWorkspace() {
  const current = state.currentWorkspaceId ? getWorkspaceById(state.currentWorkspaceId) : null;
  if (!current || current.kind !== 'workspace') return;
  state.workspaceSnapshots[current.id] = {
    blocks: cloneJson(state.blocks),
    links: cloneJson(state.links),
    panX: state.panX,
    panY: state.panY,
    scale: state.scale,
    nextId: state.nextId
  };
}

function applyWorkspaceSnapshot(workspaceId) {
  const selected = workspaceId ? getWorkspaceById(workspaceId) : null;
  if (!selected || selected.kind !== 'workspace') return;
  state.currentWorkspaceId = workspaceId;
  const snapshot = state.workspaceSnapshots[workspaceId];
  if (!snapshot) {
    state.blocks = [];
    state.links = [];
    state.nextId = 1;
    state.selectedId = null;
    state.panX = 0;
    state.panY = 0;
    state.scale = 1;
    state.connectionDraft = null;
    renderCanvas();
    return;
  }
  state.blocks = Array.isArray(snapshot.blocks) ? cloneJson(snapshot.blocks) : [];
  state.links = Array.isArray(snapshot.links) ? cloneJson(snapshot.links) : [];
  state.nextId = Number.isFinite(snapshot.nextId) ? snapshot.nextId : (state.blocks.reduce((acc, it) => {
    const numeric = Number(String(it.id).replace(/^n/, ''));
    return Number.isFinite(numeric) ? Math.max(acc, numeric + 1) : acc;
  }, 1));
  state.selectedId = state.blocks[0]?.id ?? null;
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

let standaloneCodeEditor = null;
let codeEditorBindingId = null;
let syncingCodeEditor = false;
let monacoReadyPromise = null;

function ensureMonacoReady() {
  if (window.monaco?.editor) return Promise.resolve(window.monaco);
  if (monacoReadyPromise) return monacoReadyPromise;
  monacoReadyPromise = new Promise((resolve, reject) => {
    if (!window.require) {
      reject(new Error('Monaco loader not found'));
      return;
    }
    window.require.config({
      paths: {
        vs: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.52.2/min/vs'
      }
    });
    window.require(['vs/editor/editor.main'], () => {
      if (window.monaco?.editor) resolve(window.monaco);
      else reject(new Error('Monaco init failed'));
    }, reject);
  });
  return monacoReadyPromise;
}

async function ensureStandaloneCodeEditor() {
  if (standaloneCodeEditor) return standaloneCodeEditor;
  const monaco = await ensureMonacoReady();
  const hasKotlin = monaco.languages.getLanguages().some((it) => it.id === 'kotlin');
  monaco.editor.defineTheme('aerogel-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: '', foreground: 'D8DADF' },
      { token: 'comment', foreground: '7F848E' },
      { token: 'keyword', foreground: 'C9AB6B' },
      { token: 'string', foreground: 'A8C28F' },
      { token: 'number', foreground: 'C79C8A' },
      { token: 'type.identifier', foreground: 'B8BCC8' },
      { token: 'delimiter', foreground: '9096A2' }
    ],
    colors: {
      'editor.background': '#111216',
      'editor.foreground': '#D8DADF',
      'editorLineNumber.foreground': '#5D6370',
      'editorLineNumber.activeForeground': '#9BA2B1',
      'editorCursor.foreground': '#D9DCE2',
      'editor.selectionBackground': '#2B303A',
      'editor.inactiveSelectionBackground': '#222730',
      'editorIndentGuide.background1': '#2A2F39',
      'editorIndentGuide.activeBackground1': '#424A58',
      'editorGutter.background': '#111216'
    }
  });
  standaloneCodeEditor = monaco.editor.create(el.standaloneCodeEditor, {
    value: '',
    language: hasKotlin ? 'kotlin' : 'java',
    theme: 'aerogel-dark',
    minimap: { enabled: false },
    automaticLayout: true,
    fontSize: 13,
    mouseWheelZoom: true,
    lineNumbers: 'on',
    wordWrap: 'on',
    scrollBeyondLastLine: false
  });
  standaloneCodeEditor.onDidChangeModelContent(() => {
    if (syncingCodeEditor) return;
    if (!codeEditorBindingId) return;
    const item = getWorkspaceById(codeEditorBindingId);
    if (!item || item.kind !== 'code') return;
    item.code = standaloneCodeEditor.getValue();
    saveWorkspaceState();
  });
  return standaloneCodeEditor;
}

function showCanvasView() {
  el.workspaceToolbar.classList.remove('hidden');
  el.workspace.classList.remove('hidden');
  el.codeEditorPane.classList.add('hidden');
  renderCanvas();
}

async function showCodeEditorView(codeId) {
  const item = getWorkspaceById(codeId);
  if (!item || item.kind !== 'code') return;
  const editor = await ensureStandaloneCodeEditor();
  codeEditorBindingId = codeId;
  el.workspaceToolbar.classList.add('hidden');
  el.workspace.classList.add('hidden');
  el.codeEditorPane.classList.remove('hidden');
  syncingCodeEditor = true;
  editor.setValue(item.code || "// Kotlin 코드 작성\n");
  syncingCodeEditor = false;
  editor.layout();
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
      if (kind === 'folder') return it.kind === 'folder';
      if (isLeafWorkspaceKind(kind)) return isLeafWorkspaceKind(it.kind);
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

function workspaceKindRank(kind) {
  if (kind === 'folder') return 0;
  if (kind === 'workspace') return 1;
  if (kind === 'code') return 2;
  return 3;
}

function isLeafWorkspaceKind(kind) {
  return kind === 'workspace' || kind === 'code';
}

function nextWorkspaceOrder(parentId, kind) {
  const siblings = state.workspaceItems.filter((it) => {
    if ((it.parentId || null) !== (parentId || null)) return false;
    if (kind === 'folder') return it.kind === 'folder';
    if (isLeafWorkspaceKind(kind)) return isLeafWorkspaceKind(it.kind);
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
      if (item.kind === 'code') {
        toggle.className = 'workspace-code-icon';
        toggle.style.background = 'url("/static/code-item.svg") no-repeat center / 12px 12px';
      } else {
        toggle.className = isFolder ? 'workspace-toggle' : 'workspace-dot';
      }
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
  try {
    const raw = localStorage.getItem(WORKSPACE_STORAGE_KEY);
  if (!raw) {
      state.workspaceItems = [];
      state.workspaceSnapshots = {};
      state.selectedWorkspaceId = null;
      ensureWorkspaceDefaults();
      saveWorkspaceState();
      return;
    }
    const parsed = JSON.parse(raw);
    const items = Array.isArray(parsed.items) ? parsed.items : [];
    const snapshots = parsed && typeof parsed.snapshots === 'object' && parsed.snapshots
      ? parsed.snapshots
      : {};
    state.workspaceItems = items
      .filter((it) => it && (it.kind === 'folder' || it.kind === 'workspace' || it.kind === 'code') && typeof it.name === 'string')
      .map((it) => ({
        id: typeof it.id === 'string'
          ? it.id
          : makeWorkspaceId(it.kind === 'folder' ? 'fld' : (it.kind === 'code' ? 'cd' : 'ws')),
        kind: it.kind,
        parentId: typeof it.parentId === 'string' ? it.parentId : null,
        name: it.name.trim() || (it.kind === 'folder' ? '새 폴더' : (it.kind === 'code' ? '새 코드' : '새 작업 공간')),
        expanded: it.kind === 'folder' ? it.expanded !== false : undefined,
        order: Number.isFinite(it.order) ? it.order : 0
      }));
    state.selectedWorkspaceId = typeof parsed.selectedWorkspaceId === 'string' ? parsed.selectedWorkspaceId : null;
    state.currentWorkspaceId = typeof parsed.currentWorkspaceId === 'string' ? parsed.currentWorkspaceId : null;
    state.workspaceSnapshots = {};
    for (const [id, snapshot] of Object.entries(snapshots)) {
      if (!snapshot || typeof snapshot !== 'object') continue;
      state.workspaceSnapshots[id] = {
        blocks: Array.isArray(snapshot.blocks) ? snapshot.blocks : [],
        links: Array.isArray(snapshot.links) ? snapshot.links : [],
        panX: Number.isFinite(snapshot.panX) ? snapshot.panX : 0,
        panY: Number.isFinite(snapshot.panY) ? snapshot.panY : 0,
        scale: Number.isFinite(snapshot.scale) ? snapshot.scale : 1,
        nextId: Number.isFinite(snapshot.nextId) ? snapshot.nextId : 1
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
  } catch (error) {
    state.workspaceItems = [];
    state.workspaceSnapshots = {};
    state.selectedWorkspaceId = null;
    state.currentWorkspaceId = null;
    ensureWorkspaceDefaults();
    normalizeAllWorkspaceOrders();
  }
}

async function addWorkspaceItem(kind) {
  const selected = state.selectedWorkspaceId ? getWorkspaceById(state.selectedWorkspaceId) : null;
  const parentId = selected?.kind === 'folder' ? selected.id : (selected?.parentId || null);
  const name = await requestNameDialog(
    kind === 'folder' ? '폴더 이름 입력' : (kind === 'code' ? '코드 이름 입력' : '작업 공간 이름 입력'),
    kind === 'folder' ? '예: 콘텐츠' : (kind === 'code' ? '예: 경제 시스템 코드' : '예: 기본 시스템')
  );
  if (!name || !name.trim()) return;

  const id = makeWorkspaceId(kind === 'folder' ? 'fld' : (kind === 'code' ? 'cd' : 'ws'));
  const item = {
    id,
    kind,
    parentId,
    name: name.trim(),
    order: nextWorkspaceOrder(parentId, kind)
  };
  if (kind === 'code') item.code = "// Kotlin 코드 작성\n";
  if (kind === 'folder') item.expanded = true;
  state.workspaceItems.push(item);
  if (kind === 'workspace') {
    state.workspaceSnapshots[id] = {
      blocks: [],
      links: [],
      panX: 0,
      panY: 0,
      scale: 1,
      nextId: 1
    };
    selectWorkspace(id);
    return;
  } else if (kind === 'code') {
    snapshotCurrentWorkspace();
    state.selectedWorkspaceId = id;
    saveWorkspaceState();
    renderWorkspaceTree();
    showCodeEditorView(id).catch((error) => setResult(String(error)));
    return;
  }
  saveWorkspaceState();
  renderWorkspaceTree();
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
}

function moveItemRelative(itemId, targetItemId, placeAfter) {
  const item = getWorkspaceById(itemId);
  const target = getWorkspaceById(targetItemId);
  if (!item || !target) return;
  const sameGroup = (item.kind === 'folder' && target.kind === 'folder')
    || (isLeafWorkspaceKind(item.kind) && isLeafWorkspaceKind(target.kind));
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
}

async function renameWorkspaceItem(itemId) {
  const item = getWorkspaceById(itemId);
  if (!item) return;
  const nextName = await requestNameDialog(
    item.kind === 'folder' ? '폴더 이름 변경' : (item.kind === 'code' ? '코드 이름 변경' : '작업 공간 이름 변경'),
    '새 이름 입력',
    item.name
  );
  if (!nextName || !nextName.trim()) return;
  item.name = nextName.trim();
  saveWorkspaceState();
  renderWorkspaceTree();
}

async function refreshPluginList() {
  const response = await fetch('/api/block-editor/plugins');
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

  const response = await fetch(`/api/block-editor/load?jar=${encodeURIComponent(jar)}`);
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
      fromPortClass: typeof it.fromPortClass === 'string' ? it.fromPortClass : undefined
    }));

  state.nextId = state.blocks.reduce((acc, it) => {
    const numeric = Number(String(it.id).replace(/^n/, ''));
    return Number.isFinite(numeric) ? Math.max(acc, numeric + 1) : acc;
  }, 1);

  state.selectedId = state.blocks[0]?.id ?? null;
  state.panX = 0;
  state.panY = 0;
  state.scale = 1;
  state.connectionDraft = null;
  hideContextMenu();
  snapshotCurrentWorkspace();
  saveWorkspaceState();
  renderCanvas();
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
      fromPortClass: link.fromPortClass
    }))
  };

  setResult('저장 중...');
  const response = await fetch('/api/block-editor/save', {
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
  el.addCodeMenuBtn.addEventListener('click', async () => {
    closeWorkspaceTreeMenu();
    await addWorkspaceItem('code');
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
    } else {
      snapshotCurrentWorkspace();
      state.selectedWorkspaceId = item.id;
      saveWorkspaceState();
      renderWorkspaceTree();
      showCodeEditorView(item.id).catch((error) => setResult(String(error)));
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
      && (
        targetKind === state.draggingItemKind
        || (isLeafWorkspaceKind(targetKind) && isLeafWorkspaceKind(state.draggingItemKind))
      )
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
      } else {
        snapshotCurrentWorkspace();
        state.selectedWorkspaceId = targetId;
        saveWorkspaceState();
        renderWorkspaceTree();
        if (item?.kind === 'code') showCodeEditorView(targetId).catch((error) => setResult(String(error)));
      }
    }
    showWorkspaceTreeMenu(event.clientX, event.clientY, targetId);
  });

  document.addEventListener('mousedown', (event) => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (!el.pluginSelectField.contains(target)) closePluginSelectMenu();
    if (!el.workspaceTreeMenu.contains(target)) closeWorkspaceTreeMenu();
  });

  document.addEventListener('keydown', (event) => {
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
      hideContextMenu();
      beginPan(event);
      el.workspace.style.cursor = 'grabbing';
    }
  });

  document.addEventListener('contextmenu', (event) => {
    const targetElement = event.target instanceof Element ? event.target : null;
    if (!targetElement || !el.workspace.contains(targetElement)) return;

    event.preventDefault();
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

    if (state.draggingNode) {
      const block = state.blocks.find((it) => it.id === state.draggingNode.id);
      if (block) {
        block.x = state.draggingNode.nodeX + ((event.clientX - state.draggingNode.startX) / state.scale);
        block.y = state.draggingNode.nodeY + ((event.clientY - state.draggingNode.startY) / state.scale);
        renderCanvas();
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
          state.connectionDraft.fromPortClass || 'out'
        );
      } else {
        const emptyArea = hovered === el.workspace || hovered === el.canvas;
        if (emptyArea) {
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
    state.draggingNode = null;
    state.panning = null;
    document.body.style.userSelect = '';
    el.workspace.style.cursor = state.spaceDown ? 'grab' : 'default';
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

  el.saveBtn.addEventListener('click', () => saveProject().catch((error) => setResult(String(error))));
}

async function init() {
  loadWorkspaceState();
  applyWorkspaceSnapshot(state.currentWorkspaceId || state.selectedWorkspaceId);
  renderWorkspaceTree();
  if (getWorkspaceById(state.selectedWorkspaceId)?.kind === 'code') {
    await showCodeEditorView(state.selectedWorkspaceId).catch((error) => setResult(String(error)));
  } else {
    showCanvasView();
  }
  saveWorkspaceState();
  installEvents();
  await refreshPluginList();
  setResult('우클릭으로 이벤트/함수/변수 노드를 추가하고 포트를 눌러 연결하세요.');
}

init().catch((error) => {
  setResult(String(error));
});
