const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const SubscriptionTree = require('./SubscriptionTree');
const DataStore = require('./DataStore');

const PORT = process.env.PORT || 3000;

const dataStore = new DataStore();
const subscriptionTree = new SubscriptionTree();
const clients = new Map();
let clientIdCounter = 0;

const server = http.createServer((req, res) => {
  let filePath = '.' + req.url;
  if (filePath === './') filePath = './client/index.html';

  const extname = path.extname(filePath);
  let contentType = 'text/html';
  switch (extname) {
    case '.js': contentType = 'application/javascript'; break;
    case '.css': contentType = 'text/css'; break;
    case '.json': contentType = 'application/json'; break;
    case '.html': contentType = 'text/html'; break;
  }

  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(content);
  });
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const clientId = 'client_' + (++clientIdCounter);
  clients.set(clientId, {
    ws,
    id: clientId,
    subscriptions: new Set(),
    lastGlobalVersion: 0,
    isAlive: true,
    pendingAcks: new Map()
  });

  console.log(`[${new Date().toISOString()}] Client connected: ${clientId}`);

  sendMessage(ws, { type: 'connected', clientId, serverTime: Date.now() });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      sendMessage(ws, { type: 'error', message: 'Invalid JSON' });
      return;
    }
    handleMessage(clientId, msg);
  });

  ws.on('pong', () => {
    const client = clients.get(clientId);
    if (client) client.isAlive = true;
  });

  ws.on('close', () => {
    handleDisconnect(clientId);
  });

  ws.on('error', (err) => {
    console.error(`Client ${clientId} error:`, err.message);
    handleDisconnect(clientId);
  });
});

function handleMessage(clientId, msg) {
  const client = clients.get(clientId);
  if (!client) return;
  const ws = client.ws;

  switch (msg.type) {
    case 'ping':
      sendMessage(ws, { type: 'pong', serverTime: Date.now(), clientTime: msg.clientTime });
      break;

    case 'subscribe':
      handleSubscribe(client, msg);
      break;

    case 'unsubscribe':
      handleUnsubscribe(client, msg);
      break;

    case 'get':
      handleGet(client, msg);
      break;

    case 'set':
      handleSet(client, msg);
      break;

    case 'merge':
      handleMerge(client, msg);
      break;

    case 'sync':
      handleSync(client, msg);
      break;

    case 'batch':
      handleBatch(client, msg);
      break;

    case 'ack':
      if (msg.requestId && client.pendingAcks.has(msg.requestId)) {
        client.pendingAcks.delete(msg.requestId);
      }
      break;

    default:
      sendMessage(ws, { type: 'error', message: 'Unknown message type: ' + msg.type });
  }
}

function handleSubscribe(client, msg) {
  const { path: subPath, requestId } = msg;
  const ws = client.ws;

  if (!subPath) {
    sendMessage(ws, { type: 'error', message: 'path required', requestId });
    return;
  }

  subscriptionTree.subscribe(subPath, ws);
  client.subscriptions.add(subPath);

  const snapshot = dataStore.getSnapshotWithVersion(subPath);
  client.lastGlobalVersion = Math.max(client.lastGlobalVersion, snapshot.globalVersion);

  sendMessage(ws, {
    type: 'subscribed',
    path: subPath,
    value: snapshot.value,
    version: snapshot.version,
    globalVersion: snapshot.globalVersion,
    requestId
  });
}

function handleUnsubscribe(client, msg) {
  const { path: subPath, requestId } = msg;
  const ws = client.ws;

  if (!subPath) {
    sendMessage(ws, { type: 'error', message: 'path required', requestId });
    return;
  }

  subscriptionTree.unsubscribe(subPath, ws);
  client.subscriptions.delete(subPath);

  sendMessage(ws, { type: 'unsubscribed', path: subPath, requestId });
}

function handleGet(client, msg) {
  const { path: getPath, requestId } = msg;
  const ws = client.ws;
  const snapshot = dataStore.getSnapshotWithVersion(getPath || '/');
  client.lastGlobalVersion = Math.max(client.lastGlobalVersion, snapshot.globalVersion);
  sendMessage(ws, {
    type: 'get_response',
    path: getPath || '/',
    value: snapshot.value,
    version: snapshot.version,
    globalVersion: snapshot.globalVersion,
    requestId
  });
}

function handleSet(client, msg) {
  const { path: setPath, value, clientTimestamp, requestId, writeId, conflictStrategy } = msg;
  const ws = client.ws;

  if (setPath === undefined) {
    sendMessage(ws, { type: 'error', message: 'path required', requestId });
    return;
  }

  const timestamp = clientTimestamp || Date.now();
  let logEntry;

  if (conflictStrategy === 'merge') {
    logEntry = dataStore.merge(setPath, value || {}, timestamp, client.id);
  } else {
    logEntry = dataStore.set(setPath, value, timestamp, client.id);
  }

  client.lastGlobalVersion = Math.max(client.lastGlobalVersion, logEntry.version);

  if (writeId) {
    sendMessage(ws, {
      type: 'write_ack',
      writeId,
      path: setPath,
      version: logEntry.version,
      hybridTimestamp: logEntry.hybridTimestamp,
      requestId
    });
  }

  broadcastChange(logEntry, client.id);
}

function handleMerge(client, msg) {
  const { path: mergePath, value, clientTimestamp, requestId, writeId } = msg;
  const ws = client.ws;

  if (mergePath === undefined) {
    sendMessage(ws, { type: 'error', message: 'path required', requestId });
    return;
  }

  const timestamp = clientTimestamp || Date.now();
  const logEntry = dataStore.merge(mergePath, value || {}, timestamp, client.id);

  client.lastGlobalVersion = Math.max(client.lastGlobalVersion, logEntry.version);

  if (writeId) {
    sendMessage(ws, {
      type: 'write_ack',
      writeId,
      path: mergePath,
      version: logEntry.version,
      hybridTimestamp: logEntry.hybridTimestamp,
      requestId
    });
  }

  broadcastChange(logEntry, client.id);
}

function handleSync(client, msg) {
  const { lastKnownGlobalVersion, pendingWrites, requestId } = msg;
  const ws = client.ws;

  const serverVersion = dataStore.globalVersion;
  const conflicts = [];
  const appliedWriteIds = [];
  const serverValues = new Map();

  if (pendingWrites && Array.isArray(pendingWrites)) {
    for (const write of pendingWrites) {
      const { path: writePath, value, clientTimestamp, writeId, op = 'set' } = write;
      const currentVersion = dataStore.getPathVersion(writePath);

      let logEntry;
      const timestamp = clientTimestamp || Date.now();

      if (op === 'merge') {
        logEntry = dataStore.merge(writePath, value || {}, timestamp, client.id);
      } else {
        logEntry = dataStore.set(writePath, value, timestamp, client.id);
      }

      appliedWriteIds.push(writeId);

      if (lastKnownGlobalVersion > 0 && currentVersion > lastKnownGlobalVersion) {
        conflicts.push({
          path: writePath,
          writeId,
          resolved: true,
          strategy: 'last_write_wins',
          serverVersion: currentVersion,
          finalValue: dataStore.get(writePath)
        });
      }

      broadcastChange(logEntry, client.id);
    }
  }

  const changes = lastKnownGlobalVersion > 0
    ? dataStore.getChangesSinceVersion(lastKnownGlobalVersion)
    : [];

  for (const subPath of client.subscriptions) {
    serverValues.set(subPath, dataStore.getSnapshotWithVersion(subPath));
  }

  client.lastGlobalVersion = serverVersion;

  const serverValuesObj = {};
  for (const [p, v] of serverValues) {
    serverValuesObj[p] = v;
  }

  sendMessage(ws, {
    type: 'sync_response',
    serverGlobalVersion: serverVersion,
    appliedWriteIds,
    conflicts,
    changesSinceLast: changes.map(c => ({
      id: c.id,
      path: c.path,
      value: c.value,
      version: c.version,
      hybridTimestamp: c.hybridTimestamp,
      clientId: c.clientId
    })),
    subscriptionSnapshots: serverValuesObj,
    requestId
  });
}

function handleBatch(client, msg) {
  const { operations, requestId } = msg;
  if (!Array.isArray(operations)) return;

  const results = [];
  for (const op of operations) {
    switch (op.type) {
      case 'set':
        const setEntry = dataStore.set(op.path, op.value, op.clientTimestamp || Date.now(), client.id);
        broadcastChange(setEntry, client.id);
        results.push({ type: 'set', path: op.path, version: setEntry.version });
        break;
      case 'merge':
        const mergeEntry = dataStore.merge(op.path, op.value || {}, op.clientTimestamp || Date.now(), client.id);
        broadcastChange(mergeEntry, client.id);
        results.push({ type: 'merge', path: op.path, version: mergeEntry.version });
        break;
      case 'remove':
        const remEntry = dataStore.set(op.path, null, op.clientTimestamp || Date.now(), client.id);
        broadcastChange(remEntry, client.id);
        results.push({ type: 'remove', path: op.path, version: remEntry.version });
        break;
    }
  }

  sendMessage(client.ws, { type: 'batch_response', results, requestId });
}

function broadcastChange(logEntry, originClientId) {
  const subscribers = subscriptionTree.getMatchingSubscribers(logEntry.path);
  const notificationsByPath = new Map();

  for (const subWs of subscribers) {
    let targetClient = null;
    for (const [cid, c] of clients) {
      if (c.ws === subWs) {
        targetClient = c;
        break;
      }
    }
    if (!targetClient) continue;

    const affected = thisPathMatchesSubscription(logEntry.path, targetClient.subscriptions);
    for (const subPath of affected) {
      const key = subPath + '|' + targetClient.id;
      if (!notificationsByPath.has(key)) {
        notificationsByPath.set(key, {
          client: targetClient,
          subscriptionPath: subPath,
          changedPaths: new Set()
        });
      }
      notificationsByPath.get(key).changedPaths.add(logEntry.path);
    }
  }

  for (const { client, subscriptionPath, changedPaths } of notificationsByPath.values()) {
    const newValue = dataStore.get(subscriptionPath);
    const version = dataStore.getPathVersion(subscriptionPath);

    sendMessage(client.ws, {
      type: 'change',
      subscriptionPath,
      changedPaths: Array.from(changedPaths),
      value: newValue,
      version,
      globalVersion: logEntry.version,
      changeOrigin: client.id === originClientId ? 'self' : 'remote',
      changeId: logEntry.id,
      hybridTimestamp: logEntry.hybridTimestamp,
      originClientId
    });
  }
}

function thisPathMatchesSubscription(changedPath, subscriptions) {
  const matched = [];
  const changedParts = splitPath(changedPath);
  for (const sub of subscriptions) {
    const subParts = splitPath(sub);
    if (isMatch(subParts, changedParts)) {
      matched.push(sub);
    }
  }
  return matched;
}

function splitPath(p) {
  if (p === '' || p === '/') return [];
  return p.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
}

function isMatch(subParts, changedParts) {
  if (subParts.length > changedParts.length) {
    const prefix = changedParts.slice(0, subParts.length);
    return arraysEqual(prefix, subParts);
  }
  let i = 0;
  for (; i < subParts.length; i++) {
    if (i >= changedParts.length) return false;
    if (subParts[i] === '*') continue;
    if (subParts[i] !== changedParts[i]) return false;
  }
  return true;
}

function arraysEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function handleDisconnect(clientId) {
  const client = clients.get(clientId);
  if (!client) return;
  subscriptionTree.unsubscribeAll(client.ws);
  clients.delete(clientId);
  console.log(`[${new Date().toISOString()}] Client disconnected: ${clientId}`);
}

function sendMessage(ws, msg) {
  if (ws.readyState !== 1) return;
  try {
    ws.send(JSON.stringify(msg));
  } catch (e) {
    console.error('Send error:', e.message);
  }
}

const heartbeatInterval = setInterval(() => {
  for (const [clientId, client] of clients) {
    if (!client.isAlive) {
      handleDisconnect(clientId);
      try { client.ws.terminate(); } catch (e) {}
      continue;
    }
    client.isAlive = false;
    try { client.ws.ping(); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════╗
║   Realtime Sync Engine - Server Started                 ║
╠══════════════════════════════════════════════════════════╣
║   HTTP & WebSocket: http://localhost:${PORT}               ║
║   Demo:    http://localhost:${PORT}/                      ║
║                                                          ║
║   Features:                                              ║
║     • Subscription tree (Trie) for efficient matching   ║
║     • Wildcard path support (*)                          ║
║     • Version-based change log for offline sync          ║
║     • Hybrid timestamp for LWW conflict resolution       ║
║     • Parent/descendant path broadcast                   ║
╚══════════════════════════════════════════════════════════╝
  `);
});
