(function (global) {
  'use strict';

  class EventEmitter {
    constructor() {
      this._listeners = new Map();
    }
    on(event, callback) {
      if (!this._listeners.has(event)) this._listeners.set(event, new Set());
      this._listeners.get(event).add(callback);
      return () => this.off(event, callback);
    }
    off(event, callback) {
      if (this._listeners.has(event)) {
        this._listeners.get(event).delete(callback);
      }
    }
    emit(event, ...args) {
      if (this._listeners.has(event)) {
        for (const cb of this._listeners.get(event)) {
          try { cb(...args); } catch (e) { console.error('Event error:', e); }
        }
      }
    }
    once(event, callback) {
      const off = this.on(event, (...args) => {
        off();
        callback(...args);
      });
      return off;
    }
  }

  class OfflineQueue {
    constructor(storageKey = 'rts_queue') {
      this.storageKey = storageKey;
      this.queue = this._load();
    }
    _load() {
      try {
        const raw = localStorage.getItem(this.storageKey);
        return raw ? JSON.parse(raw) : [];
      } catch (e) { return []; }
    }
    _save() {
      try { localStorage.setItem(this.storageKey, JSON.stringify(this.queue)); } catch (e) {}
    }
    enqueue(item) {
      this.queue.push(item);
      this._save();
    }
    dequeue() {
      const item = this.queue.shift();
      this._save();
      return item;
    }
    removeByWriteId(writeId) {
      const idx = this.queue.findIndex(i => i.writeId === writeId);
      if (idx >= 0) {
        this.queue.splice(idx, 1);
        this._save();
        return true;
      }
      return false;
    }
    getAll() { return [...this.queue]; }
    clear() {
      this.queue = [];
      this._save();
    }
    get size() { return this.queue.length; }
    isEmpty() { return this.queue.length === 0; }
  }

  class LocalCache {
    constructor(storageKey = 'rts_cache') {
      this.storageKey = storageKey;
      this.data = this._load();
      this.versions = new Map();
      this.globalVersion = 0;
    }
    _load() {
      try {
        const raw = localStorage.getItem(this.storageKey);
        return raw ? JSON.parse(raw) : { _v: 0, _d: {} };
      } catch (e) { return { _v: 0, _d: {} }; }
    }
    _save() {
      try { localStorage.setItem(this.storageKey, JSON.stringify(this.data)); } catch (e) {}
    }
    _splitPath(path) {
      if (path === '' || path === '/') return [];
      return path.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
    }
    get(path) {
      const parts = this._splitPath(path);
      let current = this.data._d;
      for (const part of parts) {
        if (current === null || current === undefined || typeof current !== 'object') return null;
        current = current[part];
      }
      return current !== undefined ? current : null;
    }
    set(path, value, version) {
      const parts = this._splitPath(path);
      if (parts.length === 0) {
        this.data._d = value;
      } else {
        let current = this.data._d;
        for (let i = 0; i < parts.length - 1; i++) {
          const part = parts[i];
          if (!current[part] || typeof current[part] !== 'object' || Array.isArray(current[part])) {
            current[part] = {};
          }
          current = current[part];
        }
        if (value === null || value === undefined) {
          delete current[parts[parts.length - 1]];
        } else {
          current[parts[parts.length - 1]] = value;
        }
      }
      if (version !== undefined) {
        this.versions.set(path, version);
        this.globalVersion = Math.max(this.globalVersion, version);
        this.data._v = this.globalVersion;
      }
      this._save();
    }
    merge(path, patch, version) {
      const current = this.get(path);
      if (current === null || typeof current !== 'object' || Array.isArray(current)) {
        this.set(path, patch, version);
        return;
      }
      const merged = this._deepMerge({ ...current }, patch);
      this.set(path, merged, version);
    }
    _deepMerge(target, source) {
      if (!source || typeof source !== 'object') return source;
      for (const key of Object.keys(source)) {
        const srcVal = source[key];
        const tgtVal = target[key];
        if (
          srcVal && typeof srcVal === 'object' && !Array.isArray(srcVal) &&
          tgtVal && typeof tgtVal === 'object' && !Array.isArray(tgtVal)
        ) {
          target[key] = this._deepMerge({ ...tgtVal }, srcVal);
        } else {
          target[key] = srcVal;
        }
      }
      return target;
    }
    getVersion(path) { return this.versions.get(path) || 0; }
    clear() {
      this.data = { _v: 0, _d: {} };
      this.versions.clear();
      this.globalVersion = 0;
      this._save();
    }
  }

  class RealtimeSyncClient extends EventEmitter {
    constructor(options = {}) {
      super();
      this.serverUrl = options.serverUrl || this._defaultUrl();
      this.clientId = options.clientId || null;
      this.clockSkew = 0;
      this.reconnectDelay = 1000;
      this.maxReconnectDelay = 30000;
      this.reconnectAttempts = 0;
      this.shouldReconnect = options.autoReconnect !== false;

      this.ws = null;
      this.isConnected = false;
      this.isOnline = typeof navigator !== 'undefined' ? navigator.onLine : true;
      this.reconnectTimer = null;
      this.heartbeatTimer = null;
      this.requestIdCounter = 0;
      this.pendingRequests = new Map();
      this.subscriptions = new Map();
      this.offlineQueue = new OfflineQueue(options.queueStorageKey);
      this.cache = new LocalCache(options.cacheStorageKey);
      this.conflictResolver = options.conflictResolver || null;
      this.lastKnownServerVersion = this.cache.data._v || 0;
      this.optimisticWrites = new Map();

      this._bindNetworkEvents();
    }

    _updateServerVersion(version) {
      if (!version || version <= 0) return;
      if (version > this.lastKnownServerVersion) {
        this.lastKnownServerVersion = version;
        this.cache.set('/', this.cache.get('/'), version);
      }
    }

    _rollbackOptimisticWrite(path, finalValue) {
      this.cache.set(path, finalValue, undefined);
      if (this.subscriptions.has(path)) {
        const sub = this.subscriptions.get(path);
        const prev = sub.lastValue;
        sub.lastValue = this._deepClone(finalValue);
        for (const cb of sub.callbacks) {
          try {
            cb({
              path,
              value: finalValue,
              previousValue: prev,
              changedPaths: [path],
              origin: 'rollback',
              timestamp: Date.now()
            });
          } catch (e) {}
        }
      }
      this.emit('data_changed', { path, value: finalValue, changedPaths: [path], origin: 'rollback' });
      this.emit('write_rejected', { path, finalValue });
    }

    _defaultUrl() {
      const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${proto}//${location.host}`;
    }

    _bindNetworkEvents() {
      if (typeof window !== 'undefined') {
        window.addEventListener('online', () => {
          this.isOnline = true;
          this.emit('network_online');
          if (this.shouldReconnect && !this.isConnected) {
            this._scheduleReconnect(100);
          }
        });
        window.addEventListener('offline', () => {
          this.isOnline = false;
          this.isConnected = false;
          this.emit('network_offline');
          this.emit('disconnected');
          if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
          }
        });
      }
    }

    connect() {
      if (this.isConnected || (this.ws && this.ws.readyState <= 1)) {
        return Promise.resolve();
      }
      this.shouldReconnect = true;
      return this._connect();
    }

    _connect() {
      return new Promise((resolve, reject) => {
        try {
          this.ws = new WebSocket(this.serverUrl);
        } catch (e) {
          reject(e);
          return;
        }

        this.ws.onopen = () => {
          this.reconnectAttempts = 0;
          this.reconnectDelay = 1000;
          this._startHeartbeat();
        };

        this.ws.onmessage = (evt) => {
          let msg;
          try { msg = JSON.parse(evt.data); } catch (e) { return; }
          this._handleMessage(msg, resolve, reject);
        };

        this.ws.onerror = (err) => {
          if (this.pendingRequests.size > 0) {
            for (const [, req] of this.pendingRequests) {
              req.reject(err);
            }
            this.pendingRequests.clear();
          }
          reject(err);
        };

        this.ws.onclose = () => {
          this._onClose();
        };
      });
    }

    _onClose() {
      const wasConnected = this.isConnected;
      this.isConnected = false;
      this._stopHeartbeat();
      if (this.pendingRequests.size > 0) {
        for (const [, req] of this.pendingRequests) {
          req.reject(new Error('Connection closed'));
        }
        this.pendingRequests.clear();
      }
      if (wasConnected) this.emit('disconnected');
      if (this.shouldReconnect && this.isOnline) {
        this._scheduleReconnect();
      }
    }

    _scheduleReconnect(delay) {
      if (this.reconnectTimer) return;
      const d = delay !== undefined ? delay : this.reconnectDelay;
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.reconnectAttempts++;
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
        this.emit('reconnecting', { attempt: this.reconnectAttempts });
        this.connect().catch(() => {});
      }, d);
    }

    _startHeartbeat() {
      this._stopHeartbeat();
      this.heartbeatTimer = setInterval(() => {
        if (this.isConnected && this.ws && this.ws.readyState === 1) {
          this._send({ type: 'ping', clientTime: this._timestamp() });
        }
      }, 25000);
    }

    _stopHeartbeat() {
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
    }

    disconnect() {
      this.shouldReconnect = false;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
      if (this.ws) {
        try { this.ws.close(); } catch (e) {}
      }
    }

    _timestamp() {
      return Date.now() + this.clockSkew;
    }

    _nextRequestId() {
      return 'req_' + (++this.requestIdCounter) + '_' + Math.random().toString(36).slice(2, 8);
    }

    _generateWriteId() {
      return 'w_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 10);
    }

    _send(msg) {
      if (this.ws && this.ws.readyState === 1) {
        try {
          this.ws.send(JSON.stringify(msg));
          return true;
        } catch (e) { return false; }
      }
      return false;
    }

    _request(msg) {
      return new Promise((resolve, reject) => {
        const reqId = this._nextRequestId();
        msg.requestId = reqId;
        this.pendingRequests.set(reqId, { resolve, reject, type: msg.type });
        const sent = this._send(msg);
        if (!sent) {
          this.pendingRequests.delete(reqId);
          reject(new Error('Not connected'));
        }
      });
    }

    _handleMessage(msg, connectResolve, connectReject) {
      switch (msg.type) {
        case 'connected':
          this.clientId = msg.clientId;
          this.isConnected = true;
          this.clockSkew = msg.serverTime - Date.now();
          this.emit('connected', { clientId: this.clientId });
          if (connectResolve) connectResolve();
          this._performSync();
          break;
        case 'pong':
          const newSkew = msg.serverTime - Date.now();
          this.clockSkew = this.clockSkew * 0.8 + newSkew * 0.2;
          this.emit('pong', { skewMs: this.clockSkew });
          break;
        case 'subscribed':
          this.cache.set(msg.path, msg.value, msg.version);
          this._updateServerVersion(msg.globalVersion);
          if (this.subscriptions.has(msg.path)) {
            this.subscriptions.get(msg.path).version = msg.version;
          }
          break;
        case 'unsubscribed':
          break;
        case 'change':
          this._handleChange(msg);
          break;
        case 'write_ack':
          if (msg.writeId) {
            this.offlineQueue.removeByWriteId(msg.writeId);
            this.optimisticWrites.delete(msg.writeId);
          }
          if (msg.conflict) {
            this.emit('conflict_resolved', {
              path: msg.path,
              writeId: msg.writeId,
              ...msg.conflict
            });
            if (!msg.committed) {
              this._rollbackOptimisticWrite(msg.path, msg.conflict.finalValue);
            }
          }
          this._updateServerVersion(msg.version);
          break;
        case 'sync_response':
          break;
        case 'batch_response':
          break;
        case 'get_response':
          if (msg.value !== undefined && msg.path !== undefined) {
            this.cache.set(msg.path, msg.value, msg.version);
            this._updateServerVersion(msg.globalVersion);
          }
          break;
        case 'error':
          this.emit('error', { message: msg.message });
          break;
      }

      if (msg.requestId && this.pendingRequests.has(msg.requestId)) {
        const req = this.pendingRequests.get(msg.requestId);
        this.pendingRequests.delete(msg.requestId);
        if (msg.type === 'error') {
          req.reject(new Error(msg.message));
        } else {
          req.resolve(msg);
        }
      }
    }

    _handleChange(msg) {
      const { subscriptionPath, value, version, globalVersion, changedPaths, changeOrigin } = msg;

      this.cache.set(subscriptionPath, value, version);
      this._updateServerVersion(globalVersion);

      if (this.subscriptions.has(subscriptionPath)) {
        const sub = this.subscriptions.get(subscriptionPath);
        sub.version = version;
        for (const cb of sub.callbacks) {
          try {
            cb({
              path: subscriptionPath,
              value,
              previousValue: sub.lastValue,
              changedPaths: changedPaths || [subscriptionPath],
              origin: changeOrigin,
              changeId: msg.changeId,
              timestamp: msg.hybridTimestamp
            });
          } catch (e) { console.error('Subscription callback error:', e); }
        }
        sub.lastValue = this._deepClone(value);
      }

      this.emit('data_changed', {
        path: subscriptionPath,
        value,
        changedPaths,
        origin: changeOrigin,
        timestamp: msg.hybridTimestamp
      });
    }

    _deepClone(obj) {
      if (obj === null || typeof obj !== 'object') return obj;
      if (Array.isArray(obj)) return obj.map(this._deepClone.bind(this));
      const copy = {};
      for (const k of Object.keys(obj)) copy[k] = this._deepClone(obj[k]);
      return copy;
    }

    subscribe(path, callback) {
      if (!this.subscriptions.has(path)) {
        this.subscriptions.set(path, {
          callbacks: new Set(),
          version: 0,
          lastValue: null
        });
      }
      const sub = this.subscriptions.get(path);
      if (callback) sub.callbacks.add(callback);

      const cached = this.cache.get(path);
      if (cached !== null) {
        try {
          callback && callback({
            path,
            value: cached,
            previousValue: null,
            changedPaths: [path],
            origin: 'cache',
            timestamp: null
          });
        } catch (e) {}
        sub.lastValue = this._deepClone(cached);
      }

      if (this.isConnected) {
        this._request({ type: 'subscribe', path }).then(resp => {
          if (resp.value !== null && cached === null) {
            try {
              callback && callback({
                path,
                value: resp.value,
                previousValue: cached,
                changedPaths: [path],
                origin: 'server',
                timestamp: null
              });
            } catch (e) {}
            sub.lastValue = this._deepClone(resp.value);
          }
        }).catch(err => {
          this.emit('error', { message: 'Subscribe failed: ' + err.message });
        });
      }

      return () => this.unsubscribe(path, callback);
    }

    unsubscribe(path, callback) {
      if (!this.subscriptions.has(path)) return;
      const sub = this.subscriptions.get(path);
      if (callback) {
        sub.callbacks.delete(callback);
      }
      if (sub.callbacks.size === 0 || !callback) {
        if (this.isConnected) {
          this._send({ type: 'unsubscribe', path });
        }
        this.subscriptions.delete(path);
      }
    }

    async get(path) {
      const cached = this.cache.get(path);
      if (this.isConnected) {
        try {
          const resp = await this._request({ type: 'get', path });
          return resp.value;
        } catch (e) {
          if (cached !== null) return cached;
          throw e;
        }
      }
      if (cached !== null) return cached;
      throw new Error('Offline and no cached data');
    }

    async set(path, value, options = {}) {
      const writeId = this._generateWriteId();
      const timestamp = this._timestamp();
      const op = {
        writeId,
        op: 'set',
        path,
        value,
        clientTimestamp: timestamp,
        conflictStrategy: options.conflictStrategy || 'lww'
      };

      if (options.optimistic !== false) {
        const oldValue = this.cache.get(path);
        this.optimisticWrites.set(writeId, { path, oldValue, newValue: value, timestamp });
        this.cache.set(path, value, undefined);
        if (this.subscriptions.has(path)) {
          const sub = this.subscriptions.get(path);
          const prev = sub.lastValue;
          for (const cb of sub.callbacks) {
            try {
              cb({
                path,
                value,
                previousValue: prev,
                changedPaths: [path],
                origin: 'local_optimistic',
                timestamp
              });
            } catch (e) {}
          }
          sub.lastValue = this._deepClone(value);
        }
        this.emit('data_changed', { path, value, changedPaths: [path], origin: 'local', timestamp });
      }

      if (this.isConnected) {
        try {
          const msg = {
            type: 'set',
            path,
            value,
            clientTimestamp: timestamp,
            writeId,
            conflictStrategy: options.conflictStrategy || 'lww'
          };
          await this._request(msg);
          return { writeId, timestamp, committed: true };
        } catch (e) {
          this.offlineQueue.enqueue(op);
          this.emit('write_queued', op);
          return { writeId, timestamp, committed: false, queued: true };
        }
      } else {
        this.offlineQueue.enqueue(op);
        this.emit('write_queued', op);
        return { writeId, timestamp, committed: false, queued: true };
      }
    }

    async merge(path, patch, options = {}) {
      const writeId = this._generateWriteId();
      const timestamp = this._timestamp();
      const op = {
        writeId,
        op: 'merge',
        path,
        value: patch,
        clientTimestamp: timestamp,
        conflictStrategy: 'merge'
      };

      if (options.optimistic !== false) {
        const oldValue = this.cache.get(path);
        this.cache.merge(path, patch, undefined);
        const newValue = this.cache.get(path);
        this.optimisticWrites.set(writeId, { path, oldValue, newValue, timestamp });
        if (this.subscriptions.has(path)) {
          const sub = this.subscriptions.get(path);
          const prev = sub.lastValue;
          for (const cb of sub.callbacks) {
            try {
              cb({
                path,
                value: newValue,
                previousValue: prev,
                changedPaths: [path],
                origin: 'local_optimistic',
                timestamp
              });
            } catch (e) {}
          }
          sub.lastValue = this._deepClone(newValue);
        }
        this.emit('data_changed', { path, value: newValue, changedPaths: [path], origin: 'local', timestamp });
      }

      if (this.isConnected) {
        try {
          const msg = {
            type: 'merge',
            path,
            value: patch,
            clientTimestamp: timestamp,
            writeId
          };
          await this._request(msg);
          return { writeId, timestamp, committed: true };
        } catch (e) {
          this.offlineQueue.enqueue(op);
          this.emit('write_queued', op);
          return { writeId, timestamp, committed: false, queued: true };
        }
      } else {
        this.offlineQueue.enqueue(op);
        this.emit('write_queued', op);
        return { writeId, timestamp, committed: false, queued: true };
      }
    }

    async remove(path, options = {}) {
      return this.set(path, null, options);
    }

    async batch(operations, options = {}) {
      if (!this.isConnected) {
        const timestamp = this._timestamp();
        for (const op of operations) {
          const writeId = this._generateWriteId();
          if (op.optimistic !== false) {
            if (op.type === 'set') this.cache.set(op.path, op.value, undefined);
            else if (op.type === 'merge') this.cache.merge(op.path, op.value || {}, undefined);
            else if (op.type === 'remove') this.cache.set(op.path, null, undefined);
          }
          this.offlineQueue.enqueue({
            writeId,
            op: op.type,
            path: op.path,
            value: op.value,
            clientTimestamp: timestamp,
            conflictStrategy: op.conflictStrategy || 'lww'
          });
        }
        return { committed: false, queued: true };
      }
      const timestamp = this._timestamp();
      const ops = operations.map(o => ({
        ...o,
        clientTimestamp: timestamp
      }));
      if (options.optimistic !== false) {
        for (const op of operations) {
          if (op.type === 'set') this.cache.set(op.path, op.value, undefined);
          else if (op.type === 'merge') this.cache.merge(op.path, op.value || {}, undefined);
          else if (op.type === 'remove') this.cache.set(op.path, null, undefined);
        }
      }
      return this._request({ type: 'batch', operations: ops });
    }

    async _performSync() {
      if (this.offlineQueue.isEmpty() && this.subscriptions.size === 0 && this.lastKnownServerVersion === 0) {
        for (const [path, sub] of this.subscriptions) {
          try {
            const resp = await this._request({ type: 'subscribe', path });
            sub.lastValue = this._deepClone(resp.value);
          } catch (e) {}
        }
        this.emit('synced', { status: 'full' });
        return;
      }

      try {
        const pending = this.offlineQueue.getAll();
        const resp = await this._request({
          type: 'sync',
          lastKnownGlobalVersion: this.lastKnownServerVersion,
          pendingWrites: pending
        });

        this._updateServerVersion(resp.serverGlobalVersion);

        for (const wid of resp.appliedWriteIds || []) {
          this.offlineQueue.removeByWriteId(wid);
          this.optimisticWrites.delete(wid);
        }

        for (const conflict of resp.conflicts || []) {
          this.emit('conflict_resolved', conflict);
          if (conflict.rejected && conflict.finalValue !== undefined) {
            this._rollbackOptimisticWrite(conflict.path, conflict.finalValue);
          }
          if (this.conflictResolver) {
            try {
              const result = await this.conflictResolver(conflict);
              if (result && result.value !== undefined) {
                await this.set(conflict.path, result.value, { optimistic: false });
              }
            } catch (e) {
              console.error('Custom conflict resolver error:', e);
            }
          }
        }

        for (const change of resp.changesSinceLast || []) {
          this.cache.set(change.path, change.value, change.version);
          this._updateServerVersion(change.version);
          if (this.subscriptions.has(change.path)) {
            const sub = this.subscriptions.get(change.path);
            const prev = sub.lastValue;
            sub.lastValue = this._deepClone(change.value);
            for (const cb of sub.callbacks) {
              try {
                cb({
                  path: change.path,
                  value: change.value,
                  previousValue: prev,
                  changedPaths: [change.path],
                  origin: 'sync',
                  timestamp: change.hybridTimestamp
                });
              } catch (e) {}
            }
          }
        }

        if (resp.subscriptionSnapshots) {
          for (const [path, snap] of Object.entries(resp.subscriptionSnapshots)) {
            this.cache.set(path, snap.value, snap.version);
            if (this.subscriptions.has(path)) {
              const sub = this.subscriptions.get(path);
              sub.version = snap.version;
              sub.lastValue = this._deepClone(snap.value);
            }
          }
        }

        for (const [path, sub] of this.subscriptions) {
          if (!resp.subscriptionSnapshots || !resp.subscriptionSnapshots[path]) {
            try {
              await this._request({ type: 'subscribe', path });
            } catch (e) {}
          }
        }

        this.emit('synced', {
          status: 'ok',
          serverVersion: resp.serverGlobalVersion,
          conflicts: resp.conflicts || [],
          changesApplied: (resp.changesSinceLast || []).length
        });
      } catch (e) {
        this.emit('sync_error', { error: e });
        for (const [path] of this.subscriptions) {
          this._request({ type: 'subscribe', path }).catch(() => {});
        }
      }
    }

    setConflictResolver(resolver) {
      this.conflictResolver = resolver;
    }

    getPendingWrites() {
      return this.offlineQueue.getAll();
    }

    clearCache() {
      this.cache.clear();
    }

    get stats() {
      return {
        connected: this.isConnected,
        online: this.isOnline,
        clientId: this.clientId,
        subscriptions: this.subscriptions.size,
        pendingWrites: this.offlineQueue.size,
        lastKnownServerVersion: this.lastKnownServerVersion,
        clockSkewMs: Math.round(this.clockSkew)
      };
    }
  }

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = RealtimeSyncClient;
  } else {
    global.RealtimeSyncClient = RealtimeSyncClient;
  }
})(typeof window !== 'undefined' ? window : this);
