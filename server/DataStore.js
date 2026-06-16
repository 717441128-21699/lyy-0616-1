const crypto = require('crypto');

class DataStore {
  constructor() {
    this.data = {};
    this.pathVersions = new Map();
    this.pathClientTimestamps = new Map();
    this.pathHybridTimestamps = new Map();
    this.globalVersion = 0;
    this.changeLog = [];
    this.MAX_LOG_SIZE = 10000;
  }

  _splitPath(path) {
    if (path === '' || path === '/') return [];
    return path.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
  }

  _getParentPath(path) {
    const parts = this._splitPath(path);
    if (parts.length === 0) return null;
    parts.pop();
    return '/' + parts.join('/');
  }

  _getAllAncestorPaths(path) {
    const parts = this._splitPath(path);
    const ancestors = ['/'];
    let current = '';
    for (let i = 0; i < parts.length - 1; i++) {
      current += '/' + parts[i];
      ancestors.push(current);
    }
    return ancestors;
  }

  _getAllDescendantPaths(obj, prefix) {
    const paths = [prefix];
    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
      for (const key of Object.keys(obj)) {
        const childPath = prefix === '/' ? '/' + key : prefix + '/' + key;
        paths.push(...this._getAllDescendantPaths(obj[key], childPath));
      }
    }
    return paths;
  }

  get(path) {
    const parts = this._splitPath(path);
    let current = this.data;
    for (const part of parts) {
      if (current === null || current === undefined || typeof current !== 'object') {
        return null;
      }
      current = current[part];
    }
    return current !== undefined ? current : null;
  }

  set(path, value, clientTimestamp, clientId, options = {}) {
    const serverTimestamp = Date.now();
    const normalizedPath = path || '/';
    const existingClientTs = this.pathClientTimestamps.get(normalizedPath) || 0;

    const conflictResult = this._checkConflict(existingClientTs, clientTimestamp, options.force);

    if (conflictResult.rejected) {
      return {
        id: crypto.randomBytes(8).toString('hex'),
        path: normalizedPath,
        value: this.get(normalizedPath),
        oldValue: this.get(normalizedPath),
        version: this.globalVersion,
        clientTimestamp,
        serverTimestamp,
        hybridTimestamp: this.pathHybridTimestamps.get(normalizedPath) || existingClientTs,
        clientId,
        affectedPaths: [normalizedPath],
        rejected: true,
        conflict: {
          existingTimestamp: existingClientTs,
          incomingTimestamp: clientTimestamp,
          winner: 'existing',
          strategy: 'last_write_wins',
          note: 'based on client timestamp'
        }
      };
    }

    const writeId = crypto.randomBytes(8).toString('hex');
    const version = ++this.globalVersion;
    const hybridTimestamp = this._generateHybridTimestamp(clientTimestamp, serverTimestamp);

    const parts = this._splitPath(normalizedPath);
    const oldValue = this.get(normalizedPath);

    if (parts.length === 0) {
      this.data = value;
    } else {
      let current = this.data;
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

    const affectedPaths = new Set();
    affectedPaths.add(normalizedPath);
    for (const ancestor of this._getAllAncestorPaths(normalizedPath)) {
      affectedPaths.add(ancestor);
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      for (const desc of this._getAllDescendantPaths(value, normalizedPath)) {
        affectedPaths.add(desc);
      }
    }
    if (oldValue && typeof oldValue === 'object' && !Array.isArray(oldValue)) {
      for (const desc of this._getAllDescendantPaths(oldValue, normalizedPath)) {
        affectedPaths.add(desc);
      }
    }

    for (const p of affectedPaths) {
      this.pathVersions.set(p, version);
    }
    this.pathClientTimestamps.set(normalizedPath, clientTimestamp);
    this.pathHybridTimestamps.set(normalizedPath, hybridTimestamp);

    const logEntry = {
      id: writeId,
      path: normalizedPath,
      value,
      oldValue,
      version,
      hybridTimestamp,
      clientTimestamp,
      serverTimestamp,
      clientId,
      affectedPaths: Array.from(affectedPaths),
      rejected: false,
      conflict: conflictResult.happened ? {
        existingTimestamp: existingClientTs,
        incomingTimestamp: clientTimestamp,
        winner: 'incoming',
        strategy: 'last_write_wins',
        note: 'based on client timestamp'
      } : null
    };
    this.changeLog.push(logEntry);
    if (this.changeLog.length > this.MAX_LOG_SIZE) {
      this.changeLog.splice(0, this.changeLog.length - this.MAX_LOG_SIZE);
    }

    return logEntry;
  }

  _checkConflict(existingTs, incomingTs, force) {
    if (force || existingTs === 0) {
      return { rejected: false, happened: false };
    }
    if (incomingTs > existingTs) {
      return { rejected: false, happened: true };
    }
    return { rejected: true, happened: true };
  }

  merge(path, patch, clientTimestamp, clientId) {
    const currentValue = this.get(path);
    if (currentValue === null || typeof currentValue !== 'object' || Array.isArray(currentValue)) {
      return this.set(path, patch, clientTimestamp, clientId);
    }
    const merged = this._deepMerge({ ...currentValue }, patch);
    return this.set(path, merged, clientTimestamp, clientId);
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

  _generateHybridTimestamp(clientTs, serverTs) {
    const CLOCK_SKEW_WINDOW = 300000;
    if (!clientTs) return serverTs;
    const skew = serverTs - clientTs;
    if (Math.abs(skew) <= CLOCK_SKEW_WINDOW) {
      return clientTs;
    }
    return serverTs;
  }

  resolveConflict(existingTimestamp, incomingTimestamp) {
    return incomingTimestamp >= existingTimestamp;
  }

  getPathVersion(path) {
    return this.pathVersions.get(path) || 0;
  }

  getChangesSinceVersion(version) {
    return this.changeLog.filter(log => log.version > version);
  }

  getSnapshot() {
    return JSON.parse(JSON.stringify(this.data));
  }

  getSnapshotWithVersion(path) {
    return {
      value: this.get(path),
      version: this.getPathVersion(path),
      globalVersion: this.globalVersion
    };
  }

  generateWriteId() {
    return crypto.randomBytes(8).toString('hex');
  }
}

module.exports = DataStore;
