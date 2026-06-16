class TrieNode {
  constructor() {
    this.children = new Map();
    this.subscribers = new Set();
  }
}

class SubscriptionTree {
  constructor() {
    this.root = new TrieNode();
  }

  _splitPath(path) {
    if (path === '' || path === '/') return [];
    return path.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
  }

  subscribe(path, ws) {
    const parts = this._splitPath(path);
    let node = this.root;
    for (const part of parts) {
      if (!node.children.has(part)) {
        node.children.set(part, new TrieNode());
      }
      node = node.children.get(part);
    }
    node.subscribers.add(ws);
  }

  unsubscribe(path, ws) {
    const parts = this._splitPath(path);
    let node = this.root;
    const stack = [{ node: this.root, part: null }];
    for (const part of parts) {
      if (!node.children.has(part)) return;
      node = node.children.get(part);
      stack.push({ node, part });
    }
    node.subscribers.delete(ws);

    for (let i = stack.length - 1; i > 0; i--) {
      const { node: current, part } = stack[i];
      const parent = stack[i - 1].node;
      if (current.subscribers.size === 0 && current.children.size === 0) {
        parent.children.delete(part);
      } else {
        break;
      }
    }
  }

  unsubscribeAll(ws) {
    this._removeFromNode(this.root, ws);
  }

  _removeFromNode(node, ws) {
    node.subscribers.delete(ws);
    for (const [key, child] of node.children) {
      this._removeFromNode(child, ws);
      if (child.subscribers.size === 0 && child.children.size === 0) {
        node.children.delete(key);
      }
    }
  }

  getMatchingSubscribers(changedPath) {
    const parts = this._splitPath(changedPath);
    const result = new Set();

    this._collectExactMatches(parts, result);
    this._collectParentWildcards(parts, result);
    this._collectDescendantSubscribers(parts, result);

    return result;
  }

  _collectExactMatches(parts, result) {
    let node = this.root;
    if (node.subscribers.size > 0) {
      for (const sub of node.subscribers) result.add(sub);
    }
    for (const part of parts) {
      if (!node.children.has(part)) return;
      node = node.children.get(part);
      if (node.subscribers.size > 0) {
        for (const sub of node.subscribers) result.add(sub);
      }
    }
  }

  _collectParentWildcards(parts, result) {
    let node = this.root;
    this._wildcardTraverse(node, parts, 0, result);
  }

  _wildcardTraverse(node, parts, depth, result) {
    if (depth === parts.length) return;

    if (node.children.has('*')) {
      const wildcardNode = node.children.get('*');
      this._collectAllSubscribersBelow(wildcardNode, parts, depth + 1, result);
    }

    if (node.children.has(parts[depth])) {
      this._wildcardTraverse(node.children.get(parts[depth]), parts, depth + 1, result);
    }
  }

  _collectAllSubscribersBelow(node, parts, depth, result) {
    if (node.subscribers.size > 0) {
      for (const sub of node.subscribers) result.add(sub);
    }
    if (depth >= parts.length) return;
    for (const child of node.children.values()) {
      this._collectAllSubscribersBelow(child, parts, depth + 1, result);
    }
    if (node.children.has('*')) {
      this._collectAllSubscribersBelow(node.children.get('*'), parts, depth + 1, result);
    }
  }

  _collectDescendantSubscribers(parts, result) {
    let node = this.root;
    for (const part of parts) {
      if (!node.children.has(part)) return;
      node = node.children.get(part);
    }
    this._dfs(node, result);
  }

  _dfs(node, result) {
    for (const child of node.children.values()) {
      if (child.subscribers.size > 0) {
        for (const sub of child.subscribers) result.add(sub);
      }
      this._dfs(child, result);
    }
  }

  hasSubscribers(path) {
    return this.getMatchingSubscribers(path).size > 0;
  }
}

module.exports = SubscriptionTree;
