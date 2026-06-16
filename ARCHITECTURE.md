# Realtime Sync Engine 技术架构文档

## 1. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client (Browser)                         │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │ 订阅管理     │  │ 本地缓存      │  │ 离线写入队列            │  │
│  └──────┬──────┘  └──────┬───────┘  └───────────┬────────────┘  │
│         │                │                       │               │
│         └────────────────┼───────────────────────┘               │
│                          ▼                                       │
│              ┌────────────────────────┐                          │
│              │ RealtimeSyncClient SDK │                          │
│              │  • WebSocket连接管理    │                          │
│              │  • 乐观更新/回滚        │                          │
│              │  • 时钟偏差校准          │                          │
│              │  • 重连同步协调          │                          │
│              └───────────┬────────────┘                          │
└──────────────────────────┼───────────────────────────────────────┘
                           │ WebSocket (JSON 协议)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Server (Node.js)                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ SubscriptionTree│  │    DataStore     │  │ ConnectionMgr  │  │
│  │ (Trie前缀树)    │  │ (带版本的KV存储)  │  │ (连接/心跳/ACK) │  │
│  └────────┬────────┘  └────────┬─────────┘  └───────┬────────┘  │
│           │                    │                    │            │
│           └────────────────────┼────────────────────┘            │
│                                ▼                                 │
│                    ┌──────────────────────┐                      │
│                    │   广播调度器           │                      │
│                    │   精确路径匹配         │                      │
│                    │   父/子路径联动        │                      │
│                    │   通配符匹配           │                      │
│                    └──────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 核心问题一：服务端如何高效把一次写入只推给真正订阅了相关路径的客户端

### 2.1 方案：Trie 前缀树订阅索引

我们没有采用"遍历所有客户端的订阅列表逐一检查"的 O(N) 方案（N 为客户端数），而是构建了一棵 **订阅树 (SubscriptionTree)**，将路径片段作为 Trie 节点，所有订阅了该节点路径的客户端挂在对应节点的 `subscribers` Set 上。

**关键实现文件：** [SubscriptionTree.js](file:///d:/trae-bz/TraeProjects/1/server/SubscriptionTree.js)

### 2.2 Trie 节点结构

```javascript
class TrieNode {
  children: Map<string, TrieNode>    // 子节点，key = 路径片段
  subscribers: Set<WebSocket>         // 订阅此节点对应路径的客户端
}
```

例如，3 个客户端分别订阅：
- Client A: `/chat/general`
- Client B: `/chat/*`
- Client C: `/users/123`

Trie 结构如下：
```
root
├─ chat
│   ├─ general  → subscribers={A}
│   └─ *        → subscribers={B}
└─ users
    └─ 123      → subscribers={C}
```

### 2.3 写入时的匹配流程

当路径 `/chat/general` 发生写入，`getMatchingSubscribers()` 会同时从 **三个维度** 收集订阅者：

| 维度 | 算法 | 目的 | 举例 |
|------|------|------|------|
| 精确匹配 + 祖先 | 从 root 到目标路径逐层收集 | 订阅 `/` 或 `/chat` 都应收到子路径变更 | 订阅 `/chat` 者收到 |
| 通配符匹配 | 遍历时遇到 `*` 节点则继续向下 | 支持模式订阅 | 订阅 `/chat/*` 者收到 |
| 后代收集 | 到达目标路径后 DFS 所有子节点 | 父路径变更应通知子路径订阅者 | 写入 `/` 通知所有订阅者 |

时间复杂度从 O(N×M) 降到 O(K + S)，其中 K 是路径深度，S 是实际受影响的订阅者数量。

### 2.4 额外优化：按订阅路径聚合通知

在 [index.js 的 broadcastChange()](file:///d:/trae-bz/TraeProjects/1/server/index.js#L276-L325) 中，不是对每个"写入路径×客户端"对单独发消息，而是对每个 `(客户端, 订阅路径)` 聚合一次通知，把该客户端所有变更路径打包在 `changedPaths` 字段，减少消息数量。

---

## 3. 核心问题二：离线期间客户端的本地写入如何排队并在重连后与服务端已变化数据合并

### 3.1 三层可靠保证

**实现文件：** [realtime-sync.js](file:///d:/trae-bz/TraeProjects/1/client/realtime-sync.js)

#### 第一层：写入队列持久化 (OfflineQueue)
```javascript
class OfflineQueue {
  // 使用 localStorage 持久化，浏览器关闭也不丢
  enqueue(item)    // 写入 localStorage
  removeByWriteId(id) // 收到 ACK 后精确删除
  getAll()         // 重连时按顺序全部拿出
}
```

每条写入操作生成全局唯一 `writeId`，服务端 ACK 后才从队列移除。

#### 第二层：乐观更新 + 本地缓存 (LocalCache)
用户写入时立即更新本地缓存并触发 UI 回调（`origin: 'local_optimistic'`），无需等待网络。重连后同步结果会覆盖缓存。

#### 第三层：版本号驱动的全量同步

服务端维护 `globalVersion` 单调递增版本号，每次写入 +1。客户端保存 `lastKnownServerVersion`。

**重连握手流程：**
```
  客户端                                          服务端
     │                                               │
     │ 1. 重连 WebSocket → CONNECTED(含serverTime)   │
     │                                               │
     │ 2. SYNC {                                     │
     │      lastKnownGlobalVersion: 42,              │
     │      pendingWrites: [                         │
     │        {writeId:"w1", path:"/a", value:1, ts} │
     │        {writeId:"w2", path:"/b", value:2, ts} │
     │      ]                                        │
     │    } ──────────────────────────────────────→  │
     │                                               │  ① 重放客户端离线写入
     │                                               │  ② 收集 version > 42 的所有变更
     │                                               │
     │ 3. SYNC_RESPONSE {                            │
     │      serverGlobalVersion: 47,                 │
     │      appliedWriteIds: ["w1","w2"],  ← 可删除  │
     │      conflicts: [                     ← 冲突  │
     │        {path:"/a", resolved:true,             │
     │         strategy:"lww", finalValue:..}        │
     │      ],                                       │
     │      changesSinceLast: [              ← 漏改  │
     │        {id, path, value, version: 43}         │
     │        {id, path, value, version: 45}         │
     │      ],                                       │
     │      subscriptionSnapshots: {         ← 快照  │
     │        "/chat": {value, version}              │
     │      }                                        │
     │    } ←──────────────────────────────────────  │
     │                                               │
     │ 4. 本地合并：                                  │
     │    • 删除已 ACK 的写入                          │
     │    • 应用 changesSinceLast 到缓存              │
     │    • 触发冲突回调                              │
     │    • 触发订阅回调                              │
```

### 3.2 离线写入与服务器已有变更的合并策略

如果客户端离线期间服务端同一被别人改动：

1. **默认策略 (LWW)**：服务端直接应用离线写入（使用混合时间戳决定胜负），在 `conflicts` 字段告知客户端发生过冲突，客户端用最终值覆盖本地乐观值。

2. **自定义合并策略**：客户端可注册 `conflictResolver` 异步回调，返回自定义合并后的值，系统自动再发一次写入覆盖。

---

## 4. 核心问题三："最后写入胜出"用客户端时间戳判定的时钟不同步问题及缓解

### 4.1 问题本质

分布式系统中，各本地时钟**天然不同步**（NTP 偏差通常 10~100ms，极端情况数秒至数分钟）。直接用 `clientTimestamp` 判定 LWW 会导致：

| 场景 | 问题 |
|------|------|
| **客户端 A 时钟快 2 分钟** | A 的写入永远"最后"，即使 B 是 1 分钟后真实写入 |
| **客户端 B 时钟慢 3 分钟** | B 的写入永远被认为是"更早"，新值被旧值覆盖 |
| **用户跨时区手动改时间** | 穿越式的数据回滚 |
| **单客户端休眠后唤醒** | 系统休眠期间不计时，醒来后时间戳"倒退" |

**根本矛盾**：物理时间在分布式环境不可信任，但 LWW 必须依赖一个全序关系。

### 4.2 我们的缓解方案：混合时间戳 (Hybrid Timestamp)

**实现文件：** [DataStore.js 的 _generateHybridTimestamp()](file:///d:/trae-bz/TraeProjects/1/server/DataStore.js#L156-L166)

```javascript
_generateHybridTimestamp(clientTs, serverTs) {
  const CLOCK_SKEW_WINDOW = 300000; // 5 分钟
  if (!clientTs) return serverTs;
  const skew = serverTs - clientTs;
  if (Math.abs(skew) <= CLOCK_SKEW_WINDOW) {
    return clientTs;   // 偏差 <5 分钟，信任客户端（保留用户意图）
  }
  return serverTs;    // 偏差过大，强制使用服务器时间
}
```

**决策表：**
```
  |clientTs - serverTs| ≤ 5min   →  使用 clientTs  (保留因果)
  |clientTs - serverTs| > 5min   →  使用 serverTs  (防止极端作弊)
```

### 4.3 第二层防护：客户端时钟偏差持续校准

客户端 SDK 内维护 `clockSkew`，**每次 pong 消息指数平滑更新**：
```javascript
// 每次心跳 pong 收到 serverTime
newSkew = msg.serverTime - Date.now();
clockSkew = clockSkew * 0.8 + newSkew * 0.2;  // EMA 指数平滑
```

所有本地生成的时间戳都加上偏差修正：
```javascript
_timestamp() { return Date.now() + this.clockSkew; }
```

效果：连接稳定后偏差 ≈ RTT/2，通常 <50ms。

### 4.4 第三层防护：单调版本号兜底

如果极端情况下两个写入时间戳**完全相同**（毫秒级碰撞），比较 `globalVersion`（严格单调的服务器序列号）保证总有确定赢家。`globalVersion` 也作为离线同步的水位线，不依赖时间。

### 4.5 为什么不直接用服务器时间？

| 方案 | 优点 | 缺点 |
|------|------|------|
| 纯客户端时间 | 保留用户真实意图（先点按钮的先赢） | 时钟漂移严重时违背直觉 |
| 纯服务器时间 | 绝对一致 | 同时点击但网络差异大的情况下，**实际先操作的人反而输**（违背公平） |
| 混合时间戳（我们的方案） | 公平 + 鲁棒 | 5 分钟窗口的边界条件需要理解 |

---

## 5. 协议规范 (WebSocket JSON)

### 客户端 → 服务端

| type | 字段 | 说明 |
|------|------|------|
| `ping` | `clientTime` | 心跳兼时钟校准 |
| `subscribe` | `path` | 订阅路径，支持 `*` |
| `unsubscribe` | `path` | 取消订阅 |
| `get` | `path` | 一次性读取 |
| `set` | `path, value, clientTimestamp, writeId, conflictStrategy` | 写入 |
| `merge` | `path, value, clientTimestamp, writeId` | 深度合并写入 |
| `batch` | `operations[]` | 批量 set/merge/remove |
| `sync` | `lastKnownGlobalVersion, pendingWrites[]` | 重连同步 |
| `ack` | `requestId` | 确认收到推送 |

### 服务端 → 客户端

| type | 字段 | 说明 |
|------|------|------|
| `connected` | `clientId, serverTime` | 连接成功 |
| `subscribed` | `path, value, version, globalVersion` | 订阅成功（含初始值） |
| `change` | `subscriptionPath, changedPaths, value, version, changeOrigin, hybridTimestamp, originClientId` | 数据变更推送 |
| `write_ack` | `writeId, path, version, hybridTimestamp` | 写入确认 |
| `sync_response` | `serverGlobalVersion, appliedWriteIds, conflicts[], changesSinceLast[], subscriptionSnapshots{}` | 同步结果 |
| `get_response` | `path, value, version` | 读取结果 |
| `batch_response` | `results[]` | 批量结果 |
| `error` | `message, requestId` | 错误 |

---

## 6. 快速开始

```bash
# 安装依赖
npm install

# 启动服务
npm start

# 浏览器访问两个窗口
#   http://localhost:3000
# 在其中一个窗口订阅 /chat，另一个写入 /chat，观察实时推送
# 用"模拟断网/恢复"测试离线队列功能
```

### 代码文件索引

| 文件 | 职责 |
|------|------|
| [server/SubscriptionTree.js](file:///d:/trae-bz/TraeProjects/1/server/SubscriptionTree.js) | Trie 订阅索引、三类路径匹配算法 |
| [server/DataStore.js](file:///d:/trae-bz/TraeProjects/1/server/DataStore.js) | 带版本 KV 存储、混合时间戳、变更日志 |
| [server/index.js](file:///d:/trae-bz/TraeProjects/1/server/index.js) | WebSocket 服务、广播调度、同步握手 |
| [client/realtime-sync.js](file:///d:/trae-bz/TraeProjects/1/client/realtime-sync.js) | 客户端 SDK（连接/订阅/队列/缓存/同步/冲突） |
| [client/index.html](file:///d:/trae-bz/TraeProjects/1/client/index.html) | 交互式 Demo 页面 |
