const WebSocket = require('ws');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
function assert(cond, msg) { if (!cond) throw new Error(msg || 'Assertion failed'); }

let passed = 0, failed = 0;

async function test(name, fn) {
  try {
    await fn();
    console.log(`✅ ${name}`);
    passed++;
  } catch (e) {
    console.log(`❌ ${name}: ${e.message}`);
    failed++;
  }
}

class TestClient {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this._reqId = 0;
    this._pending = new Map();
    this._listeners = new Map();
  }
  connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);
      this.ws.on('open', () => {});
      this.ws.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.requestId && this._pending.has(msg.requestId)) {
          const { resolve: res } = this._pending.get(msg.requestId);
          this._pending.delete(msg.requestId);
          res(msg);
        }
        if (this._listeners.has(msg.type)) {
          for (const cb of this._listeners.get(msg.type)) cb(msg);
        }
      });
      this.ws.on('error', reject);
      this.on('connected', () => resolve());
      setTimeout(() => reject(new Error('Connect timeout')), 3000);
    });
  }
  on(type, cb) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(cb);
  }
  request(msg) {
    return new Promise((resolve, reject) => {
      const id = 'req_' + (++this._reqId) + '_' + Math.random().toString(36).slice(2, 6);
      msg.requestId = id;
      this._pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify(msg));
      setTimeout(() => {
        if (this._pending.has(id)) {
          this._pending.delete(id);
          reject(new Error('Request timeout: ' + msg.type));
        }
      }, 5000);
    });
  }
  close() {
    try { this.ws.close(); } catch (e) {}
  }
}

async function run() {
  const url = 'ws://localhost:3000';

  console.log('═══════════════════════════════════════════');
  console.log('  核心功能验证测试');
  console.log('═══════════════════════════════════════════\n');

  // Test 1: LWW 严格按 clientTimestamp
  await test('LWW严格按客户端时间戳判定', async () => {
    const c = new TestClient(url);
    await c.connect();
    const path = '/test/lww_' + Date.now();
    const base = Date.now();

    const r1 = await c.request({ type: 'set', path, value: 'LATER', clientTimestamp: base + 10000, writeId: 'w1' });
    assert(r1.committed === true, 'First write should commit');

    const r2 = await c.request({ type: 'set', path, value: 'EARLIER', clientTimestamp: base, writeId: 'w2' });
    assert(r2.committed === false, 'Earlier write should be rejected');
    assert(r2.conflict !== null, 'Should have conflict info');
    assert(r2.conflict.winner === 'existing', 'Winner should be existing');
    assert(r2.conflict.incomingTimestamp === base, 'Incoming ts should match');
    assert(r2.conflict.existingTimestamp === base + 10000, 'Existing ts should match');

    const r3 = await c.request({ type: 'get', path });
    assert(r3.value === 'LATER', `Final value should be LATER, got ${r3.value}`);

    c.close();
  });

  // Test 2: 时间更晚的写入成功覆盖
  await test('时间更晚的写入成功覆盖', async () => {
    const c = new TestClient(url);
    await c.connect();
    const path = '/test/lww2_' + Date.now();
    const base = Date.now();

    await c.request({ type: 'set', path, value: 'FIRST', clientTimestamp: base, writeId: 'w1' });
    const r = await c.request({ type: 'set', path, value: 'SECOND', clientTimestamp: base + 5000, writeId: 'w2' });
    assert(r.committed === true, 'Later write should commit');
    assert(r.conflict !== null, 'Should have conflict info');
    assert(r.conflict.winner === 'incoming', 'Winner should be incoming');

    const r3 = await c.request({ type: 'get', path });
    assert(r3.value === 'SECOND', `Final value should be SECOND, got ${r3.value}`);

    c.close();
  });

  // Test 3: 订阅子路径后父路径变更能收到
  await test('订阅子路径后父路径覆盖能收到变更', async () => {
    const c1 = new TestClient(url);
    const c2 = new TestClient(url);
    await c1.connect();
    await c2.connect();
    const path = '/test/parent_' + Date.now();

    await c1.request({ type: 'subscribe', path: path + '/messages' });

    let changeReceived = null;
    c1.on('change', msg => {
      if (msg.subscriptionPath === path + '/messages') {
        changeReceived = msg.value;
      }
    });

    await delay(50);
    await c2.request({ type: 'set', path, value: { messages: 'hello_child', other: 'data' } });

    await delay(100);
    assert(changeReceived === 'hello_child', 
      `Child subscriber should receive new value, got ${changeReceived}`);

    c1.close();
    c2.close();
  });

  // Test 4: 同步后版本更新，重连不重复回放
  await test('同步后版本更新，重连不重复回放旧变更', async () => {
    const writer = new TestClient(url);
    const reader = new TestClient(url);
    await writer.connect();
    await reader.connect();
    const path = '/test/ver_' + Date.now();

    const subResp = await reader.request({ type: 'subscribe', path });
    let verAfterSub = subResp.globalVersion;

    for (let i = 1; i <= 3; i++) {
      await writer.request({ type: 'set', path, value: 'v' + i, clientTimestamp: Date.now() + i * 1000 });
    }
    await delay(100);

    const getResp = await reader.request({ type: 'get', path });
    const verAfterWrites = getResp.globalVersion;
    assert(verAfterWrites > verAfterSub, 'Version should increase after writes');

    reader.close();
    await delay(50);

    const reader2 = new TestClient(url);
    await reader2.connect();
    
    const syncResp = await reader2.request({
      type: 'sync',
      lastKnownGlobalVersion: verAfterWrites,
      pendingWrites: []
    });
    assert(syncResp.changesSinceLast.length === 0, 
      `Expected 0 changes when already up to date, got ${syncResp.changesSinceLast.length}`);

    await writer.request({ type: 'set', path, value: 'v4', clientTimestamp: Date.now() + 10000 });
    await delay(50);

    const syncResp2 = await reader2.request({
      type: 'sync',
      lastKnownGlobalVersion: verAfterWrites,
      pendingWrites: []
    });
    assert(syncResp2.changesSinceLast.length >= 1, 
      `Expected at least 1 new change, got ${syncResp2.changesSinceLast.length}`);

    writer.close();
    reader2.close();
  });

  // Test 5: 离线写入时间更早 → 同步时被拒绝
  await test('离线写入时间更早 → 重连同步时被拒绝', async () => {
    const writer = new TestClient(url);
    await writer.connect();
    const path = '/test/offline_lww_' + Date.now();
    const base = Date.now();

    await writer.request({ type: 'set', path, value: 'SERVER_VAL', clientTimestamp: base + 5000 });

    const offlineWrite = {
      writeId: 'w_offline_' + Date.now(),
      op: 'set',
      path,
      value: 'OFFLINE_EARLIER',
      clientTimestamp: base
    };

    const syncResp = await writer.request({
      type: 'sync',
      lastKnownGlobalVersion: 0,
      pendingWrites: [offlineWrite]
    });

    assert(syncResp.conflicts && syncResp.conflicts.length >= 1, 
      'Should have at least 1 conflict');
    
    const ourConflict = syncResp.conflicts.find(c => c.writeId === offlineWrite.writeId);
    assert(ourConflict, 'Should find our write in conflicts');
    assert(ourConflict.rejected === true, 'Earlier write should be rejected');
    assert(ourConflict.winner === 'server', 'Winner should be server');
    assert(ourConflict.finalValue === 'SERVER_VAL', 'Final value should be server value');

    writer.close();
  });

  // Test 6: 重连后重新订阅，能继续收到推送
  await test('重连后重新订阅，继续收到后续推送', async () => {
    const writer = new TestClient(url);
    const subscriber = new TestClient(url);
    await writer.connect();
    await subscriber.connect();
    const path = '/test/resub_' + Date.now();

    await subscriber.request({ type: 'subscribe', path });
    let changeCount1 = 0;
    subscriber.on('change', msg => {
      if (msg.subscriptionPath === path) changeCount1++;
    });

    await writer.request({ type: 'set', path, value: 'v1', clientTimestamp: Date.now() });
    await delay(50);
    await writer.request({ type: 'set', path, value: 'v2', clientTimestamp: Date.now() + 1000 });
    await delay(100);
    assert(changeCount1 >= 2, `Should have 2 changes before disconnect, got ${changeCount1}`);

    subscriber.close();
    await delay(100);

    const sub2 = new TestClient(url);
    await sub2.connect();
    await sub2.request({ type: 'subscribe', path });
    
    let changeCount2 = 0;
    sub2.on('change', msg => {
      if (msg.subscriptionPath === path) changeCount2++;
    });

    await delay(50);
    await writer.request({ type: 'set', path, value: 'v3', clientTimestamp: Date.now() + 2000 });
    await delay(100);
    
    assert(changeCount2 >= 1, `Should have at least 1 change after reconnect, got ${changeCount2}`);

    writer.close();
    sub2.close();
  });

  console.log('\n═══════════════════════════════════════════');
  console.log(`结果: ${passed}/${passed + failed} 通过`);
  if (failed > 0) {
    console.log(`❌ ${failed} 个测试失败`);
    process.exit(1);
  } else {
    console.log('🎉 全部通过');
    process.exit(0);
  }
}

run().catch(e => { console.error('Fatal:', e); process.exit(1); });
