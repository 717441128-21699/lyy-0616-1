const WebSocket = require('ws');

const TEST_DELAY = 500;
const results = [];

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

function test(name, fn) {
  return fn().then(() => {
    console.log(`✅ ${name}`);
    results.push({ name, pass: true });
  }).catch(e => {
    console.log(`❌ ${name}: ${e.message}`);
    results.push({ name, pass: false, error: e.message });
  });
}

function assert(cond, msg) {
  if (!cond) throw new Error(msg || 'Assertion failed');
}

async function run() {
  const baseUrl = 'ws://localhost:3000';

  await test('1. 客户端脚本可加载 (HTTP 200)', async () => {
    const http = require('http');
    await new Promise((resolve, reject) => {
      const req = http.get('http://localhost:3000/realtime-sync.js', res => {
        assert(res.statusCode === 200, 'Expected 200, got ' + res.statusCode);
        assert(res.headers['content-type'].includes('javascript'), 'Expected javascript');
        res.resume();
        resolve();
      });
      req.on('error', reject);
    });
  });

  await test('2. WebSocket 连接成功并收到 connected 消息', async () => {
    const ws = new WebSocket(baseUrl);
    await new Promise((resolve, reject) => {
      ws.on('message', data => {
        const msg = JSON.parse(data.toString());
        assert(msg.type === 'connected', 'Expected connected, got ' + msg.type);
        assert(msg.clientId, 'Expected clientId');
        ws.close();
        resolve();
      });
      ws.on('error', reject);
      setTimeout(() => reject(new Error('Timeout')), 2000);
    });
  });

  await test('3. 订阅后收到初始快照', async () => {
    const ws = new WebSocket(baseUrl);
    const reqId = 'test_' + Date.now();
    await new Promise((resolve, reject) => {
      let gotConnected = false;
      ws.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { gotConnected = true; ws.send(JSON.stringify({ type: 'subscribe', path: '/test', requestId: reqId })); }
        if (msg.type === 'subscribed' && msg.requestId === reqId) {
          assert(msg.path === '/test', 'Expected path /test');
          assert('value' in msg, 'Expected value field');
          ws.close();
          resolve();
        }
      });
      ws.on('error', reject);
      setTimeout(() => reject(new Error('Timeout')), 2000);
    });
  });

  await test('4. 写入后订阅者收到 change 推送', async () => {
    const ws1 = new WebSocket(baseUrl);
    const ws2 = new WebSocket(baseUrl);

    await new Promise((resolve, reject) => {
      let ws1Connected = false, ws2Connected = false;
      let ws1Subscribed = false;
      const testPath = '/test/change_' + Date.now();

      ws1.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { ws1Connected = true; trySubscribe(); }
        if (msg.type === 'subscribed') { ws1Subscribed = true; tryWrite(); }
        if (msg.type === 'change') {
          assert(msg.subscriptionPath === testPath, 'Expected change on ' + testPath);
          assert(msg.value === 42, 'Expected value 42');
          ws1.close(); ws2.close();
          resolve();
        }
      });

      ws2.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { ws2Connected = true; trySubscribe(); }
      });

      function trySubscribe() { if (ws1Connected) ws1.send(JSON.stringify({ type: 'subscribe', path: testPath, requestId: 'sub1' })); }
      function tryWrite() { if (ws2Connected) ws2.send(JSON.stringify({ type: 'set', path: testPath, value: 42 })); }

      ws1.on('error', reject);
      ws2.on('error', reject);
      setTimeout(() => reject(new Error('Timeout')), 3000);
    });
  });

  await test('5. LWW 时间戳判定：时间早的写入不覆盖时间晚的', async () => {
    const ws1 = new WebSocket(baseUrl);
    const now = Date.now();
    const testPath = '/test/lww_' + Date.now();

    await new Promise((resolve, reject) => {
      let connected = false;
      let step = 0;

      ws1.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected' && !connected) {
          connected = true;
          ws1.send(JSON.stringify({
            type: 'set', path: testPath, value: 'LATER',
            clientTimestamp: now + 10000, writeId: 'w_later'
          }));
          step = 1;
        } else if (msg.type === 'write_ack' && step === 1) {
          ws1.send(JSON.stringify({
            type: 'set', path: testPath, value: 'EARLIER',
            clientTimestamp: now, writeId: 'w_earlier'
          }));
          step = 2;
        } else if (msg.type === 'write_ack' && step === 2) {
          assert(msg.committed === false, 'Expected early write to be rejected');
          assert(msg.conflict, 'Expected conflict info');
          assert(msg.conflict.winner === 'existing', 'Expected existing to win');
          ws1.send(JSON.stringify({ type: 'get', path: testPath, requestId: 'get1' }));
          step = 3;
        } else if (msg.type === 'get_response' && step === 3) {
          assert(msg.value === 'LATER', `Expected 'LATER', got '${msg.value}'`);
          ws1.close();
          resolve();
        }
      });

      ws1.on('error', reject);
      setTimeout(() => reject(new Error('Timeout')), 3000);
    });
  });

  await test('6. 订阅子路径时，父路径变更能收到通知', async () => {
    const ws1 = new WebSocket(baseUrl);
    const ws2 = new WebSocket(baseUrl);
    const testPath = '/test/parent_child_' + Date.now();

    await new Promise((resolve, reject) => {
      let ws1OK = false, ws2OK = false;

      ws1.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { ws1OK = true; ws1.send(JSON.stringify({ type: 'subscribe', path: testPath + '/messages', requestId: 'sub' })); }
        else if (msg.type === 'change' && msg.subscriptionPath === testPath + '/messages') {
          assert(msg.value === 'hello_child', 'Expected child value to be updated');
          ws1.close(); ws2.close();
          resolve();
        }
      });

      ws2.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { ws2OK = true; tryWrite(); }
      });

      function tryWrite() {
        if (ws1OK && ws2OK) {
          setTimeout(() => {
            ws2.send(JSON.stringify({
              type: 'set', path: testPath,
              value: { messages: 'hello_child', other: 'data' }
            }));
          }, 200);
        }
      }

      ws1.on('error', reject);
      ws2.on('error', reject);
      setTimeout(() => reject(new Error('Timeout')), 3000);
    });
  });

  console.log('\n═══════════════════════════════════════════');
  const passed = results.filter(r => r.pass).length;
  console.log(`测试结果: ${passed}/${results.length} 通过`);
  if (passed < results.length) {
    console.log('\n失败的测试:');
    results.filter(r => !r.pass).forEach(r => console.log(`  ❌ ${r.name}: ${r.error}`));
    process.exit(1);
  } else {
    console.log('🎉 所有测试通过');
    process.exit(0);
  }
}

run().catch(e => { console.error('Fatal:', e); process.exit(1); });
