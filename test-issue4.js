const WebSocket = require('ws');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
function assert(cond, msg) { if (!cond) throw new Error(msg || 'Assertion failed'); }

async function testOfflineSyncAndVersionPersistence() {
  console.log('\n=== 测试问题4：同步后版本更新 + 重连不重复回放 ===\n');
  const baseUrl = 'ws://localhost:3000';
  const testPath = '/test/version_test_' + Date.now();

  const writer = new WebSocket(baseUrl);
  const client = new WebSocket(baseUrl);
  let writerConnected = false, clientConnected = false;
  let clientGlobalVersion = 0;
  let changeCount = 0;

  await new Promise((resolve, reject) => {
    writer.on('message', data => {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'connected') {
        writerConnected = true;
        tryStart();
      }
    });

    client.on('message', data => {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'connected') {
        clientConnected = true;
        tryStart();
      }
      if (msg.type === 'subscribed' && msg.path === testPath) {
        clientGlobalVersion = msg.globalVersion;
        console.log(`  初始订阅完成，globalVersion = ${clientGlobalVersion}`);
        doWrites();
      }
      if (msg.type === 'change' && msg.subscriptionPath === testPath) {
        changeCount++;
        clientGlobalVersion = Math.max(clientGlobalVersion, msg.globalVersion || 0);
        console.log(`  收到变更 #${changeCount}, value=${msg.value}, globalVersion=${msg.globalVersion}`);
        if (changeCount === 3) {
          console.log('  ✓ 首次连接收到全部3次变更');
          testDisconnectReconnect();
        }
      }
      if (msg.type === 'sync_response') {
        clientGlobalVersion = msg.serverGlobalVersion;
        console.log(`  同步完成，serverGlobalVersion = ${msg.serverGlobalVersion}`);
        console.log(`  本次同步收到变更数: ${msg.changesSinceLast.length}`);
        
        if (changeCount === 3) {
          console.log('  ✓ 重连后变更数仍为3，没有重复回放');
          setTimeout(() => {
            writer.close();
            client.close();
            resolve();
          }, 200);
        }
      }
    });

    function tryStart() {
      if (writerConnected && clientConnected) {
        client.send(JSON.stringify({ type: 'subscribe', path: testPath, requestId: 'sub1' }));
      }
    }

    let writeStep = 0;
    function doWrites() {
      const write = (val) => {
        writeStep++;
        writer.send(JSON.stringify({
          type: 'set', path: testPath, value: val,
          clientTimestamp: Date.now(), writeId: 'w_' + writeStep
        }));
      };
      write('val1');
      setTimeout(() => write('val2'), 50);
      setTimeout(() => write('val3'), 100);
    }

    function testDisconnectReconnect() {
      console.log('\n  --- 模拟断开连接 ---');
      client.close();
      
      setTimeout(() => {
        console.log(`  本地缓存版本 = ${clientGlobalVersion}`);
        console.log('  --- 重新连接并同步 ---');
        
        const client2 = new WebSocket(baseUrl);
        client2.on('message', data => {
          const msg = JSON.parse(data.toString());
          if (msg.type === 'connected') {
            client2.send(JSON.stringify({
              type: 'sync',
              lastKnownGlobalVersion: clientGlobalVersion,
              pendingWrites: [],
              requestId: 'sync1'
            }));
          } else if (msg.type === 'sync_response' && msg.requestId === 'sync1') {
            clientGlobalVersion = msg.serverGlobalVersion;
            console.log(`  同步完成，serverGlobalVersion = ${msg.serverGlobalVersion}`);
            console.log(`  本次同步收到变更数: ${msg.changesSinceLast.length}`);
            assert(msg.changesSinceLast.length === 0, `Expected 0 new changes, got ${msg.changesSinceLast.length}`);
            console.log('  ✓ 重连后没有重复回放已确认的旧变更');
            
            setTimeout(() => {
              console.log('\n  测试额外写入后再次重连...');
              writer.send(JSON.stringify({
                type: 'set', path: testPath, value: 'val4',
                clientTimestamp: Date.now(), writeId: 'w_extra'
              }));
              
              setTimeout(() => {
                client2.send(JSON.stringify({
                  type: 'sync',
                  lastKnownGlobalVersion: clientGlobalVersion,
                  pendingWrites: [],
                  requestId: 'sync2'
                }));
              }, 100);
            }, 200);
          } else if (msg.type === 'sync_response' && msg.requestId === 'sync2') {
            clientGlobalVersion = msg.serverGlobalVersion;
            console.log(`  二次同步，收到新变更数: ${msg.changesSinceLast.length}`);
            assert(msg.changesSinceLast.length === 1, `Expected 1 new change, got ${msg.changesSinceLast.length}`);
            assert(msg.changesSinceLast[0].value === 'val4', `Expected val4, got ${msg.changesSinceLast[0].value}`);
            console.log('  ✓ 只拉取了新的变更');
            
            client2.close();
            writer.close();
            resolve();
          }
        });
      }, 200);
    }

    writer.on('error', reject);
    client.on('error', reject);
    setTimeout(() => reject(new Error('Timeout')), 8000);
  });

  console.log('\n✅ 问题4测试通过：版本正确更新，重连不重复回放');
}

async function run() {
  try {
    await testOfflineSyncAndVersionPersistence();
    console.log('\n🎉 所有问题4测试通过');
    process.exit(0);
  } catch (e) {
    console.error('\n❌ 测试失败:', e.message);
    process.exit(1);
  }
}

run();
