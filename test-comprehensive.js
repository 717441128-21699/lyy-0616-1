const WebSocket = require('ws');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
function assert(cond, msg) { if (!cond) throw new Error(msg || 'Assertion failed'); }

let testNum = 0;
function test(name, fn) {
  testNum++;
  return fn().then(() => {
    console.log(`✅ T${testNum}. ${name}`);
    return true;
  }).catch(e => {
    console.log(`❌ T${testNum}. ${name}: ${e.message}`);
    return false;
  });
}

async function run() {
  const baseUrl = 'ws://localhost:3000';
  let allPassed = true;

  console.log('═════════════════════════════════════════════════════');
  console.log('  综合测试：重连订阅 + 版本持久化 + LWW严格判定');
  console.log('═════════════════════════════════════════════════════\n');

  // === 测试1：重连后订阅仍然有效，能收到后续推送 ===
  allPassed &= await test('重连后订阅继续生效，能收到后续推送', async () => {
    const subPath = '/test/reconnect_sub_' + Date.now();
    const subClient = new WebSocket(baseUrl);
    const writeClient = new WebSocket(baseUrl);
    
    let subConnected = false, writeConnected = false;
    let changeCount1 = 0, changeCount2 = 0;
    let reconnected = false;
    let newWs = null;

    await new Promise((resolve, reject) => {
      subClient.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') {
          if (!reconnected) {
            subConnected = true;
            tryStart();
          }
        }
        if (msg.type === 'subscribed' && msg.path === subPath) {
          if (!reconnected) {
            doFirstWrites();
          } else {
            doSecondWrite();
          }
        }
        if (msg.type === 'change' && msg.subscriptionPath === subPath) {
          if (!reconnected) {
            changeCount1++;
            if (changeCount1 === 2) {
              setTimeout(() => {
                reconnected = true;
                subClient.close();
                newWs = new WebSocket(baseUrl);
                setupNewWs(newWs);
              }, 100);
            }
          }
        }
      });

      function setupNewWs(ws) {
        let newConnected = false;
        ws.on('message', data => {
          const msg = JSON.parse(data.toString());
          if (msg.type === 'connected' && !newConnected) {
            newConnected = true;
            ws.send(JSON.stringify({ type: 'subscribe', path: subPath, requestId: 'resub' }));
          }
          if (msg.type === 'change' && msg.subscriptionPath === subPath) {
            changeCount2++;
            if (changeCount2 >= 1) {
              setTimeout(() => {
                ws.close();
                writeClient.close();
                resolve();
              }, 200);
            }
          }
        });
      }

      writeClient.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { writeConnected = true; tryStart(); }
      });

      function tryStart() {
        if (subConnected && writeConnected) {
          subClient.send(JSON.stringify({ type: 'subscribe', path: subPath, requestId: 'sub1' }));
        }
      }

      function doFirstWrites() {
        writeClient.send(JSON.stringify({ type: 'set', path: subPath, value: 'v1', clientTimestamp: Date.now() }));
        setTimeout(() => {
          writeClient.send(JSON.stringify({ type: 'set', path: subPath, value: 'v2', clientTimestamp: Date.now() + 1000 }));
        }, 50);
      }

      function doSecondWrite() {
        writeClient.send(JSON.stringify({ type: 'set', path: subPath, value: 'v3', clientTimestamp: Date.now() + 2000 }));
      }

      setTimeout(() => reject(new Error('Timeout')), 8000);
    });

    assert(changeCount1 === 2, `Expected 2 changes before reconnect, got ${changeCount1}`);
    assert(changeCount2 >= 1, `Expected at least 1 change after reconnect, got ${changeCount2}`);
  });

  // === 测试2：LWW严格按客户端时间戳，更早的时间戳即使后到也被拒绝 ===
  allPassed &= await test('LWW严格按clientTimestamp：更早的写入后到也被拒绝', async () => {
    const ws = new WebSocket(baseUrl);
    const testPath = '/test/lww_strict_' + Date.now();
    const baseTime = Date.now();
    let step = 0;
    let finalValue = null;

    await new Promise((resolve, reject) => {
      ws.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') {
          step = 1;
          ws.send(JSON.stringify({
            type: 'set', path: testPath, value: 'NEWER',
            clientTimestamp: baseTime + 10000,
            writeId: 'w_newer'
          }));
        }
        if (msg.type === 'write_ack' && step === 1) {
          assert(msg.committed === true, 'First write should be committed');
          step = 2;
          ws.send(JSON.stringify({
            type: 'set', path: testPath, value: 'OLDER',
            clientTimestamp: baseTime,
            writeId: 'w_older'
          }));
        }
        if (msg.type === 'write_ack' && step === 2) {
          assert(msg.committed === false, 'Older write should be rejected');
          assert(msg.conflict, 'Should have conflict info');
          assert(msg.conflict.winner === 'existing', 'Winner should be existing');
          step = 3;
          ws.send(JSON.stringify({ type: 'get', path: testPath, requestId: 'get1' }));
        }
        if (msg.type === 'get_response' && step === 3) {
          finalValue = msg.value;
          ws.close();
          resolve();
        }
      });
      setTimeout(() => reject(new Error('Timeout')), 5000);
    });

    assert(finalValue === 'NEWER', `Final value should be NEWER, got ${finalValue}`);
  });

  // === 测试3：离线写入时间更早，重连同步时不覆盖服务端更新值 ===
  allPassed &= await test('离线写入时间更早 → 重连后不覆盖服务端更新值', async () => {
    const testPath = '/test/offline_lww_' + Date.now();
    const baseTime = Date.now();

    const writer = new WebSocket(baseUrl);
    const offlineClient = new WebSocket(baseUrl);
    let writerOK = false, offlineOK = false;
    let offlineWriteId = 'w_offline_test';

    await new Promise((resolve, reject) => {
      writer.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { writerOK = true; tryStart(); }
      });

      offlineClient.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') {
          if (!offlineOK) {
            offlineOK = true;
            tryStart();
          }
        }
        if (msg.type === 'sync_response') {
          assert(Array.isArray(msg.conflicts), 'Should have conflicts array');
          const ourConflict = msg.conflicts.find(c => c.writeId === offlineWriteId);
          assert(ourConflict, 'Should have our write in conflicts');
          assert(ourConflict.rejected === true, 'Our earlier write should be rejected');
          assert(ourConflict.winner === 'server', 'Winner should be server');
          assert(ourConflict.finalValue === 'SERVER_LATER', `Final value should be SERVER_LATER, got ${ourConflict.finalValue}`);
          
          setTimeout(() => {
            writer.close();
            offlineClient.close();
            resolve();
          }, 100);
        }
      });

      function tryStart() {
        if (writerOK && offlineOK) {
          setTimeout(doOfflineWrite, 100);
        }
      }

      function doOfflineWrite() {
        offlineClient.close();
        setTimeout(() => {
          writer.send(JSON.stringify({
            type: 'set', path: testPath, value: 'SERVER_LATER',
            clientTimestamp: baseTime + 5000,
            writeId: 'w_server'
          }));
          setTimeout(() => {
            const recon = new WebSocket(baseUrl);
            let reconOK = false;
            recon.on('message', data => {
              const msg = JSON.parse(data.toString());
              if (msg.type === 'connected' && !reconOK) {
                reconOK = true;
                recon.send(JSON.stringify({
                  type: 'sync',
                  lastKnownGlobalVersion: 0,
                  pendingWrites: [{
                    writeId: offlineWriteId,
                    op: 'set',
                    path: testPath,
                    value: 'CLIENT_EARLIER',
                    clientTimestamp: baseTime
                  }],
                  requestId: 'sync1'
                }));
              }
              if (msg.type === 'sync_response') {
                assert(Array.isArray(msg.conflicts), 'Should have conflicts array');
                const ourConflict = msg.conflicts.find(c => c.writeId === offlineWriteId);
                assert(ourConflict, `Should have our write in conflicts, got ${JSON.stringify(msg.conflicts)}`);
                assert(ourConflict.rejected === true, 'Our earlier write should be rejected');
                assert(ourConflict.winner === 'server', 'Winner should be server');
                
                setTimeout(() => {
                  recon.close();
                  writer.close();
                  resolve();
                }, 100);
              }
            });
            recon.on('error', reject);
          }, 200);
        }, 100);
      }

      setTimeout(() => reject(new Error('Timeout')), 8000);
    });
  });

  // === 测试4：同步后版本号正确更新，再次重连不重复回放 ===
  allPassed &= await test('同步后版本号正确，重连不重复回放已确认的变更', async () => {
    const testPath = '/test/version_persist_' + Date.now();
    const writer = new WebSocket(baseUrl);
    const reader = new WebSocket(baseUrl);

    let writerOK = false, readerOK = false;
    let versionAfterFirstSync = 0;
    let changesAfterFirstSync = 0;
    let changesAfterSecondSync = 0;

    await new Promise((resolve, reject) => {
      writer.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') { writerOK = true; tryStart(); }
      });

      reader.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') {
          if (!readerOK) {
            readerOK = true;
            tryStart();
          }
        }
        if (msg.type === 'subscribed' && msg.path === testPath) {
          versionAfterFirstSync = msg.globalVersion;
          doWrites();
        }
        if (msg.type === 'change' && msg.subscriptionPath === testPath) {
          changesAfterFirstSync++;
          if (changesAfterFirstSync === 3) {
            setTimeout(() => {
              versionAfterFirstSync = Math.max(versionAfterFirstSync, msg.globalVersion || 0);
              reader.close();
              setTimeout(doReconnect, 150);
            }, 50);
          }
        }
      });

      function tryStart() {
        if (writerOK && readerOK) {
          reader.send(JSON.stringify({ type: 'subscribe', path: testPath, requestId: 'sub1' }));
        }
      }

      function doWrites() {
        for (let i = 1; i <= 3; i++) {
          setTimeout(() => {
            writer.send(JSON.stringify({
              type: 'set', path: testPath, value: 'v' + i,
              clientTimestamp: Date.now() + i * 1000
            }));
          }, i * 60);
        }
      }

      function doReconnect() {
        const reader2 = new WebSocket(baseUrl);
        let gotSync = false;
        reader2.on('message', data => {
          const msg = JSON.parse(data.toString());
          if (msg.type === 'connected') {
            reader2.send(JSON.stringify({
              type: 'sync',
              lastKnownGlobalVersion: versionAfterFirstSync,
              pendingWrites: [],
              requestId: 'sync1'
            }));
          }
          if (msg.type === 'sync_response' && !gotSync) {
            gotSync = true;
            changesAfterSecondSync = msg.changesSinceLast.length;
            
            setTimeout(() => {
              assert(changesAfterSecondSync === 0, 
                `Expected 0 new changes after reconnect (already up to date), got ${changesAfterSecondSync}`);
              
              writer.send(JSON.stringify({
                type: 'set', path: testPath, value: 'v4',
                clientTimestamp: Date.now() + 5000
              }));
              
              setTimeout(() => {
                reader2.send(JSON.stringify({
                  type: 'sync',
                  lastKnownGlobalVersion: versionAfterFirstSync,
                  pendingWrites: [],
                  requestId: 'sync2'
                }));
              }, 100);
            }, 100);
          }
          if (msg.type === 'sync_response' && msg.requestId === 'sync2') {
            const newChanges = msg.changesSinceLast.length;
            assert(newChanges >= 1, `Expected at least 1 new change after extra write, got ${newChanges}`);
            
            setTimeout(() => {
              reader2.close();
              writer.close();
              resolve();
            }, 100);
          }
        });
        reader2.on('error', reject);
      }

      setTimeout(() => reject(new Error('Timeout')), 10000);
    });

    assert(changesAfterFirstSync === 3, `Expected 3 changes on first connection, got ${changesAfterFirstSync}`);
    assert(changesAfterSecondSync === 0, `Expected 0 changes on immediate reconnect, got ${changesAfterSecondSync}`);
  });

  // === 测试5：set()返回值中包含rejected和conflict信息 ===
  allPassed &= await test('set()返回值正确包含rejected和conflict信息', async () => {
    const ws = new WebSocket(baseUrl);
    const testPath = '/test/return_val_' + Date.now();
    const baseTime = Date.now();
    let results = [];

    await new Promise((resolve, reject) => {
      let step = 0;
      const pendingRequests = new Map();
      let reqId = 0;

      function sendRequest(msg) {
        return new Promise((res, rej) => {
          const id = 'req_' + (++reqId);
          msg.requestId = id;
          pendingRequests.set(id, { res, rej });
          ws.send(JSON.stringify(msg));
        });
      }

      ws.on('message', data => {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'connected') {
          step = 1;
          sendRequest({
            type: 'set', path: testPath, value: 'FIRST',
            clientTimestamp: baseTime + 1000,
            writeId: 'w1'
          }).then(r1 => {
            results.push(r1);
            return sendRequest({
              type: 'set', path: testPath, value: 'SECOND',
              clientTimestamp: baseTime,
              writeId: 'w2'
            });
          }).then(r2 => {
            results.push(r2);
            ws.close();
            resolve();
          }).catch(reject);
        }
        if (msg.requestId && pendingRequests.has(msg.requestId)) {
          const { res } = pendingRequests.get(msg.requestId);
          pendingRequests.delete(msg.requestId);
          res(msg);
        }
      });

      setTimeout(() => reject(new Error('Timeout')), 5000);
    });

    assert(results.length === 2, `Expected 2 results, got ${results.length}`);
    assert(results[0].committed === true, 'First write should be committed');
    assert(results[0].rejected === false, 'First write should not be rejected');
    assert(results[1].committed === false, 'Second (older) write should not be committed');
    assert(results[1].rejected === true, 'Second (older) write should be rejected');
    assert(results[1].conflict !== null, 'Should have conflict info');
    assert(results[1].conflict.winner === 'existing', 'Winner should be existing');
  });

  console.log('\n═════════════════════════════════════════════════════');
  if (allPassed) {
    console.log('🎉 所有综合测试通过！');
    process.exit(0);
  } else {
    console.log('❌ 部分测试失败');
    process.exit(1);
  }
}

run().catch(e => { console.error('Fatal:', e); process.exit(1); });
