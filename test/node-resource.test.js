import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** 启动隔离的真实上游和网关进程，测试结束后关闭全部测试连接。 */
async function servers(t, handler, env = {}) {
  const upstream = http.createServer(handler);
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  const child = spawn(process.execPath, ['node-server.mjs'], {
    cwd: new URL('..', import.meta.url),
    env: { ...process.env, HOST: '127.0.0.1', PORT: '0', REDACT_BLOCK_PRIVATE_UPSTREAMS: 'false', REDACT_MAX_BODY_BYTES: '1024', REDACT_UPSTREAM_HEADER_TIMEOUT_MS: '2000', REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '2000', ...env },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let stderr = '';
  child.stderr.on('data', data => { stderr += data; });
  t.after(async () => {
    if (child.exitCode === null) { const exited = once(child, 'exit'); child.kill(); await exited; }
    upstream.closeAllConnections();
    await new Promise(resolve => upstream.close(resolve));
  });
  const port = await new Promise((resolve, reject) => {
    let output = '';
    const timer = setTimeout(() => reject(new Error(`Gateway startup timed out: ${stderr}`)), 5000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); reject(new Error(`Gateway exited (${code}): ${stderr}`)); });
    child.stdout.on('data', data => {
      output += data;
      const match = output.match(/listening on http:\/\/127\.0\.0\.1:(\d+)/);
      if (match) { clearTimeout(timer); resolve(Number(match[1])); }
    });
  });
  return { url: `http://127.0.0.1:${port}/E$http://127.0.0.1:${upstream.address().port}/v1/responses`, health: `http://127.0.0.1:${port}/healthz`, child };
}

/** 等待可观察的上游关闭或背压状态，超时给出明确断言失败。 */
async function until(predicate, timeout = 3000) {
  const end = Date.now() + timeout;
  while (!predicate() && Date.now() < end) await sleep(20);
  assert(predicate(), 'Expected connection or backpressure state was not reached');
}

test('oversized declared and chunked uploads receive 413 without forwarding or breaking the gateway', { timeout: 15000 }, async t => {
  let forwarded = 0;
  const gateway = await servers(t, (_req, res) => { forwarded++; res.end('{}'); });
  for (const declared of [true, false]) {
    const response = await new Promise((resolve, reject) => {
      const req = http.request(gateway.url, { method: 'POST', headers: { 'content-type': 'application/json', ...(declared ? { 'content-length': '1000000000' } : {}) } }, res => {
        let body = '';
        res.setEncoding('utf8'); res.on('data', data => { body += data; });
        res.on('end', () => { req.destroy(); resolve({ status: res.statusCode, body }); });
        res.on('error', reject);
      });
      req.on('error', reject);
      t.after(() => req.destroy());
      if (declared) req.flushHeaders();
      else req.write('x'.repeat(2048)); // 不结束上传，确认无需等到 EOF 才拒绝。
    });
    assert.equal(response.status, 413);
    assert.match(response.body, /exceeds 1024 bytes/);
  }
  assert.equal(forwarded, 0);
  assert.equal((await fetch(gateway.health)).status, 200);
});

test('a client disconnect while awaiting headers promptly closes the upstream socket', { timeout: 15000 }, async t => {
  let started = false, closed = false;
  const gateway = await servers(t, (req, res) => {
    req.resume(); started = true;
    res.on('close', () => { closed = true; });
  });
  const req = http.get(gateway.url);
  req.on('error', () => {});
  t.after(() => req.destroy());
  await until(() => started);
  req.destroy();
  await until(() => closed, 1500);
  assert.equal((await fetch(gateway.health)).status, 200);
});

test('a real header timeout returns 504 and cancels the upstream connection', { timeout: 15000 }, async t => {
  let closed = false;
  const gateway = await servers(t, (req, res) => { req.resume(); res.on('close', () => { closed = true; }); }, { REDACT_UPSTREAM_HEADER_TIMEOUT_MS: '100' });
  const response = await fetch(gateway.url);
  assert.equal(response.status, 504);
  await response.arrayBuffer();
  await until(() => closed);
});

test('an SSE idle timeout terminates the HTTP stream instead of appending a fake success', { timeout: 15000 }, async t => {
  let closed = false;
  const gateway = await servers(t, (req, res) => {
    req.resume(); res.on('close', () => { closed = true; });
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.write('data: {"type":"response.output_text.delta","delta":"hello"}\n\n');
  }, { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '150' });
  const response = await fetch(gateway.url);
  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  assert.match(new TextDecoder().decode((await reader.read()).value), /hello/);
  await assert.rejects(reader.read());
  await until(() => closed);
  assert.equal((await fetch(gateway.health)).status, 200);
});

test('a paused HTTP client backpressures SSE and disconnect releases the upstream', { timeout: 20000 }, async t => {
  let written = 0, closed = false, ended = false;
  const chunk = `data: ${JSON.stringify({ type: 'response.output_text.delta', delta: 'x'.repeat(32 * 1024) })}\n\n`;
  const total = 64 * 1024 * 1024;
  const gateway = await servers(t, (req, res) => {
    req.resume();
    res.writeHead(200, { 'content-type': 'text/event-stream' });
    res.on('close', () => { closed = true; });
    const pump = () => {
      while (!res.destroyed && written < total) {
        written += Buffer.byteLength(chunk);
        if (!res.write(chunk)) { res.once('drain', pump); return; }
      }
      if (!res.destroyed) { ended = true; res.end(); }
    };
    pump();
  }, { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '10000' });
  let req;
  const response = await new Promise((resolve, reject) => {
    req = http.get(gateway.url, res => { res.pause(); resolve(res); });
    req.on('error', reject);
  });
  t.after(() => { response.destroy(); req.destroy(); });
  let stable = 0, previous = -1;
  for (let i = 0; i < 30 && stable < 3; i++) {
    await sleep(100);
    stable = written === previous ? stable + 1 : 0;
    previous = written;
  }
  assert(stable >= 3, 'Upstream must stop producing while the client remains paused');
  assert.equal(ended, false, 'Gateway must not consume the entire response into its own buffers');
  assert(written < total);
  t.diagnostic(`Paused client: upstream stopped at ${written} bytes of a ${total}-byte response`);
  assert.equal((await fetch(gateway.health)).status, 200);
  response.destroy(); req.destroy();
  await until(() => closed, 1500);
});

test('cancelling half of concurrent requests leaves other request mappings and streams intact', { timeout: 15000 }, async t => {
  const closed = new Set(), finish = new Map();
  const gateway = await servers(t, (req, res) => {
    let body = '';
    req.setEncoding('utf8'); req.on('data', value => { body += value; });
    req.on('end', () => {
      const id = Number(req.headers['x-test-id']);
      const token = JSON.parse(body).input.match(/\{\{Redact:[a-f0-9]{64}\}\}/)[0];
      res.on('close', () => closed.add(id));
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      res.write('data: {"type":"response.output_text.delta","delta":"ready "}\n\n');
      finish.set(id, () => res.end(`data: ${JSON.stringify({ type: 'response.output_text.delta', delta: token })}\n\n`));
    });
  });
  const aborts = Array.from({ length: 8 }, () => new AbortController());
  const responses = await Promise.all(aborts.map((abort, id) => fetch(gateway.url, {
    method: 'POST', signal: abort.signal,
    headers: { 'content-type': 'application/json', 'x-test-id': String(id) },
    body: JSON.stringify({ input: `person${id}@example.com`, stream: true })
  })));
  for (let id = 0; id < 8; id += 2) aborts[id].abort();
  await until(() => [0, 2, 4, 6].every(id => closed.has(id)), 1500);
  assert.equal(closed.size, 4);
  for (let id = 1; id < 8; id += 2) finish.get(id)();
  for (let id = 0; id < 8; id++) {
    if (id % 2 === 0) await assert.rejects(responses[id].text());
    else {
      const text = await responses[id].text();
      assert(text.includes(`person${id}@example.com`));
      assert(!text.includes('{{Redact:'));
    }
  }
});
