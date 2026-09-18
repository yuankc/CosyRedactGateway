import test from 'node:test';
import assert from 'node:assert/strict';
import { handleRequest, RedactionContext, redactJson } from '../worker.js';

const url = 'https://gateway.example/E$https://upstream.example/v1/responses';
const encoder = new TextEncoder();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** 构造可观察拉取和取消的请求流，覆盖无长度头及伪造长度头。 */
function streamingRequest(chunks, headers = {}, signal) {
  const state = { pulls: 0, cancelled: false };
  const body = new ReadableStream({
    pull(controller) {
      const chunk = chunks[state.pulls++];
      if (chunk === undefined) controller.close();
      else controller.enqueue(encoder.encode(chunk));
    },
    cancel() { state.cancelled = true; }
  }, { highWaterMark: 0 });
  return { state, request: new Request(url, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body, duplex: 'half', signal }) };
}

test('declared oversized upload is rejected without pulling or waiting for source cancellation', { timeout: 1000 }, async () => {
  let pulls = 0, cancelled = false;
  const body = new ReadableStream({
    pull() { pulls++; },
    cancel() { cancelled = true; return new Promise(() => {}); }
  }, { highWaterMark: 0 });
  const request = new Request(url, { method: 'POST', body, duplex: 'half', headers: { 'content-length': '1000000000' } });
  const response = await handleRequest(request, { REDACT_MAX_BODY_BYTES: '10' }, { fetchImpl: () => assert.fail('must not forward') });
  assert.equal(response.status, 413);
  assert.equal(pulls, 0);
  assert.equal(cancelled, true);
});

test('actual UTF-8 bytes are bounded even without Content-Length or with a false smaller length', async () => {
  for (const headers of [{}, { 'content-length': '1' }]) {
    const { request, state } = streamingRequest(['中', '文', 'must not read'], headers);
    const response = await handleRequest(request, { REDACT_MAX_BODY_BYTES: '5' }, { fetchImpl: () => assert.fail('must not forward') });
    assert.equal(response.status, 413);
    assert.equal(state.pulls, 2);
    assert.equal(state.cancelled, true);
  }
});

test('a request exactly at its byte limit still redacts and restores correctly', async () => {
  const text = JSON.stringify({ input: '中 a@example.com' });
  const { request } = streamingRequest([text.slice(0, 9), text.slice(9)]);
  const response = await handleRequest(request, { REDACT_MAX_BODY_BYTES: String(encoder.encode(text).length) }, {
    fetchImpl: async (_url, init) => {
      assert(!init.body.includes('a@example.com'));
      const token = init.body.match(/\{\{Redact:[a-f0-9]{64}\}\}/)[0];
      return new Response(JSON.stringify({ output_text: token }), { headers: { 'content-type': 'application/json' } });
    }
  });
  assert.equal(response.status, 200);
  assert.equal((await response.json()).output_text, 'a@example.com');
});

test('cancelling an incomplete upload stops its reader and never starts an upstream request', { timeout: 1000 }, async () => {
  const abort = new AbortController();
  let cancelled = false;
  const body = new ReadableStream({ pull() {}, cancel() { cancelled = true; } });
  const request = new Request(url, { method: 'POST', body, duplex: 'half', signal: abort.signal });
  const pending = handleRequest(request, {}, { fetchImpl: () => assert.fail('must not forward') });
  abort.abort();
  assert.equal((await pending).status, 499);
  assert.equal(cancelled, true);
});

test('cancellation during a digest does not repopulate request mappings', async () => {
  const abort = new AbortController();
  const ctx = new RedactionContext({ salt: 'test', signal: abort.signal });
  const pending = ctx.tokenFor('a@example.com');
  abort.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(ctx.rawToToken.size, 0);
  assert.equal(ctx.tokenToRaw.size, 0);
  await assert.rejects(redactJson({ input: '{"mail":"a@example.com"}' }, ctx, new Set(['E'])), { name: 'AbortError' });
});

test('upstream header timeout aborts fetch and returns 504', { timeout: 1500 }, async () => {
  let signal;
  const response = await handleRequest(new Request(url), { REDACT_UPSTREAM_HEADER_TIMEOUT_MS: '20' }, {
    fetchImpl: (_url, init) => new Promise((_resolve, reject) => {
      signal = init.signal;
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    })
  });
  assert.equal(response.status, 504);
  assert.equal(signal.aborted, true);
  assert.equal(signal.reason.name, 'TimeoutError');
});

test('a stalled non-stream body is cancelled and returns 504 before sending a response', { timeout: 1500 }, async () => {
  let cancelled = false;
  const response = await handleRequest(new Request(url), { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '20' }, {
    fetchImpl: async () => new Response(new ReadableStream({ pull() {}, cancel() { cancelled = true; } }), { headers: { 'content-type': 'application/json' } })
  });
  assert.equal(response.status, 504);
  assert.equal(cancelled, true);
});

test('a stalled SSE read errors instead of emitting a successful end marker', { timeout: 1500 }, async () => {
  let cancelled = false, signal;
  const response = await handleRequest(new Request(url), { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '20' }, {
    fetchImpl: async (_url, init) => {
      signal = init.signal;
      return new Response(new ReadableStream({ pull() {}, cancel() { cancelled = true; } }), { headers: { 'content-type': 'text/event-stream' } });
    }
  });
  assert.equal(response.status, 200);
  await assert.rejects(response.text(), { name: 'TimeoutError' });
  assert.equal(cancelled, true);
  assert.equal(signal.aborted, true);
});

test('downstream backpressure does not run the upstream idle timer', { timeout: 1500 }, async () => {
  let signal, pulls = 0, cancelled = false;
  const response = await handleRequest(new Request(url), { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '10' }, {
    fetchImpl: async (_url, init) => {
      signal = init.signal;
      return new Response(new ReadableStream({
        pull(c) { pulls++; c.enqueue(encoder.encode('data: [DONE]\n\n')); },
        cancel() { cancelled = true; }
      }, { highWaterMark: 0 }), { headers: { 'content-type': 'text/event-stream' } });
    }
  });
  await sleep(40);
  assert.equal(pulls, 0);
  assert.equal(signal.aborted, false);
  const reader = response.body.getReader();
  await reader.read();
  await sleep(40);
  assert.equal(pulls, 1);
  assert.equal(signal.aborted, false);
  await reader.cancel();
  assert.equal(cancelled, true);
  assert.equal(signal.aborted, true);
});

test('regular SSE chunks reset idle time even when the complete response exceeds the timeout', { timeout: 2000 }, async () => {
  let signal, sent = 0, cancelled = false;
  const response = await handleRequest(new Request(url), { REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '100' }, {
    fetchImpl: async (_url, init) => {
      signal = init.signal;
      return new Response(new ReadableStream({
        async pull(c) {
          if (sent === 6) { c.close(); return; }
          await sleep(25);
          if (!cancelled) { sent++; c.enqueue(encoder.encode('data: {"type":"response.output_text.delta","delta":"x"}\n\n')); }
        },
        cancel() { cancelled = true; }
      }, { highWaterMark: 0 }), { headers: { 'content-type': 'text/event-stream' } });
    }
  });
  const text = await response.text();
  assert.equal((text.match(/"delta":"x"/g) || []).length, 6);
  assert.equal(signal.aborted, false);
});

test('cancelling a response releases mappings, pending SSE state and upstream source', async t => {
  const original = RedactionContext.prototype.tokenFor;
  let ctx, signal, cancelled = false;
  t.mock.method(RedactionContext.prototype, 'tokenFor', function(raw) { ctx = this; return original.call(this, raw); });
  const response = await handleRequest(new Request(url, { method: 'POST', body: '{"input":"a@example.com"}', headers: { 'content-type': 'application/json' } }), {}, {
    fetchImpl: async (_url, init) => {
      signal = init.signal;
      return new Response(new ReadableStream({ pull() {}, cancel() { cancelled = true; } }), { headers: { 'content-type': 'text/event-stream' } });
    }
  });
  assert.equal(ctx.rawToToken.size, 1);
  await response.body.cancel();
  assert.equal(ctx.rawToToken.size, 0);
  assert.equal(ctx.tokenToRaw.size, 0);
  assert.equal(signal.aborted, true);
  assert.equal(cancelled, true);
});

test('client abort cancels an unread response and successful completion detaches the request signal', async () => {
  const abort = new AbortController();
  let signal, cancelled = false;
  const response = await handleRequest(new Request(url, { signal: abort.signal }), {}, {
    fetchImpl: async (_url, init) => {
      signal = init.signal;
      return new Response(new ReadableStream({ cancel() { cancelled = true; } }), { headers: { 'content-type': 'application/octet-stream' } });
    }
  });
  abort.abort();
  await assert.rejects(response.text(), { name: 'AbortError' });
  assert.equal(cancelled, true);
  assert.equal(signal.aborted, true);

  const normal = new AbortController();
  const completed = await handleRequest(new Request(url, { signal: normal.signal }), { REDACT_UPSTREAM_HEADER_TIMEOUT_MS: '10', REDACT_UPSTREAM_IDLE_TIMEOUT_MS: '10' }, {
    fetchImpl: async (_url, init) => { signal = init.signal; return new Response('{"ok":true}', { headers: { 'content-type': 'application/json' } }); }
  });
  assert.deepEqual(await completed.json(), { ok: true });
  normal.abort();
  await sleep(30);
  assert.equal(signal.aborted, false);
});
