import test from 'node:test';
import assert from 'node:assert/strict';
import { selectSensitiveSpans, findSensitiveSpans, parseFlags, handleRequest } from '../worker.js';

const url = 'https://gateway.example/E$https://upstream.example/v1/responses';
const encoder = new TextEncoder();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/** 保留原版逐项重叠判断作为独立判定基准，验证新查询结构的选择顺序。 */
function reference(candidates, protectedSpans) {
  const overlaps = (a,b) => a.start < b.end && a.end > b.start;
  const ordered = candidates.filter(a => !protectedSpans.some(b => overlaps(a,b)));
  ordered.sort((a,b) => b.priority-a.priority || (b.end-b.start)-(a.end-a.start) || a.start-b.start);
  const result = [];
  for (const span of ordered) if (!result.some(other => overlaps(span,other))) result.push(span);
  return result.sort((a,b) => a.start-b.start);
}

test('interval queries preserve original priority, tie, containment, touching and protected-span behavior', () => {
  let seed = 12345;
  const random = n => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % n; };
  for (let round = 0; round < 300; round++) {
    const candidates = Array.from({ length: 100 }, (_, id) => {
      const start = random(300);
      return { start, end: start+1+random(60), priority: random(4), id };
    });
    const protectedSpans = [{ start: 10, end: 30 }, { start: 80, end: 100 }];
    assert.deepEqual(selectSensitiveSpans([...candidates], protectedSpans), reference(candidates, protectedSpans));
  }
  const adjacent = [{ start: 0, end: 10, priority: 1 }, { start: 10, end: 20, priority: 1 }];
  assert.deepEqual(selectSensitiveSpans([...adjacent]), adjacent);
  const duplicates = [{ start: 0, end: 10, priority: 1, id: 0 }, { start: 0, end: 10, priority: 1, id: 1 }];
  assert.deepEqual(selectSensitiveSpans(duplicates), [duplicates[0]]);
});

test('candidate limits apply before overlap elimination and accumulate across JSON fields', async () => {
  assert.throws(() => findSensitiveSpans('a@example.com b@example.com', parseFlags('E'), { maxCandidates: 1, candidateCount: 0 }), /Candidate limit/);
  const response = await handleRequest(new Request(url, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ input: [{ content: 'a@example.com' }, { content: '{"email":"b@example.com"}' }] })
  }), { REDACT_MAX_CANDIDATES: '1' }, { fetchImpl: () => assert.fail('must not forward partially redacted data') });
  assert.equal(response.status, 413);
  assert.match(await response.text(), /Candidate limit/);
});

/** 用指定事件文本构造可取消的模拟 SSE 响应。 */
async function sse(text, env) {
  let sent = false, cancelled = false, signal;
  const response = await handleRequest(new Request(url), env, { fetchImpl: async (_url, init) => {
    signal = init.signal;
    return new Response(new ReadableStream({
      pull(c) { if (!sent) { sent = true; c.enqueue(encoder.encode(text)); } else c.close(); },
      cancel() { cancelled = true; }
    }, { highWaterMark: 0 }), { headers: { 'content-type': 'text/event-stream' } });
  } });
  return { response, cancelled: () => cancelled, signal };
}

test('oversized UTF-8 events and unterminated events fail closed and cancel upstream', async () => {
  for (const suffix of ['', '\n\n']) {
    const result = await sse('data: ' + '中'.repeat(100) + suffix, { REDACT_MAX_SSE_EVENT_BYTES: '200' });
    await assert.rejects(result.response.text(), /SSE event exceeds/);
    assert.equal(result.signal.aborted, true);
    assert.equal(result.cancelled(), true);
  }
});

test('one large HTTP chunk containing many small events does not exceed the per-event limit', async () => {
  const event = 'data: {"type":"response.output_text.delta","delta":"hello"}\n\n';
  const result = await sse(event.repeat(1000), { REDACT_MAX_SSE_EVENT_BYTES: '100' });
  assert.equal(await result.response.text(), event.repeat(1000));
});

test('an unfinished placeholder cannot grow pending event count or bytes without a bound', async () => {
  const pending = 'data: {"type":"response.output_text.delta","delta":"{{Redact:"}\n\n';
  for (const env of [{ REDACT_MAX_SSE_QUEUE_EVENTS: '2' }, { REDACT_MAX_SSE_QUEUE_BYTES: '80' }]) {
    const result = await sse(pending + ': heartbeat\n\n'.repeat(20), env);
    await assert.rejects(result.response.text(), /SSE pending queue limit/);
    assert.equal(result.signal.aborted, true);
    assert.equal(result.cancelled(), true);
  }
});

/** 构造不主动完成上传的请求，用实际拉取次数确认排队阶段不读请求体。 */
function heldRequest() {
  const abort = new AbortController();
  const state = { pulls: 0, cancelled: false, controller: null };
  const body = new ReadableStream({
    start(c) { state.controller = c; },
    pull() { state.pulls++; },
    cancel() { state.cancelled = true; }
  }, { highWaterMark: 0 });
  const request = new Request(url, { method: 'POST', body, duplex: 'half', headers: { 'content-type': 'application/json' }, signal: abort.signal });
  return { request, state, abort };
}

test('redaction queue is bounded, cancellation removes waiters, and slots are reusable', { timeout: 2000 }, async t => {
  const env = { REDACT_MAX_CONCURRENT: '1', REDACT_MAX_QUEUE: '1', REDACT_QUEUE_TIMEOUT_MS: '1000' };
  const options = { fetchImpl: async () => new Response('{}', { headers: { 'content-type': 'application/json' } }) };
  const first = heldRequest(), second = heldRequest(), third = heldRequest();
  t.after(() => { first.abort.abort(); second.abort.abort(); third.abort.abort(); });
  const active = handleRequest(first.request, env, options);
  await sleep(10);
  assert.equal(first.state.pulls, 1);
  const queued = handleRequest(second.request, env, options);
  const rejected = await handleRequest(third.request, env, options);
  assert.equal(rejected.status, 503);
  assert.equal(rejected.headers.get('retry-after'), '1');
  assert.equal(second.state.pulls, 0);
  assert.equal(third.state.pulls, 0);
  assert.equal(third.state.cancelled, true);
  second.abort.abort();
  assert.equal((await queued).status, 499);
  assert.equal(second.state.cancelled, true);
  const next = handleRequest(new Request(url, { method: 'POST', body: '{}', headers: { 'content-type': 'application/json' } }), env, options);
  first.state.controller.enqueue(encoder.encode('{}')); first.state.controller.close();
  assert.equal((await active).status, 200);
  assert.equal((await next).status, 200);
});

test('queue timeout removes its request without stealing the active slot', { timeout: 2000 }, async t => {
  const first = heldRequest(), second = heldRequest();
  t.after(() => { first.abort.abort(); second.abort.abort(); });
  const env = { REDACT_MAX_CONCURRENT: '1', REDACT_MAX_QUEUE: '1', REDACT_QUEUE_TIMEOUT_MS: '20' };
  const options = { fetchImpl: () => assert.fail('unfinished uploads must not forward') };
  const active = handleRequest(first.request, env, options);
  await sleep(10);
  const response = await handleRequest(second.request, env, options);
  assert.equal(response.status, 503);
  assert.match(await response.text(), /queue timeout/);
  assert.equal(second.state.pulls, 0);
  assert.equal(second.state.cancelled, true);
  first.abort.abort();
  assert.equal((await active).status, 499);
});

test('fractional limits cannot truncate to zero and permanently block redaction admission', { timeout: 2000 }, async () => {
  const response = await handleRequest(new Request(url, { method: 'POST', body: '{}', headers: { 'content-type': 'application/json' } }), {
    REDACT_MAX_CONCURRENT: '0.5', REDACT_MAX_QUEUE: '0.5', REDACT_QUEUE_TIMEOUT_MS: '0.5'
  }, { fetchImpl: async () => new Response('{}', { headers: { 'content-type': 'application/json' } }) });
  assert.equal(response.status, 200);
});
