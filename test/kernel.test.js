import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { compileKeywordMatcher, RedactionContext, parseFlags, redactJson, handleRequest } from '../worker.js';
import { createNodeDigest } from '../node-crypto.mjs';

/** 固定种子覆盖关键词前后缀、重叠、重复、空关键词和 UTF-16 字符。 */
test('keyword automaton agrees with includes for every rule across deterministic overlapping inputs', () => {
  let seed = 20260918;
  const next = n => { seed ^= seed << 13; seed ^= seed >>> 17; seed ^= seed << 5; return (seed >>> 0) % n; };
  const alphabet = 'abcde_-中😀';
  const word = length => Array.from({ length }, () => alphabet[next(alphabet.length)]).join('');
  const lists = [[], [''], ['a', 'ab'], ['bc', 'abc'], ['c'], ['same', 'same'], ['中😀'],
    ...Array.from({ length: 180 }, () => Array.from({ length: 1 + next(4) }, () => word(1 + next(12))))];
  const match = compileKeywordMatcher(lists);
  for (let i = 0; i < 500; i++) {
    const text = (word(next(300)) + 'abc' + lists[next(lists.length)].join('') + '中😀').toLowerCase();
    assert.deepEqual([...match(text)], lists.map(words => Number(!words.length || words.some(word => text.includes(word)))));
  }
  assert.deepEqual([...match('')], lists.map(words => Number(!words.length || words.includes(''))));
});

test('Node and Web Crypto tokens match for Unicode, lone surrogates, salts and repeated values', async () => {
  for (const salt of ['', 'fixed', '中文😀\ud800']) {
    const web = new RedactionContext({ salt });
    const node = new RedactionContext({ salt, digestHex: createNodeDigest() });
    for (const raw of ['', 'a@example.com', '中文😀', '\ud800', '\udc00x', 'x'.repeat(65536)]) {
      const expected = '{{Redact:' + createHash('sha256').update(raw + salt).digest('hex') + '}}';
      assert.equal(await web.tokenFor(raw), expected);
      assert.equal(await node.tokenFor(raw), expected);
      assert.equal(await node.tokenFor(raw), expected);
      assert.equal(node.restoreText(expected), raw);
    }
    assert.deepEqual(node.rawToToken, web.rawToToken);
  }
});

test('native digest yields to I/O and cancellation leaves no mapping behind', async () => {
  const abort = new AbortController();
  const ctx = new RedactionContext({ salt: 'test', digestHex: createNodeDigest(), signal: abort.signal });
  const interrupt = new Promise(resolve => setImmediate(() => { abort.abort(new Error('client gone')); resolve(); }));
  const pending = ctx.tokenFor('a@example.com');
  await assert.rejects(pending);
  await interrupt;
  assert.equal(ctx.rawToToken.size, 0);
  assert.equal(ctx.tokenToRaw.size, 0);
  await assert.rejects(ctx.tokenFor('b@example.com'), /client gone/);
});

test('native digest retains nested JSON, mapping limits and collision rejection', async () => {
  const ctx = new RedactionContext({ salt: 'test', digestHex: createNodeDigest(), maxRedactions: 1 });
  const body = await redactJson({ input: '{"mail":"a@example.com"}' }, ctx, parseFlags('E'));
  assert.equal(ctx.restoreText(body.input), '{"mail":"a@example.com"}');
  await assert.rejects(ctx.tokenFor('b@example.com'), /Redaction limit/);
  const collision = new RedactionContext({ digestHex: async () => '0'.repeat(64) });
  await collision.tokenFor('first');
  await assert.rejects(collision.tokenFor('second'), /collision/);
});

test('gateway forwards exactly the same redacted JSON with either digest implementation', async () => {
  const sent = [];
  for (const digestHex of [undefined, createNodeDigest()]) {
    const response = await handleRequest(new Request('https://gateway.example/E$https://upstream.example/v1/responses', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ input: 'a@example.com b@example.com a@example.com' })
    }), {}, { salt: 'same', digestHex, fetchImpl: async (_url, init) => {
      sent.push(init.body);
      return new Response(init.body, { headers: { 'content-type': 'application/json' } });
    } });
    assert.equal(response.status, 200);
    assert.match((await response.json()).input, /a@example.com b@example.com a@example.com$/);
  }
  assert.equal(sent[0], sent[1]);
});

/** 取消发生在第二个宏任务轮次，不能等到整批数千个摘要完成后才响应。 */
test('native digest checks cancellation between bounded groups of new values', async () => {
  const abort = new AbortController();
  const ctx = new RedactionContext({ salt: 'batch', digestHex: createNodeDigest(), signal: abort.signal });
  setImmediate(() => setImmediate(() => abort.abort(new Error('cancel batch'))));
  await assert.rejects((async () => {
    for (let i = 0; i < 4000; i++) await ctx.tokenFor(`person${i}@example.test`);
  })());
  assert(ctx.rawToToken.size > 0);
  assert(ctx.rawToToken.size <= 64, `continued through ${ctx.rawToToken.size} digests`);
  assert.equal(ctx.rawToToken.size, ctx.tokenToRaw.size);
});
