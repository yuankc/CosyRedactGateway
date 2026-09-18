// Synthetic fixtures only. Never load production prompts or credentials here.
export const FIXTURE_VERSION = 1;

/** 用固定种子生成可复现的合成凭据，不使用真实密钥。 */
function randomText(seed, length) {
  let state = seed >>> 0 || 1;
  const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let value = '';
  for (let i = 0; i < length; i++) {
    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
    value += alphabet[(state >>> 0) % alphabet.length];
  }
  return value;
}

/** 生成固定字节规模文本，记录必须被识别的合成邮箱用于正确性检查。 */
export function makeSample(spec, seed) {
  const secrets = [];
  let text = '';
  if (spec.kind === 'dense' || spec.kind === 'repeated') {
    for (let i = 0; i < spec.items; i++) {
      const email = `person${spec.kind === 'repeated' ? 0 : i}_${seed}@example.test`;
      secrets.push(email);
      text += `contact=${email}\n`;
    }
  } else {
    const email = `person_${seed}@example.test`;
    secrets.push(email);
    const ordinary = spec.kind === 'chat'
      ? '请分析这段业务逻辑，保留已有字段并说明异常处理。This is an ordinary project discussion.\n'
      : 'function readRecord(record) { return { name: record.name, status: "ready", count: 42 }; } // ordinary source\n';
    const marker = `contact=${email}; token="${randomText(seed, 32)}";\n`;
    const block = ordinary.repeat(32) + marker;
    const repeats = Math.max(1, Math.ceil(spec.bytes / Buffer.byteLength(block)));
    text = block.repeat(repeats);
  }
  let body;
  let path;
  let endpoint;
  if (spec.protocol === 'chat') {
    body = { model: 'synthetic-model', messages: [{ role: 'user', content: text }], stream: !!spec.stream };
    path = ['messages', 0, 'content']; endpoint = 'chat/completions';
  } else if (spec.protocol === 'anthropic') {
    body = { model: 'synthetic-model', max_tokens: 1024, messages: [{ role: 'user', content: text }], stream: !!spec.stream };
    path = ['messages', 0, 'content']; endpoint = 'messages';
  } else if (spec.kind === 'nested') {
    text = JSON.stringify({ source: text, instruction: 'Review this synthetic tool result' });
    body = { model: 'synthetic-model', input: [{ type: 'function_call_output', call_id: 'synthetic-call', output: text }] };
    path = ['input', 0, 'output']; endpoint = 'responses';
  } else {
    body = { model: 'synthetic-model', input: text, stream: !!spec.stream };
    path = ['input']; endpoint = 'responses';
  }
  return { body: JSON.stringify(body), text, secrets: [...new Set(secrets)], path, endpoint };
}

/** 覆盖正文规模、敏感值密度、协议、流式恢复和低到中等并发。 */
export function benchmarkCases() {
  const cases = [
    { name: 'chat-8k', kind: 'chat', bytes: 8 * 1024, protocol: 'chat' },
    { name: 'code-128k', kind: 'code', bytes: 128 * 1024 },
    { name: 'code-512k', kind: 'code', bytes: 512 * 1024 },
    { name: 'code-1m', kind: 'code', bytes: 1024 * 1024 },
    { name: 'dense-1000', kind: 'dense', items: 1000 },
    { name: 'dense-4000', kind: 'dense', items: 4000 },
    { name: 'repeated-4000', kind: 'repeated', items: 4000 },
    { name: 'nested-json-128k', kind: 'nested', bytes: 128 * 1024 },
    { name: 'anthropic-128k', kind: 'code', bytes: 128 * 1024, protocol: 'anthropic' },
    { name: 'sse-chat-32k', kind: 'code', bytes: 32 * 1024, protocol: 'chat', stream: true },
    { name: 'sse-responses-32k', kind: 'code', bytes: 32 * 1024, stream: true },
    { name: 'sse-anthropic-32k', kind: 'code', bytes: 32 * 1024, protocol: 'anthropic', stream: true },
    { name: 'concurrent-4-code-128k', kind: 'code', bytes: 128 * 1024, concurrency: 4 },
    { name: 'concurrent-8-code-128k', kind: 'code', bytes: 128 * 1024, concurrency: 8 },
  ];
  return cases.map(spec => ({ protocol: 'responses', concurrency: 1, ...spec }));
}
