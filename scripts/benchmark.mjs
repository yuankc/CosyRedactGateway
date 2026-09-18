import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createNodeDigest } from '../node-crypto.mjs';
import { fork, execFileSync } from 'node:child_process';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { cpus, totalmem, platform, arch } from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { performance, monitorEventLoopDelay } from 'node:perf_hooks';
import { benchmarkCases, makeSample, FIXTURE_VERSION } from './benchmark-samples.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
const percentile = (values, p) => [...values].sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * p) - 1)];
const round = n => Math.round(n * 100) / 100;
const mib = n => round(n / 1024 / 1024);

/** 严格解析参数，防止拼错选项后无声地产生不可比的测试报告。 */
export function argumentsFor(argv) {
  const options = { profile: 'full', digest: 'node', flags: 'HPSIBEG', seed: 20260918, output: '.benchmarks/latest.json', timeout: 300 };
  const allowed = new Set(['profile', 'digest', 'flags', 'seed', 'output', 'compare', 'iterations', 'case', 'timeout', 'rounds', 'warmup']);
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i].replace(/^--/, '');
    if (!argv[i].startsWith('--') || !allowed.has(key) || !argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error(`Invalid option: ${argv[i]}`);
    options[key] = argv[++i];
  }
  assert(['quick', 'full'].includes(options.profile), 'profile must be quick or full');
  assert(['node', 'web'].includes(options.digest), 'digest must be node or web');
  for (const key of ['seed', 'iterations', 'timeout', 'rounds', 'warmup']) {
    if (options[key] !== undefined) {
      options[key] = Number(options[key]);
      assert(Number.isSafeInteger(options[key]) && options[key] > 0, `${key} must be a positive integer`);
    }
  }
  options.iterations ??= options.profile === 'full' ? 100 : 5;
  options.rounds ??= options.profile === 'full' ? 5 : 2;
  options.warmup ??= options.profile === 'full' ? 20 : 3;
  return options;
}

/** 用真实网关入口和无网络的模拟上游测量一次完整脱敏／恢复往返。 */
async function exercise(sample, spec, flags, api, digest) {
  let redactMs;
  let upstreamHash;
  let tokenCount;
  let firstByteMs;
  let restored = '';
  const started = performance.now();
  const response = await api.handleRequest(new Request(`https://gateway.example/${flags}$https://upstream.example/v1/${sample.endpoint}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: sample.body,
  }), {}, {
    salt: 'synthetic-benchmark-fixed-salt-v1',
    digestHex: digest === 'node' ? createNodeDigest() : undefined,
    fetchImpl: async (_url, init) => {
      redactMs = performance.now() - started;
      const body = JSON.parse(init.body);
      let text = sample.path.reduce((value, key) => value[key], body);
      if (text.startsWith(api.REDACT_NOTICE + '\n\n')) text = text.slice(api.REDACT_NOTICE.length + 2);
      const tokens = text.match(/\{\{Redact:[a-f0-9]{64}\}\}/g) || [];
      tokenCount = new Set(tokens).size;
      assert(tokenCount > 0, 'Fixture was not redacted');
      if (flags.toUpperCase().includes('E')) {
        // One pass over emails, rather than a full-text scan per expected secret.
        assert(!/person\d*_\d+@example\.test/.test(text), 'Synthetic email leaked upstream');
      }
      upstreamHash = hash(init.body);
      if (!spec.stream) return new Response(JSON.stringify({ output_text: text }), { headers: { 'content-type': 'application/json' } });
      // An in-memory mock: no model latency, network, or socket backpressure is simulated.
      let offset = 0;
      const encoder = new TextEncoder();
      const stream = new ReadableStream({
        pull(controller) {
          if (offset >= text.length) { controller.close(); return; }
          const delta = text.slice(offset, offset + 37); offset += 37;
          const data = spec.protocol === 'chat'
            ? { choices: [{ index: 0, delta: { content: delta } }] }
            : spec.protocol === 'anthropic'
              ? { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: delta } }
              : { type: 'response.output_text.delta', output_index: 0, content_index: 0, delta };
          const bytes = encoder.encode(`data: ${JSON.stringify(data)}\n\n`);
          // Split HTTP chunks independently of SSE fields and placeholder boundaries.
          controller.enqueue(bytes.slice(0, 19)); controller.enqueue(bytes.slice(19));
        },
      });
      return new Response(stream, { headers: { 'content-type': 'text/event-stream' } });
    },
  });
  assert.equal(response.status, 200, `Gateway returned ${response.status}`);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let wire = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    firstByteMs ??= performance.now() - started;
    wire += decoder.decode(value, { stream: true });
  }
  wire += decoder.decode();
  const totalMs = performance.now() - started;
  if (spec.stream) {
    for (const event of wire.split('\n\n')) {
      if (!event.trim()) continue;
      const line = event.split('\n').find(value => value.startsWith('data: '));
      const data = JSON.parse(line.slice(6));
      restored += spec.protocol === 'chat' ? data.choices[0].delta.content
        : spec.protocol === 'anthropic' ? data.delta.text : data.delta;
    }
  } else restored = JSON.parse(wire).output_text;
  assert.equal(restored, sample.text, 'Restored content changed');
  return { redactMs, firstByteMs, totalMs, upstreamHash, restoredHash: hash(restored), tokenCount };
}

/** 测量一个场景的一轮；按正式并发预热，保存逐次耗时及内存采样。 */
async function runCase(spec, options) {
  const api = await import('../worker.js');
  api.parseFlags(options.flags);
  const samples = Array.from({ length: options.iterations * spec.concurrency }, (_, i) => makeSample(spec, options.seed + i));
  for (let i = 0; i < options.warmup; i++) {
    await Promise.all(Array.from({ length: spec.concurrency }, (_, j) =>
      exercise(samples[(i * spec.concurrency + j) % samples.length], spec, options.flags, api, options.digest)));
  }
  global.gc?.();
  const startMemory = process.memoryUsage();
  const peak = { ...startMemory };
  const sampleMemory = () => {
    const current = process.memoryUsage();
    for (const key of Object.keys(peak)) peak[key] = Math.max(peak[key], current[key]);
  };
  const timer = setInterval(sampleMemory, 5);
  const delay = monitorEventLoopDelay({ resolution: 10 });
  delay.enable();
  await new Promise(resolve => setTimeout(resolve, 20));
  const cpuStart = process.cpuUsage();
  const start = performance.now();
  const results = [];
  try {
    for (let i = 0; i < samples.length; i += spec.concurrency) {
      const batch = await Promise.all(samples.slice(i, i + spec.concurrency).map(sample => exercise(sample, spec, options.flags, api, options.digest)));
      results.push(...batch); sampleMemory();
    }
    const wallMs = performance.now() - start;
    const cpu = process.cpuUsage(cpuStart);
    await new Promise(resolve => setTimeout(resolve, 20));
    sampleMemory();
    const metrics = {};
    for (const key of ['redactMs', 'firstByteMs', 'totalMs']) {
      const values = results.map(r => r[key]);
      metrics[key] = { p50: round(percentile(values, .5)), p95: round(percentile(values, .95)), max: round(Math.max(...values)) };
    }
    return {
      name: spec.name, spec, requests: samples.length,
      inputBytes: { min: Math.min(...samples.map(s => Buffer.byteLength(s.body))), max: Math.max(...samples.map(s => Buffer.byteLength(s.body))) },
      corpusHash: hash(samples.map(s => hash(s.body)).join('')),
      upstreamHash: hash(results.map(r => r.upstreamHash).join('')),
      restoredHash: hash(results.map(r => r.restoredHash).join('')),
      uniqueRedactions: { min: Math.min(...results.map(r => r.tokenCount)), max: Math.max(...results.map(r => r.tokenCount)) },
      metrics, wallMs: round(wallMs), throughputRps: round(samples.length * 1000 / wallMs),
      timings: results.map((result, i) => ({
        request: i + 1, batch: Math.floor(i / spec.concurrency) + 1, seed: options.seed + i,
        redactMs: result.redactMs, firstByteMs: result.firstByteMs, totalMs: result.totalMs,
      })),
      cpuMsPerRequest: round((cpu.user + cpu.system) / 1000 / samples.length),
      memory: { startRssMiB: mib(startMemory.rss), sampledPeakRssMiB: mib(peak.rss), processPeakRssMiB: round(process.resourceUsage().maxRSS / 1024), sampledPeakHeapMiB: mib(peak.heapUsed), sampledPeakExternalMiB: mib(peak.external) },
      eventLoopDelayMaxMs: round(delay.max / 1e6),
    };
  } finally { clearInterval(timer); delay.disable(); }
}

/** 汇总独立轮次；展示各轮指标的中位数和观测范围，不混合成一个请求总体。 */
export function aggregateRounds(rounds) {
  assert(rounds.length > 0, '缺少测试轮次');
  const first = rounds[0];
  for (const row of rounds) {
    assert.equal(row.name, first.name, '轮次场景不一致');
    assert.deepEqual(row.spec, first.spec, '轮次配置不一致');
    for (const key of ['requests', 'corpusHash', 'upstreamHash', 'restoredHash']) {
      assert.equal(row[key], first[key], `同一场景各轮的输入或行为不一致：${key}`);
    }
  }
  const range = values => {
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    const median = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    const min = sorted[0], max = sorted.at(-1);
    return { median: round(median), min, max, spreadPercent: median === 0 ? null : round((max - min) / median * 100) };
  };
  const metrics = {};
  for (const key of ['redactMs', 'firstByteMs', 'totalMs']) {
    metrics[key] = {
      p50: range(rounds.map(r => r.metrics[key].p50)).median,
      p95: range(rounds.map(r => r.metrics[key].p95)).median,
      max: Math.max(...rounds.map(r => r.metrics[key].max)),
    };
  }
  const variability = {
    totalP50: range(rounds.map(r => r.metrics.totalMs.p50)),
    totalP95: range(rounds.map(r => r.metrics.totalMs.p95)),
    redactP95: range(rounds.map(r => r.metrics.redactMs.p95)),
    cpu: range(rounds.map(r => r.cpuMsPerRequest)),
    peakRss: range(rounds.map(r => r.memory.processPeakRssMiB)),
  };
  return {
    name: first.name, spec: first.spec, requestsPerRound: first.requests, requests: first.requests * rounds.length,
    inputBytes: first.inputBytes, corpusHash: first.corpusHash, upstreamHash: first.upstreamHash, restoredHash: first.restoredHash,
    metrics, cpuMsPerRequest: variability.cpu.median, memory: { processPeakRssMiB: variability.peakRss.median },
    variability, rounds,
  };
}

/** 在开始耗时测量前拒绝旧版报告和不一致的环境、参数、场景。 */
export function validateComparison(before, after) {
  assert.equal(before.schemaVersion, 2, '旧版测量方法不可比较，请重新建立基线');
  assert.equal(after.schemaVersion, 2, '当前报告不是新版测量格式');
  assert.equal(after.fixtureVersion, before.fixtureVersion, '样本生成版本不同');
  for (const key of ['node', 'platform', 'arch', 'cpu', 'logicalCpus', 'totalMemoryBytes']) assert.equal(after.environment[key], before.environment[key], `Different environment: ${key}`);
  for (const key of ['seed', 'flags', 'iterations', 'rounds', 'warmup']) assert.equal(after.options[key], before.options[key], `Different option: ${key}`);
  assert.deepEqual(after.cases.map(c => c.spec), before.cases.map(c => c.spec), 'Different case selection');
}

/** 比较同环境、同参数及同样本报告；输出差异同时检查行为一致性。 */
export function compareReports(before, after) {
  validateComparison(before, after);
  return after.cases.map((row, i) => {
    const old = before.cases[i];
    assert.deepEqual(row.spec, old.spec, `Changed scenario: ${row.name}`);
    assert.equal(row.corpusHash, old.corpusHash, `Changed corpus: ${row.name}`);
    assert.equal(row.requests, old.requests, `Changed request count: ${row.name}`);
    assert.equal(row.rounds.length, after.options.rounds, '当前报告轮次不完整');
    assert.equal(old.rounds.length, before.options.rounds, '基线报告轮次不完整');
    const change = (a, b) => a === 0 ? null : round((b / a - 1) * 100);
    const behaviorEqual = row.upstreamHash === old.upstreamHash && row.restoredHash === old.restoredHash;
    const previous = old.variability.totalP95, current = row.variability.totalP95;
    const rangesOverlap = current.min <= previous.max && previous.min <= current.max;
    const sufficient = row.rounds.length >= 5 && row.requestsPerRound >= 100 && old.requestsPerRound >= 100;
    const assessment = !behaviorEqual ? '结果不一致，先检查行为'
      : !sufficient ? '轮次或样本不足，仅验证流程'
      : rangesOverlap ? '各轮范围重叠，暂不判定'
      : current.min > previous.max ? '本次各轮均更慢，建议复测' : '本次各轮均更快，建议复测';
    return {
      name: row.name,
      behaviorEqual, rangesOverlap, assessment,
      p50ChangePercent: change(old.metrics.totalMs.p50, row.metrics.totalMs.p50),
      redactP95ChangePercent: change(old.metrics.redactMs.p95, row.metrics.redactMs.p95),
      p95ChangePercent: change(old.metrics.totalMs.p95, row.metrics.totalMs.p95),
      cpuChangePercent: change(old.cpuMsPerRequest, row.cpuMsPerRequest),
      peakRssChangeMiB: round(row.memory.processPeakRssMiB - old.memory.processPeakRssMiB),
    };
  });
}

/** 将已保存的测试数据生成为中文报告，不重新运行测试或改变测量值。 */
export function renderReport(report) {
  const names = {
    'chat-8k': '短对话（约 8 KiB）',
    'code-128k': '代码上下文（约 128 KiB）',
    'code-512k': '代码上下文（约 512 KiB）',
    'code-1m': '代码上下文（约 1 MiB）',
    'dense-1000': '密集敏感值（1000 个不同邮箱）',
    'dense-4000': '密集敏感值（4000 个不同邮箱）',
    'repeated-4000': '重复敏感值（同一邮箱出现 4000 次）',
    'nested-json-128k': '嵌套工具结果（约 128 KiB）',
    'anthropic-128k': 'Anthropic 请求（约 128 KiB）',
    'sse-chat-32k': 'Chat 流式响应（约 32 KiB）',
    'sse-responses-32k': 'Responses 流式响应（约 32 KiB）',
    'sse-anthropic-32k': 'Anthropic 流式响应（约 32 KiB）',
    'concurrent-4-code-128k': '4 个并发请求（每个约 128 KiB）',
    'concurrent-8-code-128k': '8 个并发请求（每个约 128 KiB）',
  };
  const percent = value => value == null ? '无法计算' : `${value}%`;
  const lines = ['# Cosy 网关性能测试报告', '', `生成时间（UTC）：${report.createdAt}`, '',
    `摘要实现：${report.options.digest ?? "web"}${report.baselineDigest ? `；基线摘要实现：${report.baselineDigest}` : ""}。运行环境：Node ${report.environment.node}；处理器：${report.environment.cpu}；脱敏规则：${report.options.flags}；样本种子：${report.options.seed}`, '',
    `测量方式：${report.options.rounds} 轮独立进程；每轮预热 ${report.options.warmup} 批；正式测量 ${report.options.iterations} 批。每批请求数等于该场景并发数。`, '',
    '**本报告使用合成样本和进程内模拟上游，不能直接用于判断线上用户容量或真实模型延迟。**', '',
    '## 指标说明', '',
    '- P95：约 95% 的请求耗时不超过此值，越低越好。少量样本下该指标容易波动。',
    '- 脱敏耗时：从构造请求到调用上游，包括读取请求、解析 JSON 和脱敏。',
    '- 首块耗时：从构造请求到收到第一个恢复后的响应块，不是真实模型的首字延迟。',
    '- 完整往返：从构造请求到完整读取响应，包括模拟上游和内容恢复。',
    '- CPU 时间：每次请求平均消耗的处理器时间，不等同于用户等待时间。',
    '- 内存峰值：整个测试子进程的 RSS 高水位，包括运行环境、样本和模拟上游；不是网关单独占用的内存。',
    '- ms 是毫秒；KiB 是 1024 字节，MiB 是 1024 KiB。', '',
    '汇总表中的 P50、P95、CPU 和内存为各轮对应指标的中位数，并非将所有请求混在一起计算。完整逐次耗时保存在 JSON 的 cases[].rounds[].timings 中。', '',
    '## 测试结果', '',
    '| 测试场景 | 总请求数 | 最大请求大小（KiB） | 脱敏 P95（ms） | 首块 P95（ms） | 完整往返 P50（ms） | 完整往返 P95（ms） | 每次请求 CPU（ms） | 进程内存峰值（MiB） |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'];
  for (const c of report.cases) lines.push(`| ${names[c.name] || c.name} | ${c.requests} | ${round(c.inputBytes.max / 1024)} | ${c.metrics.redactMs.p95} | ${c.metrics.firstByteMs.p95} | ${c.metrics.totalMs.p50} | ${c.metrics.totalMs.p95} | ${c.cpuMsPerRequest} | ${c.memory.processPeakRssMiB} |`);
  if (report.options.rounds < 5 || report.cases.some(c => c.requestsPerRound < 100)) {
    lines.push('', '**当前轮次或样本不足，仅验证测试流程，不用于判断优化收益。**');
  }
  lines.push('', '## 轮间波动', '',
    '范围表示本次各轮指标的最小值～最大值，不是统计置信区间。波动幅度 =（最大值－最小值）÷ 中位数；波动越大，单次对比越不可靠。', '',
    '| 测试场景 | 完整往返 P50 范围（ms） | 完整往返 P95 范围（ms） | P95 波动幅度 | 脱敏 P95 范围（ms） | CPU 范围（ms/请求） | 内存峰值范围（MiB） |',
    '| --- | --- | --- | ---: | --- | --- | --- |');
  for (const c of report.cases) {
    const range = key => `${c.variability[key].min}～${c.variability[key].max}`;
    lines.push(`| ${names[c.name] || c.name} | ${range('totalP50')} | ${range('totalP95')} | ${percent(c.variability.totalP95.spreadPercent)} | ${range('redactP95')} | ${range('cpu')} | ${range('peakRss')} |`);
  }
  lines.push('', '## 各轮明细', '',
    '| 测试场景 | 轮次 | 请求数 | 完整往返 P50（ms） | 完整往返 P95（ms） | 每次请求 CPU（ms） | 内存峰值（MiB） |',
    '| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const c of report.cases) {
    for (const [i, row] of c.rounds.entries()) lines.push(`| ${names[c.name] || c.name} | ${i + 1} | ${row.requests} | ${row.metrics.totalMs.p50} | ${row.metrics.totalMs.p95} | ${row.cpuMsPerRequest} | ${row.memory.processPeakRssMiB} |`);
  }
  if (report.comparison) {
    lines.push('', '## 与基线对比', '',
      '耗时变化为负数表示更快，为正数表示更慢；内存变化为负数表示减少。比如 -20% 表示耗时降低 20%。小幅差异可能来自测量波动，同一版代码也会产生差异。', '',
      '“结果一致”表示脱敏后内容与恢复后内容的摘要都与基线一致；不一致时需要先检查行为变化，再判断性能收益。', '',
      '判断提示基于完整往返 P95 的各轮观测范围：重叠时不判定收益，不重叠也只提示复测，不等于统计显著或已确认回归。', '',
      '| 测试场景 | 结果是否一致 | 脱敏 P95 变化 | 完整往返 P50 变化 | 完整往返 P95 变化 | CPU 时间变化 | 内存峰值变化（MiB） | 判断提示 |',
      '| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |');
    for (const c of report.comparison) lines.push(`| ${names[c.name] || c.name} | ${c.behaviorEqual ? '一致' : '不一致，需检查'} | ${percent(c.redactP95ChangePercent)} | ${percent(c.p50ChangePercent)} | ${percent(c.p95ChangePercent)} | ${percent(c.cpuChangePercent)} | ${c.peakRssChangeMiB} | ${c.assessment} |`);
  }
  lines.push('', '## 测量范围与限制', '',
    '内存包含 Node 运行环境、预先生成的样本、模拟上游和响应接收器，进程内存高水位也包含预热阶段。每 5ms 采样可能遗漏同步执行期间的瞬时峰值。', '',
    '完整往返计时包含模拟上游的序列化、上游内容校验和摘要计算，不包含最终的恢复结果校验；CPU 时间包含正式批次的这些校验开销。', '',
    '测试不访问真实网络、不计算模型 token、不模拟真实模型等待时间，也不测量 Node HTTP 适配器。要评估线上容量，还需要实际部署环境下的持续负载测试。', '');
  return lines.join('\n');
}

/** 父进程负责隔离场景、超时终止、保存基线和生成 Markdown 对比。 */
async function main() {
  const options = argumentsFor(process.argv.slice(2));
  const specs = benchmarkCases().filter(spec => !options.case || spec.name === options.case);
  assert(specs.length, `Unknown case: ${options.case}`);
  const output = resolve(root, options.output);
  if (options.compare) assert.notEqual(output, resolve(root, options.compare), 'Do not overwrite the comparison baseline');
  const baseline = options.compare ? JSON.parse(await readFile(resolve(root, options.compare), 'utf8')) : null;
  let commit = 'unknown';
  try { commit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); } catch {}
  const report = {
    schemaVersion: 2, fixtureVersion: FIXTURE_VERSION, createdAt: new Date().toISOString(), options,
    environment: { node: process.version, platform: platform(), arch: arch(), cpu: cpus()[0]?.model, logicalCpus: cpus().length, totalMemoryBytes: totalmem() },
    source: { commit, workerHash: hash(await readFile(resolve(root, 'worker.js'))), adapterHash: hash(await readFile(resolve(root, 'node-server.mjs'))), digestHash: hash(await readFile(resolve(root, 'node-crypto.mjs'))) },
    baselineDigest: baseline ? (baseline.options.digest ?? "web") : undefined,
    cases: specs.map(spec => ({ spec })),
  };
  if (baseline) validateComparison(baseline, report);
  const measurements = new Map(specs.map(spec => [spec.name, []]));
  for (let iteration = 0; iteration < options.rounds; iteration++) {
    // 各轮轮换场景起点，避免同一场景始终固定在整轮开头或末尾。
    const offset = iteration % specs.length;
    const ordered = [...specs.slice(offset), ...specs.slice(0, offset)];
    for (const spec of ordered) {
      console.log(`第 ${iteration + 1}/${options.rounds} 轮 ${spec.name}：预热 ${options.warmup} 批，正式 ${options.iterations} 批，并发 ${spec.concurrency}`);
      const result = await new Promise((resolveResult, reject) => {
        const child = fork(fileURLToPath(import.meta.url), ['--child'], { cwd: root, execArgv: ['--expose-gc'], stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
        let message;
        const timer = setTimeout(() => { child.kill(); reject(new Error(`Timeout after ${options.timeout}s: ${spec.name}`)); }, options.timeout * 1000);
        child.on('message', value => { message = value; });
        child.on('error', error => { clearTimeout(timer); reject(error); });
        child.on('exit', code => {
          clearTimeout(timer);
          if (code !== 0 || !message?.result) reject(new Error(message?.error || `Case failed: ${spec.name} (${code})`));
          else resolveResult(message.result);
        });
        child.send({ spec, options });
      });
      measurements.get(spec.name).push(result);
      console.log(`  完整往返 P95=${result.metrics.totalMs.p95}ms；CPU/请求=${result.cpuMsPerRequest}ms；内存峰值=${result.memory.processPeakRssMiB}MiB`);
    }
  }
  report.cases = specs.map(spec => aggregateRounds(measurements.get(spec.name)));
  if (baseline) report.comparison = compareReports(baseline, report);
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(report, null, 2) + '\n');
  await writeFile(output + '.md', renderReport(report));
  console.log(`Report: ${output}\nMarkdown: ${output}.md`);
  if (report.comparison?.some(c => !c.behaviorEqual)) {
    console.error('Behavior changed; performance improvement is not equivalent.'); process.exitCode = 2;
  }
}

if (process.argv[2] === '--child') {
  process.once('message', async ({ spec, options }) => {
    try { process.send({ result: await runCase(spec, options) }); }
    catch (error) { process.send({ error: error.stack }); process.exitCode = 1; }
    finally { process.disconnect(); }
  });
} else if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error.stack); process.exitCode = 1; });
}
