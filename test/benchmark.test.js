import test from 'node:test';
import assert from 'node:assert/strict';
import { benchmarkCases, makeSample } from '../scripts/benchmark-samples.mjs';
import { aggregateRounds, argumentsFor, compareReports, renderReport } from '../scripts/benchmark.mjs';

test('synthetic corpus is reproducible and contains distinct and repeated secret workloads', () => {
  for (const spec of benchmarkCases()) {
    const first = makeSample(spec, 20260918);
    assert.deepEqual(first, makeSample(spec, 20260918));
    assert.notEqual(first.body, makeSample(spec, 20260919).body);
    assert.equal(first.path.reduce((value, key) => value[key], JSON.parse(first.body)), first.text);
    if (spec.bytes) assert(Buffer.byteLength(first.text) >= spec.bytes);
    if (spec.kind === 'dense') assert.equal(first.secrets.length, spec.items);
    if (spec.kind === 'repeated') assert.equal(first.secrets.length, 1);
  }
});

/** 构造最小报告，验证比较器拒绝不可比数据并识别行为回归。 */
function report() {
  const rounds = Array.from({ length: 5 }, (_, i) => ({
    name: 'test', spec: { name: 'test', concurrency: 1 }, requests: 100,
    corpusHash: 'input', upstreamHash: 'redacted', restoredHash: 'restored',
    inputBytes: { min: 1024, max: 1024 },
    metrics: { redactMs: { p50: 25, p95: 50, max: 60 }, firstByteMs: { p50: 30, p95: 60, max: 70 }, totalMs: { p50: 50, p95: 98 + i, max: 150 } },
    cpuMsPerRequest: 50, memory: { processPeakRssMiB: 100 },
  }));
  return {
    schemaVersion: 2, fixtureVersion: 1,
    environment: { node: 'v22', platform: 'win32', arch: 'x64', cpu: 'test', logicalCpus: 4, totalMemoryBytes: 1024 },
    options: { seed: 1, flags: 'E', iterations: 100, rounds: 5, warmup: 20 },
    cases: [aggregateRounds(rounds)],
  };
}

test('comparison calculates changes and does not hide changed behavior behind faster timings', () => {
  const before = report();
  const after = report();
  after.cases[0].metrics.totalMs.p95 = 80;
  after.cases[0].variability.totalP95 = { median: 80, min: 78, max: 82, spreadPercent: 5 };
  after.cases[0].cpuMsPerRequest = 40;
  after.cases[0].memory.processPeakRssMiB = 90;
  assert.deepEqual(compareReports(before, after)[0], { name: 'test', behaviorEqual: true, rangesOverlap: false, assessment: '本次各轮均更快，建议复测', p50ChangePercent: 0, redactP95ChangePercent: 0, p95ChangePercent: -20, cpuChangePercent: -20, peakRssChangeMiB: -10 });
  after.cases[0].upstreamHash = 'different-redaction';
  assert.equal(compareReports(before, after)[0].behaviorEqual, false);
  assert.equal(compareReports(before, after)[0].assessment, '结果不一致，先检查行为');
  after.cases[0].upstreamHash = 'redacted';
  after.cases[0].restoredHash = 'wrong-restoration';
  assert.equal(compareReports(before, after)[0].behaviorEqual, false);
});

test('comparison rejects changes to environment, workload and sample content', () => {
  const mutations = [
    r => { r.schemaVersion = 1; },
    r => { r.environment.node = 'v24'; },
    r => { r.options.seed = 2; },
    r => { r.options.flags = 'HE'; },
    r => { r.options.iterations = 10; },
    r => { r.options.rounds = 3; },
    r => { r.options.warmup = 1; },
    r => { r.cases[0].spec.concurrency = 4; },
    r => { r.cases[0].corpusHash = 'other-input'; },
    r => { r.cases[0].requests = 10; },
    r => { r.cases[0].rounds.pop(); },
  ];
  for (const mutate of mutations) {
    const after = report(); mutate(after);
    assert.throws(() => compareReports(report(), after));
  }
});

test('round summaries use medians, retain outliers and reject changing inputs or behavior', () => {
  const rounds = report().cases[0].rounds.slice(0, 3);
  rounds[0].metrics.totalMs.p95 = 10;
  rounds[1].metrics.totalMs.p95 = 11;
  rounds[2].metrics.totalMs.p95 = 100;
  const summary = aggregateRounds(rounds);
  assert.equal(summary.requests, 300);
  assert.equal(summary.requestsPerRound, 100);
  assert.equal(summary.metrics.totalMs.p95, 11);
  assert.equal(summary.variability.totalP95.min, 10);
  assert.equal(summary.variability.totalP95.max, 100);
  assert.equal(summary.rounds[2].metrics.totalMs.p95, 100);
  assert.equal(aggregateRounds(rounds.slice(0, 2)).metrics.totalMs.p95, 10.5);
  for (const key of ['corpusHash', 'upstreamHash', 'restoredHash']) {
    const changed = structuredClone(rounds);
    changed[1][key] = 'different';
    assert.throws(() => aggregateRounds(changed));
  }
});

test('comparison reports observed noise and never treats a quick run as evidence of improvement', () => {
  const before = report(), after = report();
  assert.equal(compareReports(before, after)[0].assessment, '各轮范围重叠，暂不判定');
  after.cases[0].variability.totalP95.min = 103;
  after.cases[0].variability.totalP95.max = 110;
  assert.equal(compareReports(before, after)[0].assessment, '本次各轮均更慢，建议复测');
  before.options.rounds = after.options.rounds = 2;
  before.cases[0] = aggregateRounds(before.cases[0].rounds.slice(0, 2));
  after.cases[0] = aggregateRounds(after.cases[0].rounds.slice(0, 2));
  assert.equal(compareReports(before, after)[0].assessment, '轮次或样本不足，仅验证流程');
});

test('default measurement uses five rounds, sufficient warmup and 100 batches; quick is explicit', () => {
  const defaults = argumentsFor([]);
  assert.equal(defaults.rounds, 5);
  assert.equal(defaults.warmup, 20);
  assert.equal(defaults.iterations, 100);
  const quick = argumentsFor(['--profile', 'quick']);
  assert.equal(quick.rounds, 2);
  assert.equal(quick.iterations, 5);
  const custom = argumentsFor(['--rounds', '3', '--warmup', '10', '--iterations', '50']);
  assert.equal(custom.rounds, 3);
  assert.equal(custom.warmup, 10);
  assert.equal(custom.iterations, 50);
  for (const args of [['--rounds', '0'], ['--warmup', '-1'], ['--iterations', '1.5']]) assert.throws(() => argumentsFor(args));
});

test('Chinese report explains round statistics, includes detail and labels insufficient samples', () => {
  const data = report();
  data.comparison = compareReports(data, data);
  const markdown = renderReport(data);
  assert.match(markdown, /轮间波动/);
  assert.match(markdown, /不是统计置信区间/);
  assert.match(markdown, /各轮范围重叠，暂不判定/);
  assert.match(markdown, /98～102/);
  data.options.rounds = 2;
  assert.match(renderReport(data), /当前轮次或样本不足/);
});

test('digest implementation is explicit in arguments and comparison reports', () => {
  assert.equal(argumentsFor([]).digest, 'node');
  assert.equal(argumentsFor(['--digest', 'web']).digest, 'web');
  assert.throws(() => argumentsFor(['--digest', 'unknown']));
  const data = report();
  data.options.digest = 'node';
  data.baselineDigest = 'web';
  assert.match(renderReport(data), /摘要实现：node；基线摘要实现：web/);
});
