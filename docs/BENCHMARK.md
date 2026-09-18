# 性能基准与优化对比

在项目根目录执行，使用 Node.js 20 或更新版本，无须安装额外依赖。

```powershell
# 优化之前：保存固定样本基线（同名输出会覆盖，重要报告请另行保存）
npm run bench:baseline
# 修改代码后：以相同参数比较，保留 baseline.json
npm run bench:compare
```

输出位于 `.benchmarks/`，包括机器可读 JSON 和可直接阅读的 `.json.md` 表格，该目录不进入 Git。
当前代码重复运行 compare 可以观察本机噪声；不要将单次几毫秒波动解释为优化收益。
新版报告为 `schemaVersion: 2`，旧版基线不能直接比较，需要先运行 `bench:baseline` 重建。脚本会在测量前拒绝不兼容的基线；本次代码更新不会自动覆盖已有报告。

## 摘要实现与第二阶段优化

基准默认 `--digest node`，使用与 Node HTTP 服务相同的原生 SHA-256 实现；`--digest web` 使用 Worker/Deno 默认的 Web Crypto 路径。两者保持相同的盐值、UTF-8 编码和占位符格式。Node 实现首次摘要前及之后每 64 个新摘要让出事件循环，避免密集凭据计算阻塞客户端取消。

```powershell
# 单独验证 Worker/Web Crypto 路径
npm run bench -- --profile quick --digest web --output .benchmarks/web-quick.json
# 与第二阶段之前保留的基线比较，不能重新生成基线覆盖旧实现
npm run bench -- --case dense-4000 --compare .benchmarks/kernel-before-dense-4000.json --output .benchmarks/kernel-current-dense-4000.json
```

允许明确比较两种摘要实现；报告首行同时记录当前和基线实现。旧报告未记录 `digest` 时按 `web` 标注，因为此前基准只使用 Web Crypto。跨实现对比包含摘要实现改变带来的影响，不应解释成仅关键词算法的收益。`source.digestHash` 记录 Node 摘要模块版本；原有输入、规则、环境、轮次和行为摘要检查继续生效。

## 大量样本

默认 full：5 轮，每场景每轮先预热 20 批，再正式执行 100 批；共 12000 次正式请求，另有 2400 次预热请求。单并发场景每轮 100 次，4/8 并发场景每轮分别 400/800 次。每个场景的每一轮都启动新的子进程，并按相同并发数预热。
quick 仅用于验证流程：2 轮，每轮预热 3 批、正式执行 5 批，总计 240 次正式请求，报告明确标注样本不足。完整测试耗时会明显长于旧版。
14 个场景覆盖约 8 KiB 对话、128 KiB/512 KiB/1 MiB 代码上下文、1000/4000 个不同邮箱、4000 个重复邮箱、嵌套工具结果、Anthropic 请求、三种协议的 SSE，以及 4/8 并发。
目标大小指正文规模，JSON 转义及固定块补齐会增加实际请求字节数；报告保存实际字节数。

```powershell
# 快速验证流程，单独保存，避免覆盖正式基线
npm run bench -- --profile quick --output .benchmarks/quick-baseline.json
npm run bench -- --profile quick --compare .benchmarks/quick-baseline.json --output .benchmarks/quick-current.json
# 只研究上次波动明显的场景，默认仍是 5 轮、每轮 100 批
npm run bench -- --case dense-4000 --output .benchmarks/dense-baseline.json
npm run bench -- --case dense-4000 --compare .benchmarks/dense-baseline.json --output .benchmarks/dense-current.json
# 自定义轮次、预热和正式批次，前后参数必须一致
npm run bench -- --rounds 5 --warmup 30 --iterations 200 --timeout 600 --output .benchmarks/large.json
```

固定种子默认为 `20260918`，默认脱敏规则 `HPSIBEG`。可通过 `--seed`、`--flags` 调整。
所有场景包含合成邮箱；自定义规则需要能识别样本中的敏感内容，否则正确性断言会失败。
样本按字节构造，不调用 tokenizer，不能将样本大小等同于真实模型 token 数。
每一轮重复相同的种子序列，不重新随机抽样。轮内样本不同，轮间和前后对比的对应样本完全一致。脚本检查各轮输入、脱敏和恢复摘要，发生非确定性结果时直接报错。
`--timeout` 默认 300 秒，约束单个场景的一轮（含预热），不是整份报告。各轮轮换场景起点，减少固定执行位置的影响。

## 指标及判断方法

- `redactMs`：从构造请求到调用模拟上游，包含请求读取、JSON 处理和脱敏，重点观察其 P50/P95。
- `firstByteMs`：同一起点到收到第一个恢复后的响应块。
- `totalMs`：到完整读取响应，包含脱敏、模拟上游、恢复及接收；最终恢复正确性校验不计入该计时。
- `cpuMsPerRequest`：正式批次的进程 CPU 时间除以请求数，包含模拟上游、摘要和校验开销。短测试下 Windows CPU 计时可能显示 0，需要增加迭代。
- `processPeakRssMiB`：整个场景子进程的内存高水位，包括预热、Node、提前生成的样本、模拟上游及结果接收器，不能直接当作生产网关独占内存。
- JSON 另含 RSS/堆/外部内存采样、事件循环最大延迟及模拟吞吐；5ms 采样可能遗漏同步执行期间的瞬时峰值。

同一场景的并发通过 Promise.all 发起，不代表多核并行。每轮预热完成后主动回收一次垃圾；正式测量期间允许正常 GC，不人为删除慢请求或异常值。
先计算每轮的 P50/P95，再取各轮指标的中位数作为汇总值；CPU 和内存峰值也取各轮中位数，最大耗时则保留所有轮次中的最大值。它不是混合所有请求得到的全局 P95。
中文报告同时列出每轮数据以及 P50/P95、CPU、内存的最小值～最大值。波动幅度 =（最大值－最小值）÷ 中位数。JSON 的 `cases[].rounds[].timings` 保留每个请求的原始耗时、批次号和种子，便于追查慢请求。
对比表负的耗时百分比代表更快。环境、规则、种子、预热批次、正式批次、轮数、场景或输入摘要不匹配会拒绝比较。
判断提示采用完整往返 P95 的轮间观测范围：不足 5 轮或每轮不足 100 次请求时，只验证流程；前后范围重叠时不判定收益；范围不重叠时提示本次各轮均更快或更慢，仍建议复测。观测范围不是置信区间，也没有做统计显著性检验，不能保证未来结果落在范围内。
尽量在同一台机器、同一电源模式且后台负载稳定时测量，不要同时运行两套基准。增加样本和预热不能消除 CPU 频率、温度或后台任务造成的干扰。
上下游内容摘要变化会标记 `behaviorEqual=false` 并以退出码 2 结束，避免脱敏漏检换来虚假的性能提升；这种变化也可能是有意修正规则，需要人工检查后重建基线。

## 验证边界

直接调用真实 `handleRequest`，通过注入的内存模拟上游回显脱敏文本；不访问外网、不消耗模型额度、不读取真实提示词或凭据。
每次断言邮箱已脱敏（启用 E 时）且恢复内容完全一致；SSE 同时切分事件中的占位符和传输块。
模拟上游的 JSON 编解码、摘要和断言会占用时间，指标适合相同测试器下的前后比较，不是纯算法耗时。
它不覆盖 Node HTTP 适配器、真实网络、慢客户端背压、连接断开、生产长时间内存积累或模型响应延迟，不能用模拟 RPS 推断支持多少真实用户。
后续如改动这些行为，应追加对应 HTTP 集成或持续负载测试。功能测试仍通过 `npm test` 独立运行。
