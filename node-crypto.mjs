import { createHash } from "node:crypto";
import { setImmediate } from "node:timers/promises";

/** 为单请求创建原生摘要函数；最多连续计算 64 个新摘要后让出事件循环，不共享敏感数据。 */
export function createNodeDigest() {
  let count = 0;
  return async function nodeSha256Hex(value, signal) {
    if (count++ % 64 === 0) await setImmediate(undefined, { signal });
    signal?.throwIfAborted();
    return createHash("sha256").update(value, "utf8").digest("hex");
  };
}
