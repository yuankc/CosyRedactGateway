import http from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { handleRequest } from "./worker.js";
import { createNodeDigest } from "./node-crypto.mjs";

const port = Number(process.env.PORT || 8787);
const host = process.env.HOST || "127.0.0.1";

/** 按 Web Stream 的读取需求接收请求；取消只暂停上传，保留发送 413 的连接。 */
function requestBody(req) {
  let downstream;
  const cleanup = () => {
    req.off("data", data); req.off("end", end); req.off("error", error);
  };
  const data = chunk => { req.pause(); downstream.enqueue(chunk); };
  const end = () => { cleanup(); downstream.close(); };
  const error = reason => { cleanup(); downstream.error(reason); };
  return new ReadableStream({
    start(controller) {
      downstream = controller;
      req.pause();
      req.on("data", data); req.once("end", end); req.once("error", error);
    },
    pull() { req.resume(); },
    cancel() { cleanup(); req.pause(); }
  }, { highWaterMark:0 });
}

/** 将客户端断开传入网关，并通过 pipeline 传播背压和响应流错误。 */
const server = http.createServer(async (req, res) => {
  const controller = new AbortController();
  const abort = () => controller.abort(new DOMException("Client disconnected", "AbortError"));
  const closed = () => { if (!res.writableFinished) abort(); };
  req.once("aborted", abort); req.on("error", abort); res.once("close", closed);
  let body;
  try {
    const origin = `http://${req.headers.host || `${host}:${port}`}`;
    body = req.method === "GET" || req.method === "HEAD" ? undefined : requestBody(req);
    const request = new Request(new URL(req.url, origin), { method:req.method, headers:req.headers, body, duplex:body ? "half" : undefined, signal:controller.signal });
    const response = await handleRequest(request, process.env, { digestHex:createNodeDigest() });
    if (res.destroyed) { await response.body?.cancel(); return; }
    const headers = Object.fromEntries(response.headers);
    // 未读完的上传不复用连接；先发错误响应，再由 HTTP 层关闭连接。
    if (!req.complete) headers.connection = "close";
    res.writeHead(response.status, headers);
    if (!response.body) return res.end();
    await pipeline(Readable.fromWeb(response.body), res, { signal:controller.signal });
  } catch (error) {
    controller.abort(error);
    if (res.destroyed) return;
    if (res.headersSent) res.destroy(error);
    else { res.statusCode = 500; res.end("Gateway request failed"); }
  } finally {
    req.off("aborted", abort); req.off("error", abort); res.off("close", closed);
    if (body && !body.locked) void body.cancel().catch(() => {});
  }
});
server.listen(port, host, () => console.log(`cosy-redact-gateway listening on http://${host}:${server.address().port}`));
