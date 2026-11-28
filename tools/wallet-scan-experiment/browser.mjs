#!/usr/bin/env node
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { arch, availableParallelism, cpus, loadavg, platform, release } from "node:os";
import { basename, extname } from "node:path";

const [bindingsPath, corpusPath, batchArg = "128", cyclesArg = "1", ...extra] =
  process.argv.slice(2);
const batchSize = Number(batchArg);
const cycles = Number(cyclesArg);
if (
  !bindingsPath || !corpusPath || extra.length ||
  !Number.isSafeInteger(batchSize) || batchSize <= 0 || batchSize > 0xffffffff ||
  !Number.isSafeInteger(cycles) || cycles <= 0
) {
  throw new Error("Usage: node browser.mjs <web-bindings.js> <corpus> [batch-size] [cycles]");
}

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || "playwright");
const wasmPath = bindingsPath.slice(0, -extname(bindingsPath).length) + "_bg.wasm";
const [bindings, wasm, corpus] = await Promise.all(
  [bindingsPath, wasmPath, corpusPath].map((path) => readFile(path))
);
const digest = (buffer) => createHash("sha256").update(buffer).digest("hex");
const origin = "http://127.0.0.1";
const routes = new Map([
  ["/", { contentType: "text/html", body: "<!doctype html><title>Wallet scan</title>" }],
  ["/bindings.js", { contentType: "application/javascript", body: bindings }],
  ["/scanner.wasm", { contentType: "application/wasm", body: wasm }],
  ["/corpus", { contentType: "application/octet-stream", body: corpus }],
]);

const arms = ["stock", "ordinary-prefilter", "hinted"];
const orders = arms.flatMap((first) => arms.filter((second) => first !== second).map((second) => [
  first, second, arms.find((arm) => arm !== first && arm !== second),
]));
const browser = await chromium.launch({
  executablePath: process.env.CHROMIUM_EXECUTABLE,
  headless: true,
});
try {
  const page = await browser.newPage();
  await page.route("**/*", async (request) => {
    const url = new URL(request.request().url());
    const response = url.origin === origin && routes.get(url.pathname);
    if (response) await request.fulfill(response);
    else await request.abort();
  });
  await page.goto(origin);
  const diagnostics = await page.evaluate(async (batchSize) => {
    const { default: load, BrowserScanner } = await import("/bindings.js");
    await load({ module_or_path: fetch("/scanner.wasm") });
    const corpus = new Uint8Array(await (await fetch("/corpus")).arrayBuffer());
    window.scanner = new BrowserScanner(corpus, batchSize);
    return JSON.parse(window.scanner.validate());
  }, batchSize);

  const session = await page.context().newCDPSession(page);
  await session.send("Performance.enable", { timeDomain: "threadTicks" });
  const threadTime = async () => {
    const { metrics } = await session.send("Performance.getMetrics");
    const value = metrics.find(({ name }) => name === "ThreadTime")?.value;
    if (!Number.isFinite(value)) throw new Error("Chromium did not expose renderer ThreadTime");
    return value;
  };

  const expected = diagnostics.total_receipts;
  if (!Number.isSafeInteger(expected) || expected < 0) throw new Error("invalid expected receipt count");
  const scan = async (arm) => {
    const before = await threadTime();
    const { count, elapsedMs } = await page.evaluate((arm) => {
      const start = performance.now();
      const count = window.scanner.scan(arm);
      return { count, elapsedMs: performance.now() - start };
    }, arm);
    const threadCpuMs = (await threadTime() - before) * 1000;
    if (count !== expected) throw new Error(`expected ${expected} receipts, ${arm} returned ${count}`);
    return { arm, elapsedMs, threadCpuMs };
  };
  for (const arm of arms) await scan(arm);
  const samples = [];
  const startTime = new Date().toISOString();
  const loadStart = loadavg();
  for (let cycle = 0; cycle < cycles; cycle++) {
    for (const order of orders) {
      const observations = [];
      for (const arm of order) observations.push(await scan(arm));
      samples.push({ cycle, observations });
    }
  }
  const report = {
    environment: {
      chromium: browser.version(), node: process.version, os: platform(), kernel: release(),
      arch: arch(), cpu: cpus()[0]?.model, availableCpus: availableParallelism(),
      loadStart, loadEnd: loadavg(), startTime, endTime: new Date().toISOString(),
    },
    inputs: {
      corpus: basename(corpusPath), corpusSha256: digest(corpus), wasmSha256: digest(wasm),
      bindingsSha256: digest(bindings), batchSize, cycles,
    },
    diagnostics, validatedReceiptCount: expected, samples,
    elapsedScope: "Warmed synchronous scanner call in Chromium; excludes corpus loading and hint generation.",
    cpuScope: "Renderer main-thread ThreadTime between CDP probes around each scan, including dispatch.",
  };
  process.stdout.write(JSON.stringify(report, null, 2) + "\n");
} finally {
  await browser.close();
}
