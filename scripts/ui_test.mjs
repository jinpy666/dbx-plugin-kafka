#!/usr/bin/env node
// Kafka 插件 UI 走查（真实浏览器轨道）。
//
// 用法：node scripts/ui_test.mjs [--keep-server]
// 前置：frontend 依赖已安装（脚本自行拉起 vite dev，mock 宿主，无需 sidecar）。
// 浏览器：playwright-core + 系统 Chrome（惰性安装到 /tmp/dbx-kafka-ui-deps，
// 不进项目依赖；找不到 Chrome 时整个走查 SKIP 而非 FAIL，遵循 test.sh 约定）。
// 断言失败 → exit 1；环境不可用 → exit 0（打印 SKIP）。
//
// 并行开发期守卫：frontend/mock.html 尚未落地或应用挂载点缺失时整体 SKIP
// （规则 5：并行开发期不阻塞其他路），frontend 就绪后走查即生效。
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { platform } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PLUGIN_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FRONTEND_DIR = path.join(PLUGIN_ROOT, "frontend");
const DEPS_DIR = "/tmp/dbx-kafka-ui-deps";
const PORT = 5281;

const skip = (reason) => {
  console.log(`SKIP: kafka UI walkthrough — ${reason}`);
  process.exit(0);
};

async function fetchOk(url) {
  try {
    const response = await fetch(url);
    return response.ok;
  } catch {
    return false;
  }
}

async function waitForServer(url, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fetchOk(url)) return;
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  throw new Error(`vite dev server not ready at ${url}`);
}

async function loadPlaywrightCore() {
  // A stale/corrupt deps dir (interrupted install, partial upgrade) must
  // self-heal instead of crashing: any require failure wipes the dir and
  // falls through to a fresh install below.
  for (const candidate of [path.join(DEPS_DIR, "node_modules", "playwright-core")]) {
    if (existsSync(candidate)) {
      try {
        const require = createRequire(path.join(candidate, "index.js"));
        return require("playwright-core");
      } catch {
        console.log(`==> stale playwright-core in ${DEPS_DIR}; reinstalling`);
        rmSync(DEPS_DIR, { recursive: true, force: true });
      }
    }
  }
  console.log(`==> installing playwright-core into ${DEPS_DIR} (non-project dependency)`);
  mkdirSync(DEPS_DIR, { recursive: true });
  const result = await new Promise((resolve) => {
    const child = spawn("npm", ["install", "--prefix", DEPS_DIR, "playwright-core@1.49.1", "--no-audit", "--no-fund"], {
      stdio: "ignore",
    });
    child.on("error", () => resolve(false));
    child.on("close", (code) => resolve(code === 0));
  });
  if (!result || !existsSync(path.join(DEPS_DIR, "node_modules", "playwright-core"))) {
    skip("playwright-core unavailable (no network?)");
  }
  const require = createRequire(path.join(DEPS_DIR, "node_modules", "playwright-core", "index.js"));
  return require("playwright-core");
}

function findChromeExecutable() {
  if (process.env.KAFKA_UI_CHROME) return process.env.KAFKA_UI_CHROME;
  const candidates =
    platform() === "darwin"
      ? [
          "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
          "/Applications/Chromium.app/Contents/MacOS/Chromium",
          "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ]
      : ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"];
  return candidates.find((candidate) => existsSync(candidate)) || "";
}

const tests = [];
const test = (name, fn) => tests.push({ name, fn });
const expectEqual = (actual, expected, label) => {
  if (actual !== expected) throw new Error(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
};

// -- 走查流 --------------------------------------------------------------------
// scaffold 阶段守卫最小走查：应用挂载 + 无未捕获页面错误。具体面板
// （TopicTree/MessagesPanel/StreamPanel…）的交互断言随 frontend 路落地追加。

test("mock.html mounts the app", async (page) => {
  const mounted = await page.evaluate(() => {
    const app = document.querySelector("#app");
    return Boolean(app && app.children.length > 0);
  });
  if (!mounted) {
    skip("frontend app mount point not present yet (frontend path under parallel development)");
  }
});

test("no uncaught page errors", async (page, pageErrors) => {
  expectEqual(pageErrors.length, 0, `uncaught page errors: ${pageErrors.slice(0, 3).join(" | ")}`);
});

test("MCP ui intent consumes a topic and reports state", async (page) => {
  await page.evaluate(() => {
    const reports = [];
    const invoke = window.dbxPlugin.invoke.bind(window.dbxPlugin);
    window.dbxPlugin.invoke = async (method, params, options) => {
      if (method === "kafka/ui/state/report") reports.push({ intentId: params.intentId, status: params.status });
      return invoke(method, params, options);
    };
    window.__uiReports = reports;
    window.dbxPlugin.emitKafkaUiIntent({ intentId: "ui-e2e-1", action: "search", params: { topic: "codec-lab", offsetStrategy: "earliest", limit: 10 } });
  });
  try {
    await page.waitForFunction(
      () => window.__uiReports.some((report) => report.intentId === "ui-e2e-1" && report.status === "applied"),
      undefined,
      { timeout: 8000 },
    );
  } catch {
    const seen = await page.evaluate(() => window.__uiReports || []);
    throw new Error(`expected an applied kafka/ui/state/report for ui-e2e-1, saw ${JSON.stringify(seen)}`);
  }
});

// -- main ----------------------------------------------------------------------

const playwright = await loadPlaywrightCore();
const executablePath = findChromeExecutable();
const launchOptions = executablePath ? { executablePath } : { channel: "chrome" };

if (!existsSync(path.join(FRONTEND_DIR, "package.json"))) {
  skip("frontend scaffold not present yet (frontend path under parallel development)");
}

const server = spawn("pnpm", ["--dir", "frontend", "exec", "vite", "--port", String(PORT), "--strictPort"], {
  cwd: PLUGIN_ROOT,
  stdio: ["ignore", "pipe", "pipe"],
});
// KAFKA-UIT-L10：环境无 pnpm 时 ChildProcess 会发 'error' 事件——不监听则
// 未捕获异常丑栈，而非既定的 SKIP/FAIL 语义。
server.on("error", (cause) => {
  console.error(`FAIL: kafka UI walkthrough — vite dev server failed to start: ${cause.message}`);
  process.exit(1);
});
let browser;
try {
  const url = `http://localhost:${PORT}/mock.html`;
  await waitForServer(url);
  if (!(await fetchOk(url))) {
    skip("frontend mock.html not present yet (frontend path under parallel development)");
  }
  browser = await playwright.chromium.launch({ headless: true, ...launchOptions });
  const page = await browser.newPage({ viewport: { width: 1280, height: 860 } });
  const pageErrors = [];
  page.on("pageerror", (error) => pageErrors.push(String(error)));
  await page.goto(url);

  let failures = 0;
  for (const { name, fn } of tests) {
    try {
      await fn(page, pageErrors);
      console.log(`  ✓ ${name}`);
    } catch (cause) {
      failures += 1;
      console.error(`  ✗ ${name}\n    ${cause instanceof Error ? cause.message : cause}`);
    }
  }
  console.log(failures === 0 ? `\nUI walkthrough: ${tests.length}/${tests.length} passed` : `\nUI walkthrough: ${tests.length - failures}/${tests.length} passed`);
  process.exitCode = failures === 0 ? 0 : 1;
} catch (cause) {
  if (/browser|executable|chrome/i.test(String(cause))) skip(`no Chrome available (${cause})`);
  console.error(`FAIL: kafka UI walkthrough crashed: ${cause instanceof Error ? cause.message : cause}`);
  process.exitCode = 1;
} finally {
  await browser?.close().catch(() => {});
  if (!process.argv.includes("--keep-server")) server.kill("SIGTERM");
}
