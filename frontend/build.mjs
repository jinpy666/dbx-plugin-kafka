import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "vite";
import vue from "@vitejs/plugin-vue";

const root = path.dirname(fileURLToPath(import.meta.url));
const temporary = path.join(root, "dist");
const output = path.resolve(root, "../ui");

await build({
  root,
  configFile: false,
  plugins: [vue()],
  base: "./",
  build: {
    outDir: temporary,
    emptyOutDir: true,
    cssCodeSplit: false,
    assetsInlineLimit: 10 * 1024 * 1024,
    // vite 8（rolldown）：inlineDynamicImports 已废弃，等价写法为 codeSplitting: false
    // （构建告警指引）；保持单 chunk 自包含产物形态不变。
    rolldownOptions: { output: { codeSplitting: false } },
  },
});

let html = await fs.readFile(path.join(temporary, "index.html"), "utf8");
const scriptMatch = html.match(/<script[^>]+src="\.\/([^"]+\.js)"[^>]*><\/script>/);
if (!scriptMatch) throw new Error("Vite output did not contain a JavaScript entry");
const script = await fs.readFile(path.join(temporary, scriptMatch[1]), "utf8");
// 内联双层转义：① `</script` 防止提前闭合；② `<!--` 防止 HTML 解析器进入
// script 注释态（highlight.js 语言定义同时含 `<!--` 与 `<script` 字面量，
// 组合起来会让真实闭合标签失效）——`\!` 在 JS 字符串/正则中均等价 `!`，语义不变。
const inlineScript = script.replace(/<\/script/gi, "<\\/script").replace(/<!--/g, "<\\!--");
html = html.replace(scriptMatch[0], () => `<script type="module">${inlineScript}</script>`);

const styleMatch = html.match(/<link[^>]+href="\.\/([^"]+\.css)"[^>]*>/);
if (styleMatch) {
  const style = await fs.readFile(path.join(temporary, styleMatch[1]), "utf8");
  const inlineStyle = style.replace(/<\/style/gi, "<\\/style");
  html = html.replace(styleMatch[0], () => `<style>${inlineStyle}</style>`);
}

if (html.includes(scriptMatch[0])) {
  throw new Error("Self-contained UI still contains the external JavaScript entry tag");
}
if (styleMatch && html.includes(styleMatch[0])) {
  throw new Error("Self-contained UI still contains the external stylesheet tag");
}
if ((html.match(/<script\b/gi) ?? []).length !== 1 || (html.match(/<\/script>/gi) ?? []).length !== 1) {
  throw new Error("Self-contained UI contains an invalid script structure");
}

await fs.mkdir(output, { recursive: true });
await fs.writeFile(path.join(output, "index.html"), html, "utf8");
console.log(`Wrote self-contained plugin UI to ${path.join(output, "index.html")}`);
