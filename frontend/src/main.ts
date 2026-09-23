import { createApp } from "vue";
import App from "./App.vue";
import "./style.css";
import { installHostThemeBridge } from "../../shared/frontend/themeSync";
import { pluginStore } from "./lib/pluginStore";

// 宿主令牌 → 插件变量桥：首绘即命中宿主主题，主题变化经 SDK 令牌更新自动跟随。
installHostThemeBridge();

const boot = async () => {
  // UI 状态水合（宿主 host.storage → localStorage 降级 + 旧 localStorage 键搬家）
  // 先于挂载，保证组件 setup 内的同步首读命中持久化值（files 插件同款时序）。
  await pluginStore.ready;
  createApp(App).mount("#app");
};
void boot();
