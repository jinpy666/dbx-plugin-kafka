# Kafka 插件独立仓库迁移说明

## 迁移来源与历史

本仓库由 `/Users/Jinpy/btroot/dbx-plugins/kafka` 拆出，迁移基线为源 monorepo
commit `ce19beb4fa75cc79048e3ba8852c298d674fd9b3`。初始导入使用
`git subtree split --prefix=kafka`，因此保留了 Kafka 目录相关提交历史。

源仓库当时的 `host` 是用户正在使用的子模块 checkout（存在与本迁移无关的
"M host" 状态），本次迁移没有更新、reset、清理或写入它。拆分前工作区中的
monorepo 忽略项（截图、运行时产物等）不随 subtree split 带入。

## 独立仓库内容

- 根目录直接包含 `manifest.json`、`dbx-plugin.toml`、`frontend/`、`backend/`、
  `assets/`、`ui/`、`scripts/`、`docs/` 和 `.github/`。
- `shared/frontend/` 只保留 Kafka 实际使用的 `binaryEvent`、`editorTheme`、
  `themeSync`、`uiIntent` 及说明文档的 vendored 副本；`frontend/src` 的引用
  深度从 monorepo 的三级/四级改为独立仓库的两级/三级。没有迁入 LDAP、Files
  或 SSH 代码。
- `shared/sdk/go/dbx-plugin-sdk/` 是与拆分基线匹配的最小 Go sidecar SDK
  vendoring（纯标准库）；`backend/go.mod` 的 replace 从宿主 worktree 改为
  `=> ../shared/sdk/go/dbx-plugin-sdk`，`go vet` / `go test` 不再依赖 `../host`。
  `dbx-plugin package` 打包时 CLI 仍通过 DBX_PLUGIN_SDK_ROOT + go.work 注入
  自己的解析路径，此 replace 不影响打包。
- `scripts/connection-forms/verify.mjs` 是连接表单契约的独立校验器，包含与
  DBX Host 条件语义一致的可见性/必填级联检查及 Kafka 九百六十组合的场景矩阵
  （broker 发现 × 安全协议 × SASL 机制（含 Kerberos/OAuth 子表单）× Schema
  Registry × 只读/删除门控）。用法：`node scripts/connection-forms/verify.mjs kafka`。
- `scripts/cli-platform.sh` 解决 plugin-cli 平台包后缀解析（linux 带 `-gnu`
  后缀）；`scripts/build.sh` 与 `scripts/test.sh` 改用
  `resolve_native_plugin_cli` 调用原生 CLI，避免 npm wrapper 注入的 SDK
  go.work（钉在 go 1.22）与 backend go.mod（go 1.26）冲突，任何平台都不再
  退化到 wrapper。

## CI 与发布

- `.github/workflows/ci.yml`：validate job 校验 manifest/dbx-plugin.toml/Go
  身份/连接表单；frontend job 跑 typecheck/test/build 并对提交进仓库的 `ui/`
  构建产物做 freshness 检查；backend job 跑 `go vet ./... && go test ./...`
  （go 1.26.x，经 go.sum 锁定拉取模块）；candidate job 在 5 个目标
  （linux-x64/linux-arm64/darwin-arm64/darwin-x64/windows-x64）上经
  `DBX_PLUGIN_TARGET=<t> bash scripts/build.sh` 打包 `.dbxp`，对本地构建的
  sidecar 跑离线 MCP stdio smoke，最后 `scripts/check_candidates.py` 做跨
  目标一致性检查。
- `.github/workflows/release.yml` 是自包含 build+publish（不使用 t8y2/dbx
  reusable workflow）：仅 `kafka-v<manifest.version>` 形态的 GitHub Release
  触发，5 目标构建后把 `.dbxp` 与 `release-candidates.json` 上传回 Release。
  它不内置 signing key、远端仓库或秘密。
- `manifest.json` 的 `source`/`homepage` 指向
  `https://github.com/jinpy666/dbx-plugin-kafka`。版本号保持 monorepo 基线
  （0.1.41）并与 sidecar 注入的 `main.version` 一致。

## Bug 回写与发布边界

独立仓库中的 bug 先在本仓库修复并通过 CI；若 DBX Host API、宿主安装器或公共
SDK 需要变更，应另开 DBX host/SDK 变更并在 issue/PR 中记录对应版本。不要把
本仓库重新指向 monorepo 的 `../shared`；monorepo `shared/frontend` 公共层
演进后，把变更同步回本仓库的 vendored 副本并在拆分基线上重新验证。需要跨
插件复用时应发布公共包或同步回各独立仓库的明确版本。

真实 Kafka 集群（`docker-compose.kafka-test.yml` 测试容器、
`scripts/dev-cluster.sh` dev 集群、Schema Registry）和 DBX.app host 安装管线
不是离线 CI 的通过条件；没有这些环境时相关 live/e2e/smoke 用例必须输出
`SKIP`（`scripts/smoke_mcp.py`、`scripts/smoke_test.py` 的容器用例即此语义），
不能伪报 `PASS`。
