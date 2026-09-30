# Kafka Studio

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[English](README.en.md) · [产品宣传页](docs/MEDIA.zh-CN.md) · [特性与竞品对比](docs/COMPARISON.zh-CN.md) · [MCP 使用指南](docs/MCP_USAGE.zh-CN.md) · [独立仓库迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md)

**Kafka Studio 是装进 DBX 的 Apache Kafka 主控台。** Topic 巡检、消息检索、流式消费、
消费组观测、Schema 查看和受控写操作，在同一个连接上下文里完成；再通过 11 个 MCP 工具，
把整套能力原样交给 AI。连接集群之后的那一小时，不再是命令行、网页控制台和聊天窗口
之间的来回横跳，而是一条连贯、可审计、可复用的工作流。

> Topics · Messages · Streams · Consumer Groups · Brokers · ACLs · Schema Registry ——
> 一个工作台管完 Kafka 日常运维，默认只读，AI 可自动化。

▶️ [观看功能演示视频](docs/media/kafka-studio-demo.mp4)

## 为什么是 Kafka Studio

| 你要完成的事 | Kafka Studio 给你的体验 |
| --- | --- |
| 巡检集群健康度 | Topic 树直接标出不健康分区；分区、ISR、配置与 broker、集群元数据一屏看全 |
| 找到那条消息 | key/value/header 多通道过滤、字段检索、5 种 offset 策略；压缩与多格式解码开箱即用 |
| 盯住消费延迟 | 消费组 lag、成员与 offset 视图，加监控面板的阈值告警（watch plan 可保存复用） |
| 改数据不心虚 | 默认只读；删除类操作另有 allowDelete 门；MCP 写路径强制 preview → confirmToken 两阶段确认，全程落审计轨迹 |
| 让 AI 替你值班 | 11 个 MCP 工具复用已保存连接与权限边界；digest 在 sidecar 本地聚合 + cursor 翻页，AI 拿到结论而不是全量数据 |

## 🔌 全能力矩阵

### 连接协议与认证

| 类别 | 支持范围 |
| --- | --- |
| 传输加密 | PLAINTEXT · TLS · 双向 TLS（mTLS，支持自定义 CA / 客户端证书 PEM） |
| SASL 机制 | PLAIN · SCRAM-SHA-256 · SCRAM-SHA-512 |
| Kerberos | GSSAPI（keytab + principal + krb5.conf，realm / service name 可配） |
| OAuth | OAUTHBEARER：静态 token · AWS MSK IAM 签名 token（支持 STS 临时凭据或默认凭据链） |
| 集群发现 | Bootstrap servers 直连（KRaft 与 ZooKeeper 集群皆可）· ZooKeeper broker 发现（含 chroot） |
| 凭据管理 | SASL / TLS / Kerberos / AWS / Schema Registry 凭据全部经 DBX 宿主 secret binding，插件零落盘 |
| 快速迁移 | 粘贴现成 Kafka 客户端 properties 自动填充表单（keystore 路径类键诚实列入忽略清单） |

### 兼容厂商与发行版

| 生态 | 支持能力 |
| --- | --- |
| Apache Kafka | 全功能工作台；KRaft 与 ZooKeeper 集群均可接入 |
| AWS MSK | IAM 认证（签名 token，含 AWS 默认凭据链 / 静态密钥 / STS 临时凭据） |
| AWS Glue | Glue Schema Registry 浏览与管理（区域 / registry 名称 / 三种凭据来源） |
| Confluent | 兼容 Schema Registry REST API（Platform / Cloud，含 basic auth） |
| Redpanda | 内置 Schema Registry 直接可用 |
| Kerberos / AD 域 | GSSAPI + keytab 企业域认证 |

### 消息格式与编解码

| 能力 | 明细 |
| --- | --- |
| 压缩 | 生产：gzip · lz4 · zstd · snappy（批压缩）；消费：同名算法解压，帧头预检 + 16 MiB 输出上限双重解压炸弹防护 |
| 载荷视图 | UTF-8 · JSON（pretty）· XML · Hex · BitSet · Base64 解码 |
| Schema 编解码 | Confluent wire format 自动解码：**AVRO · JSON Schema · PROTOBUF**（动态 descriptor，无需本地代码生成） |
| 检索 | key/value/header 多通道过滤 · 字段级检索 · 5 种 offset 策略（latest / earliest / committed / timestamp / offset） |
| 生产输入 | key/value 文本或 Base64 二进制保真 · 消息 headers · 指定分区 · 批量 ≤1000 条（单条 ≤64 KiB）· acks all/1 · 幂等开关 |
| 导出 | JSON · CSV（流式与批量皆可） |

### Schema Registry 管理

| 操作 | 说明 |
| --- | --- |
| 浏览 | subject / 版本 / schema 内容 / 兼容级别一览 |
| 注册 | 新增版本，支持 AVRO / JSON Schema / PROTOBUF 与 schema references |
| 兼容性 | 兼容性检查 + 兼容级别查看与设置 |
| 删除 | 按 subject 或版本删除（受 allowDelete 门保护） |
| 生产挂载 | 生产消息时选择 schema，sidecar 自动编码并打包 Confluent wire format |

### 集群运维操作

| 对象 | 操作 |
| --- | --- |
| Topic | 创建 · 删除 · 扩分区 · 配置查看/修改 · 分区 offset 巡检 · 记录清理 |
| Broker | 列表 · 动态配置查看 |
| 消费组 | lag / 成员 / offset 巡检 · 删除组 · offset 重置（earliest / latest / timestamp / 逐分区指定） |
| ACL | 查看 · 创建 · 删除 |
| 监控 | 消费组 lag 阈值 watch plan（保存 / 应用 / 超限提醒） |
| 审计 | 关键写操作轨迹在 workbench 内可查 |

界面支持简体中文、繁体中文、英语、西班牙语、意大利语、日语和葡萄牙语。

## 🖥️ 九个面板，一条工作流

**消息 · 流式 · 生产 · Topic · 消费组 · Broker · ACL · Schema · 监控**，外加审计轨迹视图：

- **消息**：按 topic 检索与消费，消息详情抽屉逐字段展开，支持 JSON/CSV 导出。
- **流式**：持续消费，暂停、恢复、缓冲随时可控，结果导出 JSON/CSV。
- **生产**：向指定 topic/partition 发送消息，支持 headers、压缩与 schema 挂载。
- **Topic**：创建、删除 topic，调整分区与配置，内部 topic 一眼可辨。
- **消费组**：lag 与成员一览，offset 重置支持 earliest/latest/timestamp/partitionOffset。
- **Broker / ACL**：集群元数据巡检与 ACL 查看、管理。
- **Schema**：浏览 subject 与版本，直接参与消息生产与解码。
- **监控**：对消费组 lag 设阈值、存 watch plan，超限即提醒。

## 🤖 AI 自动化（MCP）

推荐经 DBX MCP 桥接入，复用已保存连接、凭据解析和权限策略；也支持独立模式：

```bash
backend/bin/dbx-plugin-kafka --mcp
```

共 11 个工具：`kafka_messages_digest`（sidecar 本地聚合 + cursor 翻页）、
`kafka_cursor_next`、`kafka_messages_produce`、`kafka_groups_offsets_reset`、
`kafka_topics_delete`、`kafka_topics_records_clear`，以及 5 个 UI 类工具。
MCP 默认只读：生产消息需显式传 `readOnly: false`，删除与清理还需 `allowDelete: true`
并走两阶段确认，写路径落审计日志。配置与安全边界见
[MCP 使用指南](docs/MCP_USAGE.zh-CN.md) 与 [Kafka MCP 参考](docs/MCP.zh-CN.md)。

## 🔐 安全设计

- **默认只读**：新连接默认拒绝一切写操作，写能力逐连接显式开启。
- **删除双重门**：删除类操作要求同时关闭只读并打开 allowDelete，UI/MCP 双侧一致。
- **两阶段确认**：MCP 写操作强制 preview → confirmToken，token 单次有效。
- **审计轨迹**：关键操作在 workbench 内可查、可回放。
- **凭据零落盘**：所有密钥走宿主 secret binding，包括粘贴导入的 properties 文本。
- **解压炸弹防护**：前端解压全线 16 MiB 上限 + 帧头预检，恶意 payload 打不开你的内存。

与 kcat、kafka-console-consumer、各类 Kafka Web UI 的定位对比见
[特性与竞品对比](docs/COMPARISON.zh-CN.md)；更多宣传文案见 [产品宣传页](docs/MEDIA.zh-CN.md)。

## 🎯 适合场景

- 开发与测试环境快速确认 topic、分区、消费组和集群元数据是否符合预期。
- 排查消息格式、过滤条件、消费延迟和 offset 问题，无需在多个命令行工具间切换。
- 在只读与删除确认策略保护下完成消息生产、offset 调整、topic 与 ACL 运维。
- 让 AI 客户端经 MCP 复用已保存连接，完成日常巡检和排障。

## 📦 安装

从 [GitHub Releases](https://github.com/jinpy666/dbx-plugin-kafka/releases) 下载匹配平台的
`.dbxp` 包，在 DBX 插件中心选择本地安装。开发者也可以按照
[迁移与发布说明](docs/REPOSITORY_SPLIT.zh-CN.md) 构建候选包。

## 🛠️ 开发与验证

```bash
pnpm --dir frontend install
pnpm --dir frontend typecheck && pnpm --dir frontend test && pnpm --dir frontend build
(cd backend && go vet ./... && go test ./...)
python3 scripts/validate_repo.py && node scripts/connection-forms/verify.mjs kafka
scripts/test.sh        # 前端三步 + go 测试 + 打包（自动清理 dist/ 旧版本产物）+ 冒烟
scripts/install.sh     # 用官方安装器把 dist/ 最新 .dbxp 装进 DBX 并重启（自动清理旧安装版本；--keep-old 保留回滚）
```

打包阶段（`scripts/package.sh`，被 build/test 共用）只保留与 manifest.json 当前版本一致的
`.dbxp`/`.artifact.json`，旧版本产物自动清理；安装阶段默认清理 `io.dbx.kafka` 的旧安装版本，
需要回滚时用 `scripts/install.sh --keep-old`。

本地 Kafka/Schema Registry 测试集群用 `scripts/dev-cluster.sh` 拉起（Docker）；
MCP 离线冒烟用 `python3 scripts/smoke_mcp.py`（容器类用例在环境不可用时诚实 SKIP）。

## 📚 文档索引

- [实施计划](docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md)：能力范围与轮次记录。
- [协议文档](docs/PROTOCOL_KAFKA.zh-CN.md)：sidecar 协议方法与事件。
- [Kafka MCP 参考](docs/MCP.zh-CN.md)：工具契约、内联凭据与桥接兜底。
- [MCP 使用指南](docs/MCP_USAGE.zh-CN.md) / [英文版](docs/MCP_USAGE.en.md)：接入与常见坑。
- [产品宣传页](docs/MEDIA.zh-CN.md) / [英文版](docs/MEDIA.en.md)：可直接引用的宣传素材。
- [特性与竞品对比](docs/COMPARISON.zh-CN.md) / [英文版](docs/COMPARISON.en.md)：定位对比。
- [独立仓库迁移说明](docs/REPOSITORY_SPLIT.zh-CN.md) / [英文版](docs/REPOSITORY_SPLIT.en.md)。
