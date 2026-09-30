# Kafka Studio

[![CI](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/jinpy666/dbx-plugin-kafka/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/jinpy666/dbx-plugin-kafka?display_name=tag)](https://github.com/jinpy666/dbx-plugin-kafka/releases)

[中文](README.md) · [Showcase](docs/MEDIA.en.md) · [Feature comparison](docs/COMPARISON.en.md) · [MCP guide](docs/MCP_USAGE.en.md) · [Repository split notes](docs/REPOSITORY_SPLIT.en.md)

**Kafka Studio is the Apache Kafka command center inside DBX.** Topic inspection,
message search, stream consumption, consumer-group observation, schema checks, and
guarded write operations all happen in one connection context — and the same
capabilities are handed to AI through 11 MCP tools. The hour after connecting to a
cluster stops being a jump between CLIs, web consoles, and chat windows, and becomes
one coherent, auditable, reusable workflow.

> Topics · Messages · Streams · Consumer Groups · Brokers · ACLs · Schema Registry —
> one workspace for everyday Kafka operations. Guarded writes, AI-automatable.

▶️ [Watch the demo video](docs/media/kafka-studio-demo.mp4)

## Why teams reach for it

| Your job | The Kafka Studio workflow |
| --- | --- |
| Check cluster health | The topic tree flags unhealthy partitions outright; partitions, ISR, configs, brokers, and cluster metadata in one view |
| Find that one message | key/value/header multi-channel filters, field search, 6 offset strategies; compression and multi-format decoding out of the box |
| Keep lag on a leash | Consumer-group lag, member, and offset views, plus threshold alerts in the monitor panel (watch plans are saved and reusable) |
| Change data with confidence | Writes are gated per connection; delete-class operations sit behind a separate allowDelete gate; MCP writes enforce a preview → confirmToken two-phase flow and land in the audit trail |
| Let AI take the shift | 11 MCP tools reuse saved connections and permission boundaries; digest aggregates locally in the sidecar with cursor paging, so AI gets conclusions instead of raw bulk data |

## 🔌 Full capability matrix

### Connection protocols & authentication

| Category | Supported |
| --- | --- |
| Transport encryption | PLAINTEXT · TLS · mutual TLS (mTLS, custom CA / client certificates in PEM) |
| SASL mechanisms | PLAIN · SCRAM-SHA-256 · SCRAM-SHA-512 |
| Kerberos | GSSAPI (keytab + principal + krb5.conf; realm / service name configurable) |
| OAuth | OAUTHBEARER: static tokens · AWS MSK IAM signed tokens (default credential chain, static keys, or STS session tokens) |
| Cluster discovery | Direct bootstrap servers (both KRaft and ZooKeeper clusters) · ZooKeeper broker discovery (chroot supported) |
| Credential handling | SASL / TLS / Kerberos / AWS / Schema Registry credentials all go through DBX host secret bindings — the plugin persists nothing |
| Fast migration | Paste an existing Kafka client properties snippet and the form fills itself (Java keystore path keys are honestly reported as ignored) |

### Compatible vendors & distributions

| Ecosystem | Supported capabilities |
| --- | --- |
| Apache Kafka | Full-featured workbench; both KRaft and ZooKeeper clusters connect |
| AWS MSK | IAM authentication (signed tokens; AWS default credential chain, static keys, or STS session tokens) |
| AWS Glue | Glue Schema Registry browsing and management (region / registry name / three credential sources) |
| Confluent | Schema Registry REST-compatible (Platform / Cloud, with basic auth) |
| Redpanda | Built-in Schema Registry works out of the box |
| Kerberos / AD | GSSAPI + keytab enterprise domain authentication |

### Message formats & codecs

| Capability | Details |
| --- | --- |
| Compression | Produce: gzip · lz4 · zstd · snappy (batch compression); consume: matching decompression with bomb guards — frame-header pre-checks plus a 16 MiB output cap |
| Payload views | UTF-8 · JSON (pretty) · XML · Hex · BitSet · Base64 decoding |
| Schema codecs | Automatic Confluent wire-format decoding: **AVRO · JSON Schema · PROTOBUF** (dynamic descriptors, no local code generation) |
| Search | key/value/header multi-channel filters · field-level search · 6 offset strategies (latest / earliest / recent / committed / timestamp / offset) |
| Produce input | Text or Base64-faithful binary key/value · message headers · target partition · batches ≤1000 records (≤64 KiB each) · acks all/1 · idempotence toggle |
| Export | JSON · CSV (streaming and batch) |

### Schema Registry management

| Operation | Notes |
| --- | --- |
| Browse | Subjects / versions / schema content / compatibility levels at a glance |
| Register | New versions with AVRO / JSON Schema / PROTOBUF and schema references |
| Compatibility | Compatibility checks plus viewing and setting compatibility levels |
| Delete | By subject or version (behind the allowDelete gate) |
| Produce-mount | Pick a schema while producing; the sidecar encodes the payload and wraps the Confluent wire format |

### Cluster operations

| Object | Operations |
| --- | --- |
| Topics | Create · delete · expand partitions · view/alter configs · partition offsets · clear records |
| Brokers | List · inspect dynamic configs |
| Consumer groups | Lag / member / offset inspection · delete groups · offset reset (earliest / latest / timestamp / per-partition) |
| ACLs | View · create · delete |
| Monitoring | Consumer-group lag threshold watch plans (save / apply / breach alerts) |
| Audit | Key write operations inspectable in the workbench |

The UI ships in Simplified Chinese, Traditional Chinese, English, Spanish, Italian,
Japanese, and Portuguese.

## 🖥️ Nine panels, one workflow

**Messages · Stream · Produce · Topics · Consumer Groups · Brokers · ACLs · Schemas ·
Monitor**, plus an audit-trail view:

- **Messages**: search and consume per topic; the detail drawer expands every field; export to JSON/CSV.
- **Stream**: continuous consumption with pause, resume, and buffering at your fingertips; export to JSON/CSV.
- **Produce**: send records to a topic/partition with headers, compression, and schema mounting.
- **Topics**: create and delete topics, adjust partitions and configs; internal topics are clearly marked.
- **Consumer Groups**: lag and members at a glance; offset reset supports earliest/latest/timestamp/partitionOffset.
- **Brokers / ACLs**: cluster metadata inspection plus ACL viewing and management.
- **Schemas**: browse subjects and versions, feeding directly into producing and decoding.
- **Monitor**: set lag thresholds per consumer group, save watch plans, get alerted on breach.

## 🤖 AI automation (MCP)

The DBX MCP bridge is the recommended entry — it reuses saved connections, credential
resolution, and permission policy. Standalone mode also works:

```bash
backend/bin/dbx-plugin-kafka --mcp
```

There are 11 tools: `kafka_messages_digest` (local sidecar aggregation with cursor
paging), `kafka_cursor_next`, `kafka_messages_produce`, `kafka_groups_offsets_reset`,
`kafka_topics_delete`, `kafka_topics_records_clear`, plus five UI tools. MCP is
read-only by default: producing requires an explicit `readOnly: false`; deletion and
clearing additionally require `allowDelete: true` and the two-phase confirmation, and
write paths are recorded in the audit log. See the [MCP guide](docs/MCP_USAGE.en.md)
and the [Kafka MCP reference](docs/MCP.zh-CN.md) for configuration and safety details.

## 🔐 Security

- **Gated writes + read-only MCP defaults**: write capability is enabled per connection via the read_only / allow_delete form switches; MCP write tools default to read-only and require an explicit `readOnly: false` to proceed.
- **Double gate on deletion**: delete-class operations require both read-only off and allowDelete on, consistently across UI and MCP.
- **Two-phase confirmation**: MCP writes enforce preview → confirmToken, with single-use tokens.
- **Audit trail**: key operations are inspectable and replayable in the workbench.
- **Zero credential persistence**: every secret goes through host secret bindings — including pasted properties text.
- **Decompression-bomb guards**: frontend decompression is capped at 16 MiB with frame-header pre-checks, so a hostile payload can't blow up your memory.

See the [feature and competitor comparison](docs/COMPARISON.en.md) for how this
positions against kcat, kafka-console-consumer, and popular Kafka web UIs; the
[showcase page](docs/MEDIA.en.md) collects copy-ready messaging.

## 🎯 Use cases

- Confirm topic, partition, consumer-group, and cluster metadata in dev and test environments.
- Investigate payload formats, filters, consumer lag, and offset behavior without juggling CLI tools.
- Produce messages, adjust offsets, and manage topics and ACLs behind read-only and delete-confirmation safeguards.
- Let AI clients reuse saved connections over MCP for routine inspection and troubleshooting.

## 📦 Install

Download the `.dbxp` package for your platform from
[GitHub Releases](https://github.com/jinpy666/dbx-plugin-kafka/releases) and
install it locally from the DBX plugin center. Developers can build candidate
packages by following the [repository split notes](docs/REPOSITORY_SPLIT.en.md).

## 🛠️ Development

```bash
pnpm --dir frontend install
pnpm --dir frontend typecheck && pnpm --dir frontend test && pnpm --dir frontend build
(cd backend && go vet ./... && go test ./...)
python3 scripts/validate_repo.py && node scripts/connection-forms/verify.mjs kafka
scripts/test.sh
```

Start a local Kafka/Schema Registry test cluster with `scripts/dev-cluster.sh`
(Docker); run the MCP smoke with `python3 scripts/smoke_mcp.py` (container
scenarios honestly SKIP when their environment is absent).

## 📚 Documentation

- [Implementation plan](docs/IMPL_PLAN_DBX_KAFKA.zh-CN.md): scope and iteration log.
- [Protocol reference](docs/PROTOCOL_KAFKA.zh-CN.md): sidecar methods and events.
- [Kafka MCP reference](docs/MCP.zh-CN.md): tool contracts, inline credentials, and the app-bridge fallback.
- [MCP guide](docs/MCP_USAGE.en.md) / [Chinese](docs/MCP_USAGE.zh-CN.md): onboarding and common pitfalls.
- [Showcase](docs/MEDIA.en.md) / [Chinese](docs/MEDIA.zh-CN.md): copy-ready messaging.
- [Feature comparison](docs/COMPARISON.en.md) / [Chinese](docs/COMPARISON.zh-CN.md): positioning against alternatives.
- [Repository split notes](docs/REPOSITORY_SPLIT.en.md) / [Chinese](docs/REPOSITORY_SPLIT.zh-CN.md).
