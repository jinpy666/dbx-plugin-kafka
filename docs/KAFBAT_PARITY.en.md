# Kafbat parity tracker

Reference: Kafbat UI **v1.5.0**, commit `afc9c91` (release tag).
Plugin starting point: Kafka Studio **v0.1.57**,
commit `0fb79d19f087a1409881a8fe63a6946fe27ee3a6`.
Public references: [Kafbat API](https://github.com/kafbat/kafka-ui/blob/v1.5.0/contract/src/main/resources/swagger/kafbat-ui-api.yaml),
[Kafbat release](https://github.com/kafbat/kafka-ui/releases/tag/v1.5.0),
[SerDe initialization](https://github.com/kafbat/kafka-ui/blob/v1.5.0/api/src/main/java/io/kafbat/ui/serdes/SerdesInitializer.java).

This is a staged delivery tracker, not a declaration of full parity. Existing
plugin capabilities are marked unverified until compared with the reference.
Each implementation contribution must add its tests and update this evidence.
The first HEL1 candidate requires only the Connect milestone plus host acceptance;
the other milestones remain in scope for subsequent releases.

| Capability | State | Evidence / next gate |
| --- | --- | --- |
| Multiple Connect clusters, TLS/mTLS, basic/bearer authentication | Implemented | `connect_test.go`; host secret binding/installation acceptance pending |
| Connector/plugin/config discovery, validation, create/update/delete | Implemented | `smoke_connect.py` C2/C3/C7; real worker acceptance must pass |
| Connector and task status, failed task isolation | Implemented | `connect_test.go`; `smoke_connect.py` C5 |
| Pause/resume/stop and restart variants, task restart, offsets reset | Implemented | `connect_test.go`; `smoke_connect.py` C4/C7 |
| Connector/topic/group navigation | Implemented | `ConnectPanel.spec.ts`; browser walkthrough |
| Read-only, destructive confirmation, redaction/audit | Implemented | `connect_test.go`; `ConnectPanel.spec.ts`; `smoke_connect.py` C6 |
| Topic/broker/group/config/ACL inspection and baseline mutations | Existing; reference comparison pending | Existing suites; check reference behaviors before claiming parity |
| Topic clone/recreate, replication/reassignment and log-directory operations | Pending | Admin milestone |
| Consumer offset deletion, active producer view and topic analysis | Pending | Admin/observation milestones |
| ACL helper workflows and CSV sync; client quotas | Pending | Admin milestone |
| Cluster/broker/topic metrics, Prometheus graphs/store/export | Pending | Observation milestone; optional services required |
| Message browse/produce/stream/export and existing filters | Existing; reference comparison pending | Existing suites |
| CEL filters, filter testing and full forward/backward/tailing behavior | Pending | Message milestone |
| Backend masking across browse/stream/export/MCP | Pending | Message milestone; no masked-data bypass permitted |
| Schema browse/register/compatibility/delete and current codecs | Existing; reference comparison pending | Existing suites |
| Remaining Schema Registry auth, references and subject strategies | Pending | Schema milestone |
| Built-in numeric/UUID/MessagePack, raw/file Protobuf and internal/MM2 SerDes | Pending | Codec milestone; golden input/output comparison |
| Custom Java SerDe JAR compatibility | Pending | Optional managed JVM; v1.5.0 SerDe API fixtures |
| ksqlDB queries, commands and stream/table browsing | Pending | Integration milestone; real disposable ksqlDB required |
| Cloud authentication/managed service gaps | Pending | Compare stable-reference providers; local mocks are insufficient for cloud acceptance |
| Multi-cluster setup, dynamic configuration and file import | Partial via DBX | Compare host connection workflows; no alternate secret store |
| API documentation and MCP surface | Partial via plugin RPC/MCP | Contract docs maintained; compare reference coverage separately |
| Audit history/export/integrations and discovery integrations | Partial; assessment pending | Inspect reference implementation, not README alone |
| UI login providers and per-user resource RBAC | Accepted host gap | DBX core/SDK changes excluded; read-only flags are not RBAC |
| HEL1 signed package/install/restart/persistence/resource/rollback acceptance | Pending | Isolated DBX Web 0.6.69 gate, then operator rollout |

The plugin ID remains `io.dbx.kafka`. Upstream feature PRs do not bump versions or
publish packages. A downstream signed fork release must use a new unique SemVer,
record its actual source SHA and artifact hashes, and pin its update source. Keep
official and fork packages from running simultaneously. Preserve MIT notices and
Apache-2.0 notices for any reused Kafbat components. No Kafbat source is copied in
the first Connect contribution; the REST adapter is implemented in Go.
