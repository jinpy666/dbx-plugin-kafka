# Kafka Connect

The Kafka workbench includes a native Connect panel. The Go sidecar calls the
worker REST API; the browser receives neither Connect authentication credentials
nor a direct worker URL to execute requests against. No Kafbat service is required.

## Configure a saved Kafka connection

Add an optional `connect_clusters` JSON array in the DBX connection form:

```json
[
  {"name": "primary", "url": "https://connect.example.test", "auth": "basic"},
  {"name": "secondary", "url": "http://localhost:8083", "auth": "none"}
]
```

Names must be unique (letters, digits, dots, underscores and hyphens), at most
128 characters. At most 16 clusters are supported. Reverse-proxy URL base paths
are preserved. URLs must not include credentials, query strings or fragments.

The separate `connect_credentials` field uses DBX **secret binding**. It accepts
a JSON object keyed by the same cluster names. Each value can contain `username`,
`password`, `bearerToken`, `tlsCaCert`, `tlsClientCert` and `tlsClientKey`.
Certificate and key values contain PEM text, not workstation file paths. Basic
authentication requires username/password; bearer authentication requires a token;
mutual TLS requires both certificate and private key. Keep real values out of Git.

Reconnect after editing the saved connection. Old connections without these fields
continue to work and display a setup hint in the Connect panel. Saved cluster
configuration belongs to the host; the workbench does not keep another copy.
HTTP connections follow the host's SOCKS5 route when one exists. TLS hostname
verification remains enabled; redirects are rejected, including same-host redirects.
Environment HTTP proxy settings are deliberately ignored.

## Workflows

- Select a Connect cluster, test its version/cluster ID, and refresh its connector
  list. One inaccessible connector does not hide healthy connectors.
- Select a connector to inspect its state, type, worker, task states, error classes,
  topics and consumer group. Open topics and groups directly in their DBX panels.
  The Topics toolbar also opens Connect filtered to the selected topic.
- Create a connector using a discovered plugin class, inspect its configuration
  definitions, validate JSON string-valued configuration, and save it. The editor's
  connector name is included in validation because workers require `name`.
- Pause/resume/stop/restart connectors, restart all or failed tasks, and restart an
  individual task. Restart-all/failed use Kafka's `includeTasks`/`onlyFailed` options.
- Stop a connector before resetting its offsets. Delete and reset both require
  typing the exact connector name, write permission and `allow_delete`.

## Safety and compatibility

All mutations enforce the connection's effective read-only policy in the backend,
including the host's standard read-only flag. Unsupported/unavailable operation
endpoints remain visible with an explicit unavailable result; an unavailable
endpoint is never reported as a successful mutation. HTTP 409 reports a state or
rebalance conflict without automatically replaying a write.

Configuration values declared as `PASSWORD` by plugin definitions, plus common
sensitive field names, are replaced by `[REDACTED]` before leaving the sidecar.
If definitions cannot be fetched, all configuration values are masked. Saving an
existing masked value (or omitting a sensitive key) restores its current value on
the server side. An explicitly supplied empty string clears it. New connectors
cannot contain redacted placeholders. Validation of an existing connector also
rehydrates its masked values on the backend.

Worker error bodies and validation messages can echo credentials and are withheld.
Validation exposes field names/types/required flags and error counts. Task traces
expose only a scrubbed Java error class: use restricted worker logs for full stack
traces. Audit records contain action, cluster/connector target and outcome, never
configuration bodies or error text. The editor clears entered values when closed.
These safeguards are not multi-user RBAC; authenticated identity propagation
requires host support and is outside this plugin-only change.

Requests use a 20-second overall budget and a 4 MiB JSON response limit. Connector
lists fan out to at most eight workers at a time and at most 2,000 connectors.
No reconnect or inspection commits Kafka consumer offsets.

The first contribution covers native UI/RPC Connect operations. No new MCP write
tools are published by this change; the existing MCP confirmation model is intact.
New panel labels use the existing seven locales with explicit English fallback
for new operational explanations.

## Verification

```bash
(cd backend && go vet ./... && go test -race ./...)
pnpm --dir frontend typecheck
pnpm --dir frontend test
pnpm --dir frontend build
(cd backend && go build -o bin/dbx-plugin-kafka .)
python3 scripts/smoke_connect.py
```

The acceptance script creates a uniquely named disposable Compose project using
Apache Kafka 4.3.1 for broker and Connect. Ports 39092/38083 are loopback-only and
can be changed using `CONNECT_TEST_KAFKA_PORT`/`CONNECT_TEST_HTTP_PORT`. It exercises
plugin discovery/validation, create/update/inspect, failed task isolation, all
restart modes, read-only/reconnect/confirmation gates, stop/reset/delete. Missing
Docker, plugins or methods fail rather than skip. The project is removed on exit;
no pre-existing container, installed DBX data or production endpoint is touched.

The companion `KAFKA_CONNECT_CI.patch` wires this acceptance gate into CI and release
workflows before packaging. A maintainer with workflow write permission must apply
it with `git apply --unidiff-zero docs/KAFKA_CONNECT_CI.patch`; this contribution
author's OAuth token cannot update workflow files. Until it
is applied, run the acceptance command explicitly before any release.
Separate DBX Web installation, signing, persistence and HEL1 acceptance remain
required before a production rollout; passing sidecar tests does not prove them.
