#!/usr/bin/env python3
"""MCP smoke test for the dbx-kafka-plugin sidecar (M3, scenarios K1-K16).

Covers the shared/IMPL_PLAN_PLUGIN_MCP.zh-CN.md (v2) §1/§2/§3/§4/§6.3 tool
face over the stdio-jsonl protocol (mcp/tools + mcp/call + mcp/settings,
ldap Go skeleton parity), plus the standalone `--mcp` stdio mode
(design §0.2/§5 stdio row, scenarios K12/K13/K16):

    K1  mcp/settings/get + set           -> defaults, partial update, invalid refused
    K2  mcp/tools lists the 11 kafka tools -> names + JSON Schema required fields
    K3  read-only connection tools list  -> all 4 write tools omitted with reasons
    K4  write tool on read-only conn     -> refused (-32000)
    K5  UI intent without a frontend     -> state=pending + digest fallback hint
    K6  UI intent with a report          -> state=applied + summary roundtrip
    K7  kafka_ui_state                   -> by intentId + latest snapshot (+ streams)
    K8  digest/cursor gates              -> unknown connection/cursor/topic refused
    K9  unknown tool / method            -> clear error (-32601 SKIP semantics)
    K10 allow_delete=false               -> only the delete-class tools are omitted
    K11 digest + cursor + produce + two-phase writes + audit source=mcp
        (needs the Kafka test container)
    K12 stdio --mcp: initialize/tools-list/ui UNAVAILABLE/gates (offline)
    K13 stdio --mcp inline credentials: produce/digest/cursor/two-phase delete
        (needs the Kafka test container)
    K14 digest schema mount + JSON-path projection on wire-format data
        (needs the Kafka test container AND a Schema Registry)
    K15 aggregate clamps + cursor paging to the end + reset timestampMs
        landing + produce boundary errors (needs the Kafka test container)
    K16 stdio bridge fallback: fail-closed w/o the DBX app + mock-bridge
        forward contract + 404 surfaced (offline)
    K17 stdio robustness: malformed JSON/UTF-8, notification silence, request
        shape tiering, 8MiB line, pipelining, CRLF — process stays alive
        (offline, 可靠性纵深轮)
    K18 consume pool churn: earliest/latest alternating 50 rounds, matched
        stable (needs the Kafka test container, round-4 P0 regression surface)
    K19 enum invalid values list valid options (offsetStrategy/resetTo) +
        case normalization online (needs the Kafka test container, 第七轮口径拉齐)

SKIP semantics (M0 §5.2, same as smoke_test.py):
  * method/tool not registered  -> SKIP (never FAIL);
  * no sidecar binary           -> whole suite SKIP (FAIL with KAFKA_TEST_REQUIRE=1);
  * K11/K13/K14/K15/K18/K19 additionally SKIP without a reachable Kafka test container.

Usage:
    DBX_PLUGIN_SIDECAR=/path/to/dbx-plugin-kafka python3 scripts/smoke_mcp.py
"""

from __future__ import annotations

import http.server
import json
import os
import select
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sidecar_client_jsonl import (  # noqa: E402
    SidecarClient,
    SidecarError,
    default_binary,
    is_method_not_registered,
    lifecycle_params,
)
from smoke_test import _OPENER, _ensure_local_url  # noqa: E402

HOST = os.environ.get("KAFKA_TEST_HOST", "127.0.0.1")
PORT = int(os.environ.get("KAFKA_TEST_PORT", "9092"))
BOOTSTRAP = f"{HOST}:{PORT}"
REQUIRE = os.environ.get("KAFKA_TEST_REQUIRE", "") == "1"

EXPECTED_TOOLS = [
    "kafka_ui_search",
    "kafka_ui_focus",
    "kafka_ui_select",
    "kafka_ui_state",
    "kafka_ui_topics",
    "kafka_messages_digest",
    "kafka_cursor_next",
    "kafka_messages_produce",
    "kafka_topics_delete",
    "kafka_groups_offsets_reset",
    "kafka_topics_records_clear",
]

WRITE_TOOLS = ["kafka_messages_produce", "kafka_topics_delete", "kafka_groups_offsets_reset", "kafka_topics_records_clear"]
DELETE_CLASS_TOOLS = ["kafka_topics_delete", "kafka_topics_records_clear"]


class SkipScenario(Exception):
    """Raised by a scenario when a precondition is unavailable."""


class ScenarioResult:
    def __init__(self, no: str, name: str, status: str, detail: str = ""):
        self.no = no
        self.name = name
        self.status = status
        self.detail = detail


RESULTS: list[ScenarioResult] = []


def scenario(no: str, name: str):
    def decorate(fn):
        def run(*args, **kwargs):
            try:
                note = fn(*args, **kwargs)
            except SkipScenario as cause:
                RESULTS.append(ScenarioResult(no, name, "SKIP", str(cause)))
            except (SidecarError, AssertionError) as cause:
                if is_method_not_registered(cause):
                    RESULTS.append(ScenarioResult(no, name, "SKIP", f"backend not implemented: {cause}"))
                else:
                    RESULTS.append(ScenarioResult(no, name, "FAIL", str(cause)))
            except Exception as cause:  # unexpected crash counts as FAIL
                RESULTS.append(ScenarioResult(no, name, "FAIL", f"unexpected: {cause}"))
            else:
                RESULTS.append(ScenarioResult(no, name, "PASS", note or ""))
        run.no = no  # type: ignore[attr-defined]
        run.name = name  # type: ignore[attr-defined]
        return run
    return decorate


# -- helpers -------------------------------------------------------------------


def expect_error(fn, markers: tuple[str, ...] = ()) -> SidecarError:
    try:
        fn()
    except SidecarError as cause:
        lowered = str(cause).lower()
        if markers and not any(marker in lowered for marker in markers):
            raise AssertionError(f"error message mismatch: {cause}") from cause
        return cause
    raise AssertionError("expected a sidecar error but the call succeeded")


def unwrap(result: dict) -> dict:
    """mcp/call returns the MCP content envelope; unwrap the JSON text payload."""
    content = result.get("content") or []
    assert content and content[0].get("type") == "text", result
    return json.loads(content[0]["text"])


def call_tool(client: SidecarClient, tool: str, **arguments) -> dict:
    return unwrap(client.request("mcp/call", {"tool": tool, "arguments": arguments}))


def make_connection(connection_id: str, **extra_config) -> dict:
    external = {
        "display_name": f"smoke-mcp-{connection_id}",
        "bootstrap_servers": BOOTSTRAP,
        "security_protocol": "PLAINTEXT",
        "client_id": "dbx-kafka-smoke-mcp",
        "read_only": False,
        "allow_delete": True,
    }
    external.update(extra_config)
    return {
        "id": connection_id,
        "name": f"smoke-mcp-{connection_id}",
        "external_config": external,
        "connection_secrets": {},
    }


def report_intent_applied(message: dict) -> dict | None:
    """on_event hook: answer a kafka/ui/intent notification with a report."""
    if message.get("method") == "kafka/ui/intent":
        intent_id = (message.get("params") or {}).get("intentId", "")
        return {
            "method": "kafka/ui/state/report",
            "params": {
                "intentId": intent_id,
                "status": "applied",
                "summary": {
                    "count": 2,
                    "anchor": "order-events-p0-o42",
                    "rows": [{"partition": 0, "offset": 42}],
                },
            },
        }
    return None


def kafka_reachable() -> tuple[bool, str]:
    try:
        with socket.create_connection((HOST, PORT), timeout=2.0):
            return True, ""
    except OSError as cause:
        return False, f"no Kafka test container at {BOOTSTRAP}: {cause}"


# -- scenarios (offline: no Kafka server needed) --------------------------------


@scenario("K1", "mcp/settings get/set defaults + validation")
def k1_settings(client: SidecarClient) -> str:
    settings = client.request("mcp/settings/get")["settings"]
    assert settings["reportWaitMs"] == 5000, settings
    assert settings["responseLimitBytes"] == 16 * 1024, settings
    assert settings["digestGroupLimit"] == 20 and settings["digestTopN"] == 10, settings
    assert settings["digestScanLimit"] == 1000, settings
    updated = client.request("mcp/settings/set", {"reportWaitMs": 300, "cellWidth": 80})["settings"]
    assert updated["reportWaitMs"] == 300 and updated["cellWidth"] == 80, updated
    expect_error(lambda: client.request("mcp/settings/set", {"reportWaitMs": 0}), ("between 1 and",))
    expect_error(lambda: client.request("mcp/settings/set", {"reportWaitMs": "fast"}), ("positive integer",))
    expect_error(lambda: client.request("mcp/settings/set", {"digestScanLimit": 100001}), ("between 1 and",))
    # 白名单外字段容忍（部分更新语义），当前值不被污染。
    tolerated = client.request("mcp/settings/set", {"unknownField": 1})["settings"]
    assert tolerated["reportWaitMs"] == 300, tolerated
    return "defaults 5s/16KiB; partial update; invalid refused"


@scenario("K2", "mcp/tools lists the 11 kafka tools with schemas")
def k2_tools(client: SidecarClient) -> str:
    tools = client.request("mcp/tools")["tools"]
    names = [tool["name"] for tool in tools]
    for expected in EXPECTED_TOOLS:
        assert expected in names, f"{expected} missing from {names}"
    digest = next(tool for tool in tools if tool["name"] == "kafka_messages_digest")
    schema = digest["inputSchema"]
    assert schema["type"] == "object" and "topic" in schema["properties"], schema
    assert set(schema["required"]) >= {"connectionId", "topic"}, schema
    delete = next(tool for tool in tools if tool["name"] == "kafka_topics_delete")
    assert "confirmToken" in delete["inputSchema"]["properties"], delete
    return f"{len(tools)} tools with JSON Schema"


@scenario("K3", "read-only connection omits all write tools")
def k3_readonly_tools(client: SidecarClient) -> str:
    ro_id = "smoke-mcp-ro"
    client.request("connection/connect", lifecycle_params(make_connection(ro_id, read_only=True)))
    result = client.request("mcp/tools", {"connectionId": ro_id})
    names = [tool["name"] for tool in result["tools"]]
    for write_tool in WRITE_TOOLS:
        assert write_tool not in names, f"{write_tool} must be omitted"
    omitted = {row["name"]: row["reason"] for row in result.get("omittedWriteTools") or []}
    assert set(omitted) == set(WRITE_TOOLS), omitted
    # 全量清单（不带 connectionId）仍包含写工具。
    all_names = [tool["name"] for tool in client.request("mcp/tools")["tools"]]
    for write_tool in WRITE_TOOLS:
        assert write_tool in all_names
    return "4 write tools omitted with a reason for read-only"


@scenario("K4", "write tool refused on a read-only connection")
def k4_readonly_write(client: SidecarClient) -> str:
    ro_id = "smoke-mcp-ro"
    error = expect_error(
        lambda: client.request("mcp/call", {
            "tool": "kafka_messages_produce",
            "arguments": {"connectionId": ro_id, "topic": "t", "value": "v"},
        }),
        ("read-only",),
    )
    assert error.code == -32000, error.code
    return "mcp/call refused before any write"


@scenario("K5", "UI intent pends without a frontend")
def k5_intent_pending(client: SidecarClient) -> str:
    # K1 把 reportWaitMs 调到 300ms：无前端时快速收敛为 pending。
    result = call_tool(client, "kafka_ui_focus", panel="topics")
    assert result["state"] == "pending", result
    assert result["intentId"], result
    assert "kafka_messages_digest" in result.get("hint", ""), result
    # 未知面板在 sidecar 侧提前报明确错误（不等前端 rejected / pending）。
    expect_error(
        lambda: call_tool(client, "kafka_ui_focus", panel="banana"),
        ("panel must be one of",),
    )
    return "pending + digest fallback hint + unknown panel refused"


@scenario("K6", "UI intent applied via the report callback")
def k6_intent_applied(client: SidecarClient) -> str:
    result = unwrap(client.request(
        "mcp/call",
        {"tool": "kafka_ui_search", "arguments": {"topic": "order-events", "limit": 100, "valueFilter": "x"}},
        on_event=report_intent_applied,
    ))
    assert result["state"] == "applied", result
    summary = result.get("summary") or {}
    assert summary.get("count") == 2 and summary.get("anchor"), result
    # 定位参数缺失的工具校验：双缺一次枚举全点名（ssh 同款 §3.9）。
    expect_error(
        lambda: client.request("mcp/call", {"tool": "kafka_ui_select", "arguments": {}}),
        ("missing required parameters: partition, offset",),
    )
    return "applied + summary roundtrip over kafka/ui/state/report"


@scenario("K7", "kafka_ui_state reads intent result and snapshot")
def k7_ui_state(client: SidecarClient) -> str:
    # 定位参数用字符串数字（LLM 常见变体）：sidecar 宽容折算。
    applied = unwrap(client.request(
        "mcp/call",
        {"tool": "kafka_ui_select", "arguments": {"partition": "0", "offset": "42"}},
        on_event=report_intent_applied,
    ))
    assert applied["state"] == "applied", applied
    state = call_tool(client, "kafka_ui_state", intentId=applied["intentId"])
    assert state["state"] == "applied" and state["summary"]["count"] == 2, state
    # 快照型 report 后，无 intentId 的 kafka_ui_state 返回最新快照。
    client.request("kafka/ui/state/report", {"status": "snapshot", "summary": {"panel": "messages", "topic": "order-events", "count": 42}})
    snapshot = call_tool(client, "kafka_ui_state")
    assert snapshot["snapshot"]["panel"] == "messages" and snapshot["snapshot"]["count"] == 42, snapshot
    # connectionId 给出时附带 stream 段（无会话 = 空数组）。
    with_streams = call_tool(client, "kafka_ui_state", connectionId="smoke-mcp-ro")
    assert "streams" in with_streams and isinstance(with_streams["streams"], list), with_streams
    expect_error(
        lambda: call_tool(client, "kafka_ui_state", intentId="i-nonexistent"),
        ("unknown intentid",),
    )
    return "by intentId + latest snapshot + streams segment all work"


@scenario("K8", "digest tools refuse unknown connections/cursors/topics")
def k8_digest_gates(client: SidecarClient) -> str:
    expect_error(
        lambda: call_tool(client, "kafka_messages_digest", connectionId="smoke-mcp-nope", topic="t"),
        ("not connected", "verify the connectionid"),
    )
    expect_error(
        lambda: call_tool(client, "kafka_messages_digest", connectionId="smoke-mcp-ro"),
        ("missing required parameters: topic",),
    )
    # 缺参枚举（ssh 同款 §3.9）：双缺按 schema required 顺序一次全点名。
    expect_error(
        lambda: call_tool(client, "kafka_messages_digest"),
        ("missing required parameters: connectionid, topic",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_messages_digest", connectionId="smoke-mcp-ro", topic="t", format="bogus"),
        ('got "bogus"',),
    )
    expect_error(
        lambda: call_tool(client, "kafka_cursor_next", cursorId="cur-nope"),
        ("unknown cursorid",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_ui_topics"),
        ("missing required parameters: connectionid",),
    )
    # 参数容错红线：非法 partitions 项在连接解析前显式报错（不静默丢弃后
    # 全分区扫描——静默丢弃会改变消费语义）。
    expect_error(
        lambda: call_tool(client, "kafka_messages_digest", connectionId="smoke-mcp-nope", topic="t", partitions=["p0"]),
        ("partitions[0]",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_cursor_next", cursorId="cur-nope", n="many"),
        ("n must be a positive integer",),
    )
    return "unknown connection / cursor / missing topic / bad format / bad partitions all refused"


@scenario("K9", "unknown tool and unknown method")
def k9_unknown(client: SidecarClient) -> str:
    expect_error(
        lambda: client.request("mcp/call", {"tool": "kafka_nonexistent", "arguments": {}}),
        ("unknown tool",),
    )
    try:
        client.request("mcp/nonexistent", {})
        raise AssertionError("expected -32601 for an unregistered method")
    except SidecarError as cause:
        if not is_method_not_registered(cause):
            raise AssertionError(f"expected method-not-registered, got: {cause}") from cause
    return "unknown tool -32000; unknown method -32601"


@scenario("K10", "allow_delete=false omits only the delete-class tools")
def k10_allow_delete(client: SidecarClient) -> str:
    nd_id = "smoke-mcp-nodelete"
    client.request("connection/connect", lifecycle_params(make_connection(nd_id, allow_delete=False)))
    result = client.request("mcp/tools", {"connectionId": nd_id})
    names = [tool["name"] for tool in result["tools"]]
    assert "kafka_messages_produce" in names and "kafka_groups_offsets_reset" in names, names
    for write_tool in DELETE_CLASS_TOOLS:
        assert write_tool not in names, f"{write_tool} must be omitted"
    omitted = {row["name"] for row in result.get("omittedWriteTools") or []}
    assert omitted == set(DELETE_CLASS_TOOLS), omitted
    # offsets reset 预检在预览前拒绝（不签发令牌、不触集群）。
    expect_error(
        lambda: call_tool(client, "kafka_groups_offsets_reset", connectionId=nd_id, group="g", resetTo="timestamp", topics=["t"]),
        ("timestampms is required",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_groups_offsets_reset", connectionId=nd_id, group="g", resetTo="latest"),
        ("topics is required",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_groups_offsets_reset", connectionId=nd_id, group="g", resetTo="partitionOffset"),
        ("partitionoffsets is required",),
    )
    expect_error(
        lambda: call_tool(client, "kafka_groups_offsets_reset", connectionId=nd_id, group="g", resetTo="banana"),
        ("resetto must be",),
    )
    return "delete-class tools omitted; offsets reset modes prevalidated before preview"


# -- container scenario (K11) ----------------------------------------------------


@scenario("K11", "digest + cursor + produce + two-phase writes + audit (container)")
def k11_container(client: SidecarClient, data_dir: str) -> str:
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    topic = f"dbx-smoke-mcp-{run}"
    conn_id = f"smoke-mcp-main-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(conn_id)))
    try:
        client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [topic], "partitions": 2, "replicationFactor": 1})

        # 元发现：topic 名清单（≤50 截断语义）。
        topics = call_tool(client, "kafka_ui_topics", connectionId=conn_id)
        assert topic in topics["topics"], topics

        # 单阶段写：produce（source=mcp 审计）。
        for index in range(3):
            produced = call_tool(client, "kafka_messages_produce", connectionId=conn_id, topic=topic,
                                 key=f"k-{index}", value=json.dumps({"run": run, "v": index}))
            assert produced["success"] is True and produced["partition"] in (0, 1), produced
        # 字符串 partition（LLM 变体）必须生效：消息落在显式分区 1。
        pinned = call_tool(client, "kafka_messages_produce", connectionId=conn_id, topic=topic,
                           key="k-3", value=json.dumps({"run": run, "v": 3}), partition="1")
        assert pinned["success"] is True and pinned["partition"] == 1, pinned

        # digest：本地聚合 + cursor 物化。
        digest = call_tool(client, "kafka_messages_digest", connectionId=conn_id, topic=topic,
                           fields=["$.v"], maxScanRecords=2000)
        assert digest["matched"] == 4 and digest["scanned"] >= 4, digest
        stats = digest["stats"]
        assert sum(stats["perPartition"].values()) == 4, stats
        assert stats["keys"].get("k-0") == 1, stats
        assert stats["timeHistogram"], stats
        assert stats["fields"][0]["field"] == "$.v" and stats["fields"][0]["valueCount"] == 4, stats
        assert len(digest["sample"]) <= 5 and "cursorId" in digest, digest
        cursor_id = digest["cursorId"]

        # 不存在的 topic：明确报错 + kafka_ui_topics 指引（不是静默 matched=0）。
        expect_error(
            lambda: call_tool(client, "kafka_messages_digest", connectionId=conn_id, topic=f"dbx-smoke-mcp-missing-{run}"),
            ("does not exist", "kafka_ui_topics"),
        )

        # cursor 翻页：定位字段行（partition/offset），不重发条件；
        # 第二页用字符串 n（LLM 变体）验证宽容折算。
        page = call_tool(client, "kafka_cursor_next", cursorId=cursor_id, n=2)
        assert len(page["rows"]) == 2 and page["rows"][0]["topic"] == topic, page
        assert "partition" in page["rows"][0] and "offset" in page["rows"][0], page
        page2 = call_tool(client, "kafka_cursor_next", cursorId=cursor_id, n="1")
        # 行序是扫描序（跨分区不保证 offset 单调），只断言批大小与游标推进。
        assert len(page2["rows"]) == 1 and page2["nextOffset"] == page["nextOffset"] + 1, page2

        # 两阶段 delete：preview + confirmToken → 参数改动作废 → 重开预览 → 执行。
        scratch = f"dbx-smoke-mcp-del-{run}"
        client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [scratch], "partitions": 1, "replicationFactor": 1})
        preview = call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[scratch])
        assert preview["preview"]["topics"] == [scratch] and preview.get("confirmToken"), preview
        expect_error(
            lambda: call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[topic], confirmToken=preview["confirmToken"]),
            ("arguments changed",),
        )
        preview2 = call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[scratch])
        confirm = call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[scratch], confirmToken=preview2["confirmToken"])
        assert confirm["success"] is True, confirm
        # 一次性：同 token 复用 → unknown。
        expect_error(
            lambda: call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[scratch], confirmToken=preview2["confirmToken"]),
            ("unknown or already used",),
        )
        deleted_names = [row["topic"] for row in confirm["results"]]
        assert scratch in deleted_names, confirm

        # 两阶段 offsets reset：preview → confirm（字符串 timestampMs 宽容
        # 折算进 canonical 请求——预览不触集群，只断言折算值）。
        group = f"smoke-mcp-g-{run}"
        ts_ms = str(int(time.time() * 1000))
        reset_preview_ts = call_tool(client, "kafka_groups_offsets_reset", connectionId=conn_id, group=group,
                                     topics=[topic], resetTo="timestamp", timestampMs=ts_ms)
        assert reset_preview_ts["preview"]["timestampMs"] == int(ts_ms), reset_preview_ts
        reset_preview = call_tool(client, "kafka_groups_offsets_reset", connectionId=conn_id, group=group,
                                  topics=[topic], resetTo="latest")
        assert reset_preview["preview"]["resetTo"] == "latest", reset_preview
        reset_confirm = call_tool(client, "kafka_groups_offsets_reset", connectionId=conn_id, group=group,
                                  topics=[topic], resetTo="latest", confirmToken=reset_preview["confirmToken"])
        assert reset_confirm["success"] is True and reset_confirm["rows"], reset_confirm

        # 两阶段 records clear：preview → confirm → 清空后 digest matched=0。
        clear_preview = call_tool(client, "kafka_topics_records_clear", connectionId=conn_id, topic=topic)
        assert clear_preview["preview"]["topic"] == topic, clear_preview
        clear_confirm = call_tool(client, "kafka_topics_records_clear", connectionId=conn_id, topic=topic,
                                  confirmToken=clear_preview["confirmToken"])
        assert clear_confirm["success"] is True, clear_confirm
        after = call_tool(client, "kafka_messages_digest", connectionId=conn_id, topic=topic, maxScanRecords=2000)
        assert after["matched"] == 0, after

        # 审计：MCP 写路径 source=mcp（audit.jsonl 与 kafka/audit 事件同条）。
        audit_path = os.path.join(data_dir, "audit.jsonl")
        sources = {}
        with open(audit_path, encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                if record.get("connectionId") == conn_id:
                    sources.setdefault(record["action"], set()).add(record.get("source"))
        # audit.jsonl 的 action 带 kafka/ 前缀（main 层 auditAction）。
        assert "mcp" in sources.get("kafka/produce", set()), sources
        assert "mcp" in sources.get("kafka/topics-delete", set()), sources
        assert "mcp" in sources.get("kafka/group-offsets-reset", set()), sources
        assert "mcp" in sources.get("kafka/topics.records.clear", set()), sources
        # 工作台路径（topics/create 走领域方法）不带 source 字段。
        assert None in sources.get("kafka/topics-create", set()), sources
        return f"digest matched=4 (string partition pinned) -> cleared; two-phase delete/reset/clear executed; audit source=mcp"
    finally:
        try:
            client.request("connection/disconnect", {"connection": {"id": conn_id}})
        except Exception:
            pass


# -- schema registry helpers (K14) -----------------------------------------------


def sr_base_url() -> str:
    return os.environ.get("KAFKA_TEST_SR_URL", "http://127.0.0.1:19081")


def schema_registry_reachable() -> tuple[bool, str]:
    import urllib.request
    url = sr_base_url()
    # 与 smoke_test 同款 SSRF 守卫：仅 http(s) 且解析到环回/私网才放行，
    # 且不跟随重定向。
    _ensure_local_url(url)
    try:
        with _OPENER.open(url + "/subjects", timeout=3) as resp:
            resp.read()
        return True, ""
    except OSError as cause:
        return False, f"no Schema Registry at {url}: {cause}"


def sr_register_json_schema(subject: str) -> dict:
    """Register a deterministic JSON schema (idempotent per subject version 1)."""
    import urllib.request
    schema = json.dumps({
        "type": "object",
        "properties": {
            "user": {"type": "object", "properties": {"id": {"type": "string"}}},
            "region": {"type": "string"},
        },
    })
    url = f"{sr_base_url()}/subjects/{subject}/versions"
    _ensure_local_url(url)
    request = urllib.request.Request(
        url,
        data=json.dumps({"schemaType": "JSON", "schema": schema}).encode(),
        method="POST",
    )
    request.add_header("Content-Type", "application/vnd.schemaregistry.v1+json")
    with _OPENER.open(request, timeout=10) as resp:
        return json.loads(resp.read().decode())


@scenario("K14", "digest schema mount + projection on wire-format data (container+SR)")
def k14_schema_digest(client: SidecarClient) -> str:
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    ok, reason = schema_registry_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    topic = f"dbx-smoke-mcp-schema-{run}"
    subject = f"{topic}-value"
    sr_register_json_schema(subject)
    conn_id = f"smoke-mcp-sr-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(
        conn_id, schema_registry="confluent", sr_url=sr_base_url())))
    plain_id = f"smoke-mcp-nosr-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(plain_id)))
    try:
        client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [topic],
                                               "partitions": 2, "replicationFactor": 1})
        regions = ["eu", "us", "ap", "eu", "us", "eu"]
        for index, region in enumerate(regions):
            produced = call_tool(client, "kafka_messages_produce", connectionId=conn_id,
                                 topic=topic, key=f"k-{index}", partition="0",
                                 value=json.dumps({"user": {"id": f"u{index % 2}"}, "region": region}),
                                 schema={"subject": subject})
            assert produced["success"] is True, produced

        # 无 schema 挂载：raw 通道（wire 字节原样，matched 照常计数）。
        raw_digest = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                               topic=topic, maxScanRecords=500)
        assert raw_digest["matched"] == len(regions), raw_digest
        assert raw_digest["sample"][0]["value"].startswith("\x00\x00\x00\x00"), raw_digest["sample"][0]

        # schema 挂载 + JSON path 投影：解码后 distinct/topN 命中真实值。
        digest = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                           topic=topic, maxScanRecords=500,
                           schema={"subject": subject},
                           fields=["$.user.id", "$.region", "$.missing.path"])
        assert digest["matched"] == len(regions), digest
        sample = digest["sample"][0]
        assert json.loads(sample["value"])["region"] in ("eu", "us", "ap"), sample
        assert sample.get("schemaSubject") == subject, sample
        field_stats = {row["field"]: row for row in digest["stats"]["fields"]}
        assert field_stats["$.user.id"]["valueCount"] == 2, field_stats
        assert field_stats["$.user.id"]["values"].get("u0") == 3, field_stats
        assert field_stats["$.region"]["values"].get("eu") == 3, field_stats
        assert field_stats["$.missing.path"]["valueCount"] == 0, field_stats
        # 投影至少部分命中：无 fieldsNote（全 0 才提示）。
        assert "fieldsNote" not in digest, digest.get("fieldsNote")

        # 投影字段全不命中（无 schema 时 raw 字节非 JSON）→ fieldsNote 指引。
        zero = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                         topic=topic, maxScanRecords=500, fields=["$.user.id"])
        assert "fieldsNote" in zero and "matched 0 values" in zero["fieldsNote"], zero

        # 坏 schema 版本：逐条 decodeError 可见（计数 + 指引），不静默吞掉。
        bad = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                        topic=topic, maxScanRecords=500,
                        schema={"subject": subject, "version": 999})
        assert bad.get("decodeFailures") == len(regions), bad
        assert "decodeNote" in bad and "schema.subject/version" in bad["decodeNote"], bad
        assert "decodeError" in bad["sample"][0] and "404" in bad["sample"][0]["decodeError"], bad["sample"][0]

        # 未配置 SR 的连接挂 schema：门禁显式报错（schema 参数不静默丢弃）。
        expect_error(
            lambda: call_tool(client, "kafka_messages_digest", connectionId=plain_id,
                              topic=topic, schema={"subject": subject}),
            ("schema registry is not enabled",),
        )
        return (f"raw passthrough + schema decode projection ($.user.id distinct=2, top u0=3) + "
                f"zero-projection note + bad-version decodeFailures + SR gate")
    finally:
        for connection in (conn_id, plain_id):
            try:
                client.request("connection/disconnect", {"connection": {"id": connection}})
            except Exception:
                pass


@scenario("K15", "aggregate clamps + cursor paging + reset timestamp landing (container)")
def k15_aggregate_reset(client: SidecarClient) -> str:
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    conn_id = f"smoke-mcp-agg-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(conn_id)))
    try:
        def tool(name, **arguments):
            return call_tool(client, name, **arguments)

        # -- 聚合 clamp（真实多分区/多 key 数据）--
        topic = f"dbx-smoke-mcp-agg-{run}"
        client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [topic],
                                               "partitions": 4, "replicationFactor": 1})
        total = 45  # 45 distinct keys > 20 组上限；45 distinct 值 > topN 10
        for index in range(total):
            tool("kafka_messages_produce", connectionId=conn_id, topic=topic,
                 key=f"key-{index:02d}", value=json.dumps({"n": index, "v": f"value-{index:02d}"}))
        digest = tool("kafka_messages_digest", connectionId=conn_id, topic=topic,
                      fields=["$.v"], maxScanRecords=2000)
        assert digest["matched"] == total, digest["matched"]
        stats = digest["stats"]
        assert len(stats["keys"]) == 20 and stats.get("keysLimit") is True, stats["keys"]
        assert sum(stats["perPartition"].values()) == total, stats["perPartition"]
        histogram = stats["timeHistogram"]
        assert histogram and len(histogram["buckets"]) <= 12, histogram
        field_stats = stats["fields"][0]
        assert field_stats["valueCount"] == total and len(field_stats["values"]) == 10 \
            and field_stats["truncated"] is True, field_stats
        assert digest["cursorTruncated"] is False, digest

        # -- cursor 翻页到物化尽头（45 行 = 20 + 20 + 5，done 收口）--
        cursor_id = digest["cursorId"]
        seen = 0
        for _ in range(4):
            page = tool("kafka_cursor_next", cursorId=cursor_id)
            seen += len(page["rows"])
            if page["done"]:
                break
        assert seen == total and page["done"] and page["nextOffset"] == total, (seen, page)

        # -- reset timestampMs 落点核验（真实两批消息 + 分界时间戳）--
        reset_topic = f"dbx-smoke-mcp-reset-{run}"
        client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [reset_topic],
                                               "partitions": 1, "replicationFactor": 1})
        first_batch = 6
        for index in range(first_batch):
            tool("kafka_messages_produce", connectionId=conn_id, topic=reset_topic,
                 partition="0", key=f"a-{index}", value=f"batch1-{index}")
        time.sleep(0.5)
        cut_ms = int(time.time() * 1000)
        time.sleep(0.3)
        for index in range(4):
            tool("kafka_messages_produce", connectionId=conn_id, topic=reset_topic,
                 partition="0", key=f"b-{index}", value=f"batch2-{index}")
        group = f"smoke-mcp-reset-{run}"
        preview = tool("kafka_groups_offsets_reset", connectionId=conn_id, group=group,
                       topics=[reset_topic], resetTo="timestamp", timestampMs=str(cut_ms))
        assert preview["preview"]["timestampMs"] == cut_ms, preview
        confirm = tool("kafka_groups_offsets_reset", connectionId=conn_id, group=group,
                       topics=[reset_topic], resetTo="timestamp", timestampMs=str(cut_ms),
                       confirmToken=preview["confirmToken"])
        assert confirm["success"] is True, confirm
        offsets = client.request("kafka/groups/offsets/list", {
            "connectionId": conn_id, "group": group, "topics": [reset_topic]})
        rows = {row["partition"]: row["committedOffset"] for row in offsets["rows"]}
        # 落点 = 分界时间戳后第一条消息的 offset（broker ListOffsetsAfterMilli 语义）。
        assert rows.get(0) == first_batch, (rows, first_batch)

        # -- produce 边界矩阵（真集群错误文本）--
        expect_error(
            lambda: tool("kafka_messages_produce", connectionId=conn_id, topic=topic,
                         value="x", partition=99),
            ("partition", "99"),
        )
        expect_error(
            lambda: tool("kafka_messages_produce", connectionId=conn_id,
                         topic=f"dbx-smoke-mcp-missing-{run}", value="x"),
            ("unknown_topic_or_partition", "kafka_ui_topics"),
        )
        return (f"clamps (keys 45->20 keysLimit, topN 45->10 truncated, histogram<=12) + "
                f"cursor paged to done + reset timestampMs landed p0@{first_batch} + boundary errors")
    finally:
        try:
            client.request("connection/disconnect", {"connection": {"id": conn_id}})
        except Exception:
            pass


# -- stdio mode (K12/K13): standalone `--mcp` newline-delimited JSON-RPC ------


class McpStdioClient:
    """Drive the sidecar in standalone `--mcp` stdio MCP mode.

    MCP 2024-11-05: one JSON-RPC 2.0 message per line on stdin/stdout.
    Tool execution errors arrive as results with isError=true (never raised);
    protocol-level errors (unknown method, parse error) raise SidecarError.
    """

    def __init__(self, process: subprocess.Popen, timeout: float = 30.0):
        self.process = process
        self.timeout = timeout
        self.next_id = 100
        # 响应可能乱序写出（逐请求 goroutine）：不匹配当前等待 id 的帧先进
        # 缓冲，绝不丢弃——丢弃会让后续 read_frame 永远等不到（K17 挂死根因）。
        self.pending: list[dict] = []
        # os.read 的原始字节缓冲（_read_line 自行分行）：BufferedReader 的
        # 用户态缓冲会让 select 漏报就绪——表现成"响应丢失"假超时。
        self._rawbuf = b""

    @classmethod
    def start(cls, binary: str, data_dir: str, timeout: float = 30.0,
              extra_env: dict | None = None) -> "McpStdioClient":
        env = dict(os.environ)
        env["DBX_PLUGIN_DATA_DIR"] = data_dir
        env.update(extra_env or {})
        process = subprocess.Popen(
            [binary, "--mcp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        return cls(process, timeout)

    def _send(self, message: dict) -> None:
        assert self.process.stdin
        self.process.stdin.write((json.dumps(message, ensure_ascii=False) + "\n").encode())
        self.process.stdin.flush()

    def _read_line(self, deadline: float) -> bytes:
        """One stdout line with a real mid-read deadline.

        Two traps avoided here: (1) plain readline() would block forever when
        the sidecar stops answering (the suite timeout only fires between
        reads); (2) select() on the BufferedReader reports "not ready" when a
        previous readline already pulled later frames into the userspace
        buffer — the data is there but the pipe is empty, which looks exactly
        like a lost response (K17 flake root cause, ldap M18 同源). So:
        os.read + our own line buffer, never the buffered reader.

        On Windows select() only accepts sockets, so the raw read runs in a
        daemon thread and the deadline is enforced on the queue wait (same
        pattern as sidecar_client_jsonl._read_line).
        """
        while True:
            index = self._rawbuf.find(b"\n")
            if index >= 0:
                line = self._rawbuf[:index + 1]
                self._rawbuf = self._rawbuf[index + 1:]
                return line
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SidecarError("timeout waiting for MCP stdio response")
            if os.name == "nt":
                import queue as _queue

                chunks: "_queue.Queue[bytes]" = _queue.Queue()

                def _reader() -> None:
                    try:
                        chunks.put(os.read(self.process.stdout.fileno(), 65536))
                    except (OSError, ValueError):
                        chunks.put(b"")

                threading.Thread(target=_reader, daemon=True).start()
                try:
                    chunk = chunks.get(timeout=remaining)
                except _queue.Empty:
                    raise SidecarError("timeout waiting for MCP stdio response")
            else:
                ready, _, _ = select.select([self.process.stdout.fileno()], [], [], remaining)
                if not ready:
                    raise SidecarError("timeout waiting for MCP stdio response")
                chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise SidecarError("MCP stdio sidecar closed stdout")
            self._rawbuf += chunk

    def _next_frame(self, request_id, deadline: float) -> dict:
        """Next response frame whose id matches (None = id null). True JSON-RPC
        notifications (no id key) never answer anything and are dropped; every
        other non-matching frame is buffered for the caller that awaits it."""
        while True:
            for index, message in enumerate(self.pending):
                if message.get("id") == request_id:
                    return self.pending.pop(index)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SidecarError("timeout waiting for MCP stdio response")
            line = self._read_line(deadline)
            if not line:
                raise SidecarError("MCP stdio sidecar closed stdout")
            text = line.decode(errors="replace").strip()
            if not text:
                continue
            message = json.loads(text)
            if "id" not in message:
                continue  # notification: silent by protocol
            self.pending.append(message)

    def request(self, method: str, params: dict | None = None, timeout: float | None = None) -> dict:
        if timeout:
            previous, self.timeout = self.timeout, timeout
        self.next_id += 1
        request_id = self.next_id
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})
        try:
            deadline = time.monotonic() + self.timeout
            while True:
                message = self._next_frame(request_id, deadline)
                if message.get("error") is not None:
                    error = message["error"]
                    raise SidecarError(error.get("message", ""), error.get("code"))
                return message.get("result") or {}
        finally:
            if timeout:
                self.timeout = previous

    def notify(self, method: str, params: dict | None = None) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def call_tool(self, name: str, **arguments) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})

    # -- 可靠性纵深段（K17）用的低层原语 -------------------------------

    def send_raw(self, raw: bytes) -> None:
        """Send a pre-encoded stdin line verbatim (malformed JSON, CRLF, ...)."""
        assert self.process.stdin
        self.process.stdin.write(raw)
        self.process.stdin.flush()

    def read_frame(self, request_id, timeout: float | None = None) -> dict:
        """Read the next full JSON-RPC frame whose id matches (None = id null).
        Out-of-order frames are buffered (never discarded) — see pending."""
        if timeout:
            previous, self.timeout = self.timeout, timeout
        try:
            return self._next_frame(request_id, time.monotonic() + self.timeout)
        finally:
            if timeout:
                self.timeout = previous

    def alive(self) -> bool:
        """The sidecar process must keep serving (never crash on bad input)."""
        return self.process.poll() is None

    def close(self) -> None:
        try:
            if self.process.stdin:
                self.process.stdin.close()
            self.process.wait(timeout=5)
        except Exception:
            self.process.kill()


def unwrap_stdio(result: dict) -> dict:
    """Unwrap a successful tools/call content envelope into the JSON payload."""
    content = result.get("content") or []
    assert content and content[0].get("type") == "text", result
    assert result.get("isError") is not True, result
    return json.loads(content[0]["text"])


def expect_tool_error(result: dict, markers: tuple[str, ...]) -> str:
    assert result.get("isError") is True, result
    text = result["content"][0]["text"]
    for marker in markers:
        assert marker in text, f"{marker!r} missing from: {text}"
    return text


@scenario("K12", "stdio --mcp: initialize/tools/UNAVAILABLE/gates (offline)")
def k12_stdio_offline() -> str:
    data_dir = tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-")
    client = McpStdioClient.start(default_binary(), data_dir)
    try:
        init = client.request("initialize", {"protocolVersion": "2024-11-05"})
        assert init["protocolVersion"] == "2024-11-05", init
        # MCP 2026-07-28 一致性：成功结果统一带 resultType；受支持的请求
        # 版本原样回显；server/discover 列出受支持集合。
        assert init["resultType"] == "complete", init
        assert init["serverInfo"]["name"] == "io.dbx.kafka", init
        assert init["serverInfo"]["version"], init
        negotiated = client.request("initialize", {"protocolVersion": "2026-07-28", "capabilities": {}})
        assert negotiated["protocolVersion"] == "2026-07-28", negotiated
        discover = client.request("server/discover")
        assert discover["supportedVersions"] == ["2026-07-28", "2024-11-05"], discover
        assert "tools" in discover["capabilities"], discover
        # notifications/initialized 不回包：下一条响应必须属于紧随其后的 ping。
        client.notify("notifications/initialized")
        assert client.request("ping") == {"resultType": "complete"}, "ping after notification"
        # tools/list：11 工具照常列出；连接类工具补内联凭据声明并放宽 required。
        listing = client.request("tools/list")
        # 2026-07-28 缓存信封：ttlMs/cacheScope 必填（严格校验、无兜底）；
        # 0/private = 永不缓存（连接态会带外变化）。
        assert listing["ttlMs"] == 0 and listing["cacheScope"] == "private", listing
        tools = listing["tools"]
        names = [tool["name"] for tool in tools]
        assert len(names) == 11 and set(names) == set(EXPECTED_TOOLS), names
        digest = next(tool for tool in tools if tool["name"] == "kafka_messages_digest")
        schema = digest["inputSchema"]
        for key in ("brokers", "securityProtocol", "saslMechanism", "saslPassword",
                    "schemaRegistry", "schemaRegistryUrl", "schemaRegistryPassword", "readOnly"):
            assert key in schema["properties"], key
        assert "connectionId" not in schema["required"], schema
        # UI 类工具（含元发现 kafka_ui_topics）照常列出但 UNAVAILABLE，不假死。
        unavailable = client.call_tool("kafka_ui_focus", panel="topics")
        expect_tool_error(unavailable, ("UNAVAILABLE", "此工具需要 DBX 工作台"))
        expect_tool_error(client.call_tool("kafka_ui_topics"), ("UNAVAILABLE",))
        # 连接解析门：缺参引导（未知 connectionId 的桥兜底 fail-closed /
        # mock 桥转发在 K16 专场景）。
        expect_tool_error(
            client.call_tool("kafka_messages_digest", topic="t"),
            ("brokers (required)",),
        )
        # 字符串布尔：宽变体（yes/no/on/off）被解析接受（不再报布尔校验错，
        # 后续错误只会是业务/连接层）；非法值仍显式报错——静默回落表单默认
        # 会翻转读写语义。
        variant = client.call_tool("kafka_messages_digest", brokers=[BOOTSTRAP],
                                   securityProtocol="PLAINTEXT", readOnly="no",
                                   allowDelete="off", topic="t")
        assert "must be true or false" not in variant["content"][0]["text"], variant
        expect_tool_error(
            client.call_tool("kafka_messages_digest", brokers=[BOOTSTRAP], securityProtocol="PLAINTEXT",
                             readOnly="maybe", topic="t"),
            ("must be true or false",),
        )
        # 未知方法返回 JSON-RPC 错误不崩。
        try:
            client.request("mcp/nonexistent", {})
            raise AssertionError("expected -32601 for an unknown method")
        except SidecarError as cause:
            assert cause.code == -32601, cause
        return "initialize + tools-list(11 + inline schema) + ui UNAVAILABLE + gates"
    finally:
        client.close()


@scenario("K13", "stdio --mcp inline credentials: produce/digest/cursor/two-phase delete (container)")
def k13_stdio_container() -> str:
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    scratch = f"dbx-smoke-stdio-{run}"
    # stdio 工具面没有 topic/create：scratch topic 经工作台协议（不带 --mcp）
    # 预建，stdio 侧只做 MCP 工具流。
    setup_dir = tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-setup-")
    setup = SidecarClient.start(data_dir=setup_dir)
    try:
        setup.initialize()
        setup.request("connection/connect", lifecycle_params(make_connection(f"stdio-setup-{run}")))
        setup.request("kafka/topics/create", {"connectionId": f"stdio-setup-{run}",
                                              "topics": [scratch], "partitions": 1, "replicationFactor": 1})
    finally:
        try:
            setup.request("connection/disconnect", {"connection": {"id": f"stdio-setup-{run}"}})
        except Exception:
            pass
        setup.close()

    data_dir = tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-")
    client = McpStdioClient.start(default_binary(), data_dir)
    # 读路径用表单默认（readOnly 缺省 true）；写路径显式 readOnly=False +
    # allowDelete=True（两组参数各自池化为独立连接）。
    inline_read = dict(brokers=[BOOTSTRAP], securityProtocol="PLAINTEXT", clientId="dbx-kafka-smoke-stdio")
    inline_write = dict(inline_read, readOnly=False, allowDelete=True)
    try:
        # 单阶段写：produce（内联凭据，审计 source=mcp）。
        for index in range(2):
            produced = unwrap_stdio(client.call_tool(
                "kafka_messages_produce", **inline_write, topic=scratch,
                key=f"k-{index}", value=json.dumps({"run": run, "v": index})))
            assert produced["success"] is True and produced["partition"] == 0, produced

        # digest + cursor（本地聚合与翻页，消息体不出 sidecar）。
        digest = unwrap_stdio(client.call_tool(
            "kafka_messages_digest", **inline_read, topic=scratch, maxScanRecords=2000))
        assert digest["matched"] == 2 and digest["cursorId"], digest
        page = unwrap_stdio(client.call_tool("kafka_cursor_next", cursorId=digest["cursorId"], n=1))
        assert len(page["rows"]) == 1 and page["rows"][0]["topic"] == scratch, page

        # 两阶段 delete：preview → confirm → token 一次性（确认执行删真实 topic）。
        preview = unwrap_stdio(client.call_tool("kafka_topics_delete", **inline_write, topics=[scratch]))
        assert preview["preview"]["topics"] == [scratch] and preview.get("confirmToken"), preview
        confirm = unwrap_stdio(client.call_tool(
            "kafka_topics_delete", **inline_write, topics=[scratch],
            confirmToken=preview["confirmToken"]))
        assert confirm["success"] is True, confirm
        expect_tool_error(
            client.call_tool("kafka_topics_delete", **inline_write, topics=[scratch],
                             confirmToken=preview["confirmToken"]),
            ("unknown or already used",),
        )
        # 审计：stdio 模式写路径照常落 audit.jsonl（source=mcp）。
        audit_path = os.path.join(data_dir, "audit.jsonl")
        sources = {}
        with open(audit_path, encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                if record.get("source") == "mcp":
                    sources.setdefault(record["action"], 0)
                    sources[record["action"]] += 1
        assert "kafka/produce" in sources and "kafka/topics-delete" in sources, sources
        return f"stdio produce=2 -> digest matched=2 -> two-phase delete executed; audit source=mcp"
    finally:
        client.close()


# -- stdio robustness (K17, 可靠性纵深轮): real-process adversarial protocol
#    input against the standalone `--mcp` server -----------------------------


@scenario("K17", "stdio robustness: malformed protocol input stays alive (offline)")
def k17_stdio_robustness() -> str:
    data_dir = tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-")
    client = McpStdioClient.start(default_binary(), data_dir, timeout=45)
    try:
        client.request("initialize", {"protocolVersion": "2024-11-05"})
        # 1) 非法 JSON / 非法 UTF-8 字节流 → -32700（id null），进程存活。
        for name, raw in (
            ("garbage", b"not json\n"),
            ("truncated", b'{"jsonrpc":"2.0","id":1,"method":\n'),
            ("invalid-utf8", b"\xff\xfe\x7b\x7d\n"),
        ):
            client.send_raw(raw)
            frame = client.read_frame(None)
            assert frame["error"]["code"] == -32700 and frame["id"] is None, (name, frame)
        assert client.alive() and client.request("ping") == {"resultType": "complete"}, "alive after malformed lines"

        # 2) notification（无 id，含未知通知名）→ 不响应，流不错位。
        client.send_raw(b'{"jsonrpc":"2.0","method":"notifications/unknown/x"}\n')
        assert client.request("ping") == {"resultType": "complete"}, "notification must not corrupt the stream"

        # 3) 请求形状分档（MCP_ACCEPTANCE §2）：缺 id / id 为 object /
        #    method 缺失或非字符串 / jsonrpc 版本非法 → -32600 结构化报错。
        #    注意 read_frame 按各自回包 id 取帧：缺 id/id 非法回 null id，
        #    其余回显请求 id。
        for name, line, frame_id in (
            ("missing-id", '{"jsonrpc":"2.0","method":"ping"}', None),
            ("id-object", '{"jsonrpc":"2.0","id":{"a":1},"method":"ping"}', None),
            ("missing-method", '{"jsonrpc":"2.0","id":7}', 7),
            ("method-number", '{"jsonrpc":"2.0","id":8,"method":42}', 8),
            ("bad-version", '{"jsonrpc":"1.0","id":9,"method":"ping"}', 9),
        ):
            client.send_raw(line.encode() + b"\n")
            frame = client.read_frame(frame_id)
            assert frame["error"]["code"] == -32600, (name, frame)
            if frame_id is not None:
                assert frame["id"] == frame_id, (name, frame)
        assert client.request("ping") == {"resultType": "complete"}, "alive after invalid request shapes"

        # 4) 8 MiB 单行 arguments → 优雅工具层报错（连接门引导），不 panic 不挂死。
        huge = "x" * (8 * 1024 * 1024)
        client.next_id += 1
        big_id = client.next_id
        client.send_raw(json.dumps({
            "jsonrpc": "2.0", "id": big_id, "method": "tools/call",
            "params": {"name": "kafka_messages_digest",
                       "arguments": {"topic": f"t{huge}"}}}).encode() + b"\n")
        frame = client.read_frame(big_id)
        assert "error" not in frame and frame["result"]["isError"] is True, frame
        assert client.request("ping") == {"resultType": "complete"}, "alive after the 8MiB line"

        # 5) pipelining：不等待响应连发 3 个不同 id 请求 → 一一对应。
        client.next_id += 1
        ids = (client.next_id, client.next_id + 1, client.next_id + 2)
        client.send_raw(json.dumps({"jsonrpc": "2.0", "id": ids[0], "method": "ping"}).encode() + b"\n")
        client.send_raw(json.dumps({"jsonrpc": "2.0", "id": ids[1], "method": "mcp/nope"}).encode() + b"\n")
        client.send_raw(json.dumps({"jsonrpc": "2.0", "id": ids[2], "method": "ping"}).encode() + b"\n")
        seen = set()
        for request_id in ids:
            frame = client.read_frame(request_id)
            if "error" in frame:
                assert frame["error"]["code"] == -32601, frame
            seen.add(frame["id"])
        assert seen == set(ids), (seen, ids)
        assert client.request("ping") == {"resultType": "complete"}, "alive after pipelining"

        # 6) 空行 / CRLF 容忍。
        client.send_raw(b"\r\n\n   \r\n")
        assert client.request("ping") == {"resultType": "complete"}, "blank/CRLF lines tolerated"
        assert client.alive(), "process must survive the whole flood"
        return ("malformed bytes -32700; notifications silent; shapes -32600; 8MiB line; "
                "pipelining 3 ids; CRLF — alive throughout")
    finally:
        client.close()


# -- consume pool churn (K18, 可靠性纵深轮): earliest/latest alternating
#    digests on the reuse pool — the round-4 P0 fix regression surface ------


@scenario("K18", "consume pool churn: earliest/latest alternating 50 rounds (container)")
def k18_consume_pool_churn(client: SidecarClient) -> str:
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    topic = f"dbx-smoke-pool-{run}"
    conn_id = f"smoke-mcp-pool-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(conn_id)))
    client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [topic],
                                           "partitions": 1, "replicationFactor": 1})
    try:
        # 造数：10 条唯一消息。
        for index in range(10):
            produced = call_tool(client, "kafka_messages_produce", connectionId=conn_id,
                                 topic=topic, key=f"k-{index}", value=json.dumps({"n": index}))
            assert produced["success"] is True, produced

        # earliest/latest 交替 50 轮：earliest 恒 matched=10（池复用 reset
        # 回起点——第四轮 P0 修复的回归面），latest 恒 matched=0。
        for round_index in range(50):
            strategy = "earliest" if round_index % 2 == 0 else "latest"
            digest = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                               topic=topic, offsetStrategy=strategy)
            want = 10 if strategy == "earliest" else 0
            assert digest["matched"] == want, (round_index, strategy, digest["matched"], digest["scanned"])
        # 收尾：删掉 churn topic（两阶段，保持 dev 集群整洁）。
        preview = call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[topic])
        call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[topic],
                  confirmToken=preview["confirmToken"])
        return "50 alternating rounds: earliest matched=10 stable, latest matched=0 (pool reuse resets)"
    finally:
        try:
            client.request("connection/disconnect", {"connection": {"id": conn_id}})
        except Exception:
            pass


@scenario("K19", "enum invalid values list valid options + case normalization (container)")
def k19_enum_online(client: SidecarClient) -> str:
    """第七轮（契约 §3.3 在线钉桩）：带真实连接的调用里 enum 参数传非法值，
    报错必须列出 schema enum 全部合法值；大小写归一在线生效（EARLIEST 正常
    消费）。离线探针（K10）够不到的在线段在这里补齐。"""
    ok, reason = kafka_reachable()
    if not ok:
        raise SkipScenario(reason)
    run = uuid.uuid4().hex[:6]
    topic = f"dbx-smoke-enum-{run}"
    conn_id = f"smoke-mcp-enum-{run}"
    client.request("connection/connect", lifecycle_params(make_connection(conn_id)))
    client.request("kafka/topics/create", {"connectionId": conn_id, "topics": [topic],
                                           "partitions": 1, "replicationFactor": 1})
    try:
        produced = call_tool(client, "kafka_messages_produce", connectionId=conn_id,
                             topic=topic, key="k", value=json.dumps({"n": 1}))
        assert produced["success"] is True, produced

        # offsetStrategy="bogus"：报错列出 schema enum 全部合法值。
        strategy_error = expect_error(
            lambda: call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                              topic=topic, offsetStrategy="bogus"),
            ("offsetstrategy must be latest, recent, earliest, committed, timestamp, or offset",),
        )
        for option in ("latest", "recent", "earliest", "committed", "timestamp", "offset"):
            assert option in str(strategy_error), (option, strategy_error)

        # resetTo="bogus"：报错列出 schema enum 全部合法值（带实际值）。
        reset_error = expect_error(
            lambda: call_tool(client, "kafka_groups_offsets_reset", connectionId=conn_id,
                              group="g", resetTo="bogus"),
            ("resetto must be earliest, latest, timestamp, or partitionoffset",),
        )
        for option in ("earliest", "latest", "timestamp", "partitionoffset"):
            assert option in str(reset_error).lower(), (option, reset_error)
        assert "bogus" in str(reset_error), reset_error

        # 大小写归一在线钉一组：offsetStrategy="EARLIEST" 正常消费。
        digest = call_tool(client, "kafka_messages_digest", connectionId=conn_id,
                           topic=topic, offsetStrategy="EARLIEST")
        assert digest["matched"] >= 1, digest

        # 收尾：删掉 enum topic（两阶段，保持 dev 集群整洁）。
        preview = call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[topic])
        call_tool(client, "kafka_topics_delete", connectionId=conn_id, topics=[topic],
                  confirmToken=preview["confirmToken"])
        return "offsetStrategy/resetTo enum errors list options; EARLIEST normalized (matched>=1)"
    finally:
        try:
            client.request("connection/disconnect", {"connection": {"id": conn_id}})
        except Exception:
            pass


# -- stdio bridge fallback (K16, design §5 stdio row; ldap M14 同款分工) ------


class MockBridge:
    """Local mock of the DBX app's `/call-plugin-tool` TCP bridge.

    Minimal host-contract implementation: publishes its port in
    `<app_data_dir>/mcp-bridge-port`, records every request (path + snake_case
    body), and replies with a configurable status/body. Lets the forward path
    be exercised without a real DBX.app.
    """

    def __init__(self, status: int = 200, body: dict | None = None):
        self.requests: list[tuple[str, dict]] = []
        self.status = status
        self.body = body or {
            "content": [{"type": "text", "text": json.dumps({"matched": 7})}],
            "isError": False,
        }
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                outer.requests.append(
                    (self.path, json.loads(self.rfile.read(length) or b"{}"))
                )
                payload = json.dumps(outer.body).encode()
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = self.server.server_address[1]
        # app-data 目录由 mock 自建（tempfile），端口文件写入点不经过外部
        # 参数；调用方经 self.app_data 注入 DBX_APP_DATA_DIR。
        self.app_data = tempfile.mkdtemp(prefix="dbx-kafka-mockbridge-")
        Path(self.app_data, "mcp-bridge-port").write_text(str(port), encoding="utf-8")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@scenario("K16", "stdio bridge fallback: fail-closed + mock-bridge forward")
def k16_bridge_fallback() -> str:
    # 1) 桥未发布：空 app-data + no-op launch（`:`，无 UI 弹出）→ ensure 跑满
    #    app-start 预算（30s，与 ssh/ldap smoke 同款：这正是被测行为）→
    #    fail-closed 可行动错误（桥失败原因 + 内联凭据出路），不假死。
    app_data = tempfile.mkdtemp(prefix="dbx-kafka-mcp-appdata-")
    client = McpStdioClient.start(
        default_binary(), tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-"),
        timeout=45,
        extra_env={"DBX_APP_DATA_DIR": app_data, "DBX_APP_LAUNCH_CMD": ":"},
    )
    try:
        client.request("initialize", {"protocolVersion": "2024-11-05"})
        result = client.call_tool("kafka_messages_digest", connectionId="mcp-nope",
                                  topic="orders")
        text = expect_tool_error(result, ("DBX app bridge", "inline connection parameters"))
        assert "mcp-nope" in text, text
    finally:
        client.close()

    # 2) 桥存在（本地 mock 桥）：未池化 connectionId 转发宿主契约五字段
    #    （plugin_id=io.dbx.kafka），应用侧 MCP envelope 逐字透传为 stdio
    #    响应；本地池不被污染；timeoutSecs 字符串宽容折算 timeout_ms。
    mock = MockBridge()
    client = McpStdioClient.start(
        default_binary(), tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-"),
        extra_env={"DBX_APP_DATA_DIR": mock.app_data, "DBX_APP_LAUNCH_CMD": ":"},
    )
    try:
        client.request("initialize", {"protocolVersion": "2024-11-05"})
        forwarded = unwrap_stdio(client.call_tool(
            "kafka_messages_digest", connectionId="saved-orders",
            topic="orders", timeoutSecs="30"))
        assert forwarded == {"matched": 7}, forwarded
        assert len(mock.requests) == 1, mock.requests
        path, body = mock.requests[0]
        assert path == "/call-plugin-tool", path
        assert body["plugin_id"] == "io.dbx.kafka", body
        assert body["connection_id"] == "saved-orders" and body["tool"] == "kafka_messages_digest", body
        assert body["arguments"]["topic"] == "orders", body
        assert body["timeout_ms"] == 30000, body
    finally:
        client.close()
        mock.stop()

    # 3) 桥 404（连接不存在/旧版应用）：错误带 "DBX app bridge returned HTTP"。
    mock = MockBridge(status=404,
                      body={"error": "Connection with id 'ghost' not found"})
    client = McpStdioClient.start(
        default_binary(), tempfile.mkdtemp(prefix="dbx-kafka-mcp-stdio-"),
        extra_env={"DBX_APP_DATA_DIR": mock.app_data, "DBX_APP_LAUNCH_CMD": ":"},
    )
    try:
        client.request("initialize", {"protocolVersion": "2024-11-05"})
        result = client.call_tool("kafka_messages_digest", connectionId="ghost", topic="orders")
        expect_tool_error(result, ("DBX app bridge returned HTTP 404",))
    finally:
        client.close()
        mock.stop()
    return "fail-closed without the app; mock-bridge forward contract + envelope verbatim; 404 surfaced"


# -- runner ----------------------------------------------------------------------


def report() -> None:
    widths = (4, 52, 6)
    print(f"{'No.':<{widths[0]}} {'Scenario':<{widths[1]}} {'Status':<{widths[2]}} Detail")
    for result in RESULTS:
        print(f"{result.no:<{widths[0]}} {result.name:<{widths[1]}} {result.status:<{widths[2]}} {result.detail}")
    counts = {status: sum(1 for r in RESULTS if r.status == status) for status in ("PASS", "FAIL", "SKIP")}
    print(f"\ntotal={len(RESULTS)} PASS={counts['PASS']} FAIL={counts['FAIL']} SKIP={counts['SKIP']}")


def _all_scenarios():
    return [k1_settings, k2_tools, k3_readonly_tools, k4_readonly_write, k5_intent_pending,
            k6_intent_applied, k7_ui_state, k8_digest_gates, k9_unknown, k10_allow_delete]


def _stdio_scenarios():
    # stdio 模式自持进程（--mcp），不走 jsonl client 生命周期。
    return [k12_stdio_offline, k13_stdio_container, k17_stdio_robustness, k16_bridge_fallback]


def main() -> int:
    binary = default_binary()
    if not os.path.exists(binary):
        message = f"sidecar binary not found: {binary} (set DBX_PLUGIN_SIDECAR)"
        for fn in _all_scenarios() + _stdio_scenarios() + [k11_container, k14_schema_digest, k15_aggregate_reset, k18_consume_pool_churn, k19_enum_online]:
            RESULTS.append(ScenarioResult(fn.no, fn.name, "FAIL" if REQUIRE else "SKIP", message))
        report()
        return 0

    # 隔离数据目录：mcp-settings.json 与 audit.jsonl 不污染真实插件数据。
    data_dir = tempfile.mkdtemp(prefix="dbx-kafka-mcp-smoke-")
    client = SidecarClient.start(data_dir=data_dir)
    try:
        client.initialize()
        for fn in _all_scenarios():
            fn(client)
        k11_container(client, data_dir)
        k14_schema_digest(client)
        k15_aggregate_reset(client)
        k18_consume_pool_churn(client)
        k19_enum_online(client)
    finally:
        for conn_id in ("smoke-mcp-ro", "smoke-mcp-nodelete"):
            try:
                client.request("connection/disconnect", {"connection": {"id": conn_id}})
            except Exception:
                pass
        try:
            client.close()
        except Exception:
            pass

    for fn in _stdio_scenarios():
        fn()

    report()
    return 1 if any(result.status == "FAIL" for result in RESULTS) else 0


if __name__ == "__main__":
    raise SystemExit(main())
