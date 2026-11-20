#!/usr/bin/env python3
"""Required Connect acceptance over real sidecar JSON-RPC and disposable workers.

No SKIP paths. The unique Compose project is always removed, including on failure.
Only loopback-bound ephemeral endpoints are used; installed DBX state is untouched.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid
from sidecar_client_jsonl import SidecarClient, SidecarError, lifecycle_params

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "dbx-connect-smoke-" + uuid.uuid4().hex[:8]
PORT = int(os.environ.get("CONNECT_TEST_HTTP_PORT", "38083"))
BROKER_PORT = int(os.environ.get("CONNECT_TEST_KAFKA_PORT", "39092"))
URL = f"http://127.0.0.1:{PORT}"

def compose(*args):
    return subprocess.run(["docker", "compose", "-p", PROJECT, "-f", str(ROOT / "docker-compose.connect-test.yml"), *args], check=True)

def wait_for(function, description, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = function()
            if result:
                return result
        except (OSError, SidecarError, AssertionError) as error:
            last = type(error).__name__
        time.sleep(1)
    raise AssertionError(f"Timeout: {description} ({last})")

def ready():
    with urllib.request.urlopen(URL + "/connectors", timeout=2) as response:
        return response.status == 200

def main():
    client = None
    try:
        compose("up", "-d", "--wait", "--wait-timeout", "120")
        wait_for(ready, "Connect REST readiness")
        print("PASS C1: isolated Kafka/Connect ready", flush=True)
        with tempfile.TemporaryDirectory(prefix="dbx-connect-data-") as data:
            binary = ROOT / "backend/bin/dbx-plugin-kafka"
            client = SidecarClient.start(binary=str(binary), data_dir=data, timeout=30)
            client.initialize()
            connection = {"id": "connect-smoke", "name": "Disposable Connect", "external_config": {
                "bootstrap_servers": f"127.0.0.1:{BROKER_PORT}", "read_only": False, "allow_delete": True,
                "connect_clusters": json.dumps([{"name": "local", "url": URL}]),
            }}
            client.request("connection/connect", lifecycle_params(connection))
            def call(method, **params):
                return client.request("kafka/connect/" + method, {"connectionId": "connect-smoke", "cluster": "local", **params})
            def state(name, expected):
                row = call("connectors/get", connector=name)["connector"]
                return row if row.get("state") == expected else None
            assert call("test")["version"]
            assert call("clusters/list")["clusters"][0]["name"] == "local"
            plugins = call("plugins/list")["plugins"]
            source_class = "org.apache.kafka.connect.file.FileStreamSourceConnector"
            sink_class = "org.apache.kafka.connect.file.FileStreamSinkConnector"
            assert {source_class, sink_class}.issubset({p["class"] for p in plugins})
            assert call("plugins/config", plugin=source_class)["definitions"]
            print("PASS C2: version, cluster, plugins and config definitions", flush=True)
            source = "dbx-source"
            config = {"name": source, "connector.class": source_class, "tasks.max": "1", "file": "/tmp/dbx-connect-input.txt", "topic": "dbx-connect-events"}
            invalid = call("config/validate", plugin=source_class, config={})
            assert invalid["errorCount"] > 0
            validity = call("config/validate", plugin=source_class, config=config)
            assert validity["errorCount"] == 0, validity
            assert call("connectors/create", connector=source, config=config)["success"]
            wait_for(lambda: state(source, "RUNNING"), "source connector running")
            wait_for(lambda: call("connectors/get", connector=source)["connector"]["tasks"], "source task created")
            assert call("config/get", connector=source)["config"]["topic"] == "dbx-connect-events"
            assert call("config/set", connector=source, config=config)["success"]
            print("PASS C3: validate, create, describe and update connector", flush=True)
            for action, expected in [("pause", "PAUSED"), ("resume", "RUNNING")]:
                assert call("connectors/action", connector=source, action=action)["success"]
                wait_for(lambda: state(source, expected), expected)
            for action in ["restart", "restart-all", "restart-failed"]:
                assert call("connectors/action", connector=source, action=action)["success"]
                wait_for(lambda: state(source, "RUNNING"), "connector restart")
            assert call("tasks/restart", connector=source, taskId=0)["success"]
            print("PASS C4: pause/resume and connector/task restart variants", flush=True)
            broken = "dbx-broken-sink"
            bad_config = {"connector.class": sink_class, "tasks.max": "1", "topics": "dbx-connect-events", "file": "/missing/dbx-connect/output.txt"}
            assert call("connectors/create", connector=broken, config=bad_config)["success"]
            def failed_task():
                rows = call("connectors/list")["connectors"]
                assert len(rows) == 2
                failed = next(row for row in rows if row["name"] == broken)
                return failed if any(task["state"] == "FAILED" for task in failed["tasks"]) else None
            failed = wait_for(failed_task, "failed task alongside healthy connector")
            assert failed["topics"] == ["dbx-connect-events"]
            assert failed["consumerGroup"] == "connect-" + broken
            print("PASS C5: task failure isolation and topic/group links", flush=True)
            connection["external_config"]["read_only"] = True
            client.request("connection/connect", lifecycle_params(connection))
            try:
                call("connectors/action", connector=source, action="pause")
                raise AssertionError("read-only operation succeeded")
            except SidecarError as error:
                assert "read-only" in str(error)
            connection["external_config"]["read_only"] = False
            client.request("connection/connect", lifecycle_params(connection))
            try:
                call("connectors/delete", connector=source, confirmConnector="wrong")
                raise AssertionError("confirmation was ignored")
            except SidecarError as error:
                assert error.code == -32602
            print("PASS C6: reconnect and destructive/read-only gates", flush=True)
            assert call("connectors/action", connector=source, action="stop")["success"]
            wait_for(lambda: state(source, "STOPPED"), "source stopped")
            assert call("offsets/reset", connector=source, confirmConnector=source)["success"]
            for name in [source, broken]:
                assert call("connectors/delete", connector=name, confirmConnector=name)["success"]
            assert call("connectors/list")["connectors"] == []
            print("PASS C7: stop, reset offsets and delete", flush=True)
            client.close()
            client = None
        return 0
    except Exception:
        # This stack contains only synthetic fixture data, never real credentials.
        compose("logs", "--tail", "60")
        raise
    finally:
        if client is not None:
            client.close()
        compose("down", "-v", "--remove-orphans")

if __name__ == "__main__":
    raise SystemExit(main())
