package mcp

// churn_test.go：会话/存储长期 churn 可靠性（可靠性纵深轮，S-CHURN-*，
// ldap Go 版同表）：ConfirmStore/CursorStore/IntentStore 大量「登记→消费/
// 过期→淘汰」循环后内部表无无界增长，活跃条目不被误逐，TTL/容量调整
// 不追溯（MCP_ACCEPTANCE §5/§8）。全部离线（store 纯逻辑 + 注入时钟）。

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"io.dbx.kafka.plugin/internal/kafkaconn"
)

func kafkaRow(topic string, offset int64) CursorRow {
	return CursorRow{Topic: topic, Partition: 0, Offset: offset}
}

// S-CHURN-CONF-1 ConfirmStore churn：600 轮 issue→consume（ok/expired/mismatch
// 混合）+ 周期性「只要预览不确认」的弃单，内部表必须收敛（Issue 时 prune
// 过期未消费令牌），不随轮数无界增长。
func TestConfirmChurnBounded(t *testing.T) {
	store := NewConfirmStore()
	now := intentBase
	hash := HashParams([]byte(`{"action":"delete"}`))
	const rounds = 600
	for index := 0; index < rounds; index++ {
		now = now.Add(time.Second)
		token, _, _ := store.Issue(hash, now)
		switch index % 3 {
		case 0:
			if got := store.Consume(token, hash, now); got != ConfirmOK {
				t.Fatalf("round %d: fresh token must consume ok: %v", index, got)
			}
		case 1:
			if got := store.Consume(token, hash, now.Add(2*ConfirmTTL)); got != ConfirmExpired {
				t.Fatalf("round %d: stale token must expire: %v", index, got)
			}
		default:
			if got := store.Consume(token, hash+"x", now); got != ConfirmHashMismatch {
				t.Fatalf("round %d: changed hash must invalidate: %v", index, got)
			}
		}
		// 每 5 轮弃一张单（签发后从不消费）：只能靠 Issue 的 prune 收敛。
		if index%5 == 0 {
			_, _, _ = store.Issue(hash, now)
		}
	}
	if len(store.items) > 61 {
		t.Fatalf("token table must converge after churn, got %d", len(store.items))
	}
	// 弃单过期后再消费 → unknown（prune 后不存在）。
	now = now.Add(2 * ConfirmTTL)
	store.Issue(hash, now) // 触发 prune
	if len(store.items) != 1 {
		t.Fatalf("prune must clear expired unconsumed tokens, got %d", len(store.items))
	}
}

// S-CHURN-CONF-2 confirmTtlSecs 接线（第五轮补齐 ldap 同名键）：SetTTL 对
// 新签发令牌生效、已签发令牌不追溯；过期报文携带实际生效 TTL。
func TestConfirmChurnTTLLifecycleAndWiring(t *testing.T) {
	store := NewConfirmStore()
	if store.TTL() != ConfirmTTL {
		t.Fatalf("default TTL: %v", store.TTL())
	}
	now := intentBase
	hash := HashParams([]byte(`{"a":1}`))
	legacy, _, _ := store.Issue(hash, now)
	store.SetTTL(10 * time.Second)
	// 旧令牌在原 60s 窗口内仍可用（若追溯为 10s 就会 expired）。
	if got := store.Consume(legacy, hash, now.Add(50*time.Second)); got != ConfirmOK {
		t.Fatalf("legacy token keeps its original 60s expiry (no retroactive TTL): %v", got)
	}
	// 新令牌按 10s 过期；≤0 的 SetTTL 被忽略。
	fresh, _, _ := store.Issue(hash, now)
	if got := store.Consume(fresh, hash, now.Add(11*time.Second)); got != ConfirmExpired {
		t.Fatalf("new token must expire on the configured TTL: %v", got)
	}
	store.SetTTL(0)
	if store.TTL() != 10*time.Second {
		t.Fatalf("non-positive SetTTL must be ignored: %v", store.TTL())
	}
	// Server 接线：mcp/settings/set 后新 preview 的过期报文带实际 TTL。
	server := NewServer(kafkaconn.NewService(), nil)
	if _, err := server.SettingsSet(map[string]any{"confirmTtlSecs": float64(30)}); err != nil {
		t.Fatal(err)
	}
	if server.confirms.TTL() != 30*time.Second {
		t.Fatalf("settings must wire confirm TTL: %v", server.confirms.TTL())
	}
	server.now = func() time.Time { return intentBase }
	if err := connect2At(server.svc, "churn-reset", false, true, "127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"connectionId": "churn-reset", "group": "g", "resetTo": "latest", "topics": []any{"t"}}
	preview, err := server.groupsOffsetsReset(args)
	if err != nil {
		t.Fatal(err)
	}
	server.now = func() time.Time { return intentBase.Add(31 * time.Second) }
	_, err = server.groupsOffsetsReset(map[string]any{
		"connectionId": "churn-reset", "group": "g", "resetTo": "latest",
		"topics": []any{"t"}, "confirmToken": preview["confirmToken"]})
	if err == nil || !strings.Contains(err.Error(), "expired (TTL 30s)") || !strings.Contains(err.Error(), "request a new preview") {
		t.Fatalf("expiry message must carry the effective TTL: %v", err)
	}
}

// S-CHURN-CUR-1 CursorStore churn：300 轮物化→翻页→淘汰循环，内部
// sessions/order 两表一致且收敛在容量内。
func TestCursorChurnBounded(t *testing.T) {
	store := NewCursorStore(time.Hour, 8, 100)
	now := intentBase
	lastID := ""
	for index := 0; index < 300; index++ {
		now = now.Add(time.Second)
		rows := []CursorRow{kafkaRow("t", int64(index)), kafkaRow("t", int64(index)+1000)}
		session := store.Put(rows, "t", now)
		lastID = session.ID
		if _, status := store.Next(session.ID, NextRequest{Offset: -1}, now); status != LookupFound {
			t.Fatalf("round %d: fresh session must page: %v", index, status)
		}
		if len(store.sessions) != len(store.order) || len(store.sessions) > 8 {
			t.Fatalf("round %d: store diverged: sessions=%d order=%d",
				index, len(store.sessions), len(store.order))
		}
	}
	// 最新会话存活且行内容 churn 后仍正确（显式 offset=0 重读首页）。
	result, status := store.Next(lastID, NextRequest{Offset: 0}, now)
	if status != LookupFound || len(result.Rows) != 2 || result.Rows[0].Offset != 299 {
		t.Fatalf("surviving session content drift: %+v %v", result, status)
	}
}

// S-CHURN-CUR-2 真 LRU：持续翻页的活跃会话不被误逐；被淘汰的旧 cursorId
// 在 Server 层报文带 TTL/容量/重建指引（第五轮对齐 ldap 报文质量）。
func TestCursorChurnActiveSurvivesAndMessageQuality(t *testing.T) {
	store := NewCursorStore(time.Hour, 3, 0)
	now := intentBase
	a := store.Put([]CursorRow{kafkaRow("t", 1)}, "t", now)
	b := store.Put([]CursorRow{kafkaRow("t", 2)}, "t", now)
	c := store.Put([]CursorRow{kafkaRow("t", 3)}, "t", now)
	for i := 0; i < 10; i++ {
		now = now.Add(time.Second)
		if _, status := store.Next(a.ID, NextRequest{Offset: -1}, now); status != LookupFound {
			t.Fatalf("active session must stay found: %v", status)
		}
	}
	now = now.Add(time.Second)
	d := store.Put([]CursorRow{kafkaRow("t", 4)}, "t", now)
	if _, status := store.Next(a.ID, NextRequest{Offset: -1}, now); status != LookupFound {
		t.Fatal("active session must not be evicted (LRU, not FIFO)")
	}
	if _, status := store.Next(b.ID, NextRequest{Offset: -1}, now); status != LookupUnknown {
		t.Fatal("least-recently-used session must be evicted")
	}
	for _, session := range []*CursorSession{c, d} {
		if _, status := store.Next(session.ID, NextRequest{Offset: -1}, now); status != LookupFound {
			t.Fatalf("session must survive: %v", status)
		}
	}
	// Server 层报文质量：淘汰的 cursorId 报 unknown 且带 TTL/容量/重建指引。
	server := NewServer(kafkaconn.NewService(), nil)
	evicted := server.cursors.Put([]CursorRow{kafkaRow("t", 9)}, "t", now)
	for i := 0; i < 9; i++ {
		server.cursors.Put([]CursorRow{kafkaRow("t", int64(10+i))}, "t", now)
	}
	_, err := server.Call("kafka_cursor_next", map[string]any{"cursorId": evicted.ID})
	if err == nil || !strings.Contains(err.Error(), "unknown cursorId") ||
		!strings.Contains(err.Error(), "TTL 600s") || !strings.Contains(err.Error(), "evicted") ||
		!strings.Contains(err.Error(), "re-run kafka_messages_digest") {
		t.Fatalf("evicted cursor message quality mismatch: %v", err)
	}
}

// S-CHURN-INT-1 IntentStore churn：500 轮登记→回报→过期循环后表收敛在
// LRU 容量内，快照读取不受 churn 影响，过期回报不复活。
func TestIntentChurnBoundedSnapshotIntact(t *testing.T) {
	store := NewIntentStore(60*time.Second, 20)
	snapshot := map[string]any{"panel": "messages", "count": float64(42)}
	store.SetSnapshot(snapshot)
	now := intentBase
	for index := 0; index < 500; index++ {
		now = now.Add(time.Second)
		id := "i-" + strconv.Itoa(index)
		store.Register(id, "search", map[string]any{"n": index}, now)
		if index%2 == 0 {
			if !store.Report(id, IntentApplied, map[string]any{"count": float64(index)}, "", now) {
				t.Fatalf("round %d: report must land", index)
			}
		}
		if len(store.items) > 20 || len(store.order) > 20 {
			t.Fatalf("round %d: intent table over capacity: %d/%d", index, len(store.items), len(store.order))
		}
	}
	got := store.Snapshot()
	if got["panel"] != "messages" || got["count"] != float64(42) || len(got) != 2 {
		t.Fatalf("snapshot corrupted by churn: %+v", got)
	}
	intent, status := store.Get("i-498", now)
	if status != LookupFound || intent.State != IntentApplied {
		t.Fatalf("latest reported intent must survive churn: %+v %v", intent, status)
	}
	now = now.Add(2 * time.Minute)
	store.Register("i-final", "search", nil, now) // 触发 prune
	if _, status := store.Get("i-499", now); status != LookupUnknown {
		t.Fatalf("expired intents must be pruned: %v", status)
	}
	if store.Report("i-final", IntentApplied, nil, "", now.Add(2*time.Minute)) {
		t.Fatal("late report must fail")
	}
}
