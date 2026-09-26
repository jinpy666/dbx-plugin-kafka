package kafkaconn

// consume_retention_test.go：digest 大扫描留存的内存预算（评审 H-1）——
// 命中消息的 value 字节累计超预算即停止留存（matched 计数不受影响），
// 且 digest 路径跳过 valueBase64 通道（聚合只读 valueText，base64 是纯
// 冤枉驻留）。全离线：预算判定与消息构建均为纯函数。

import (
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// S-RETAIN-1 预算判定：budget<=0 恒 admitted（契约原语义）；累计超预算拒绝；
// 首条消息恒 admitted（预算小于单条消息时保证 digest 至少有 1 条样本）。
func TestConsumeRetentionTracker(t *testing.T) {
	t.Run("no budget admits everything", func(t *testing.T) {
		tracker := consumeRetentionTracker{budget: 0}
		for i := 0; i < 1000; i++ {
			if !tracker.admit(512 * 1024) {
				t.Fatalf("budget=0 must never reject (rejected at %d)", i)
			}
		}
	})
	t.Run("budget caps accumulation", func(t *testing.T) {
		tracker := consumeRetentionTracker{budget: 100}
		if !tracker.admit(60) {
			t.Fatal("first message under budget must be admitted")
		}
		if !tracker.admit(40) {
			t.Fatal("message exactly reaching budget must be admitted")
		}
		if tracker.admit(1) {
			t.Fatal("message exceeding budget must be rejected")
		}
		if tracker.retained != 100 {
			t.Fatalf("retained must stay at budget: %d", tracker.retained)
		}
	})
	t.Run("first message always admitted", func(t *testing.T) {
		tracker := consumeRetentionTracker{budget: 10}
		if !tracker.admit(4096) {
			t.Fatal("first message must be admitted even over budget")
		}
		if tracker.admit(1) {
			t.Fatal("subsequent messages over budget must be rejected")
		}
	})
}

// S-RETAIN-2 skipValueBase64：digest 路径跳过 base64 通道（ValueBase64 空、
// ValueText 与 Truncated 语义不变）；默认双通道不变。
func TestMessageFromRecordSkipValueBase64(t *testing.T) {
	record := &kgo.Record{
		Topic:     "t",
		Partition: 1,
		Offset:    7,
		Timestamp: time.UnixMilli(42),
		Key:       []byte("k"),
		Value:     []byte(strings.Repeat("v", 2048)),
	}

	defaultMessage := messageFromRecordWithSchema(record, record.Value, false, "", false, nil, false)
	if defaultMessage.ValueBase64 == "" {
		t.Fatal("default dual-channel must keep valueBase64")
	}
	skipped := messageFromRecordWithSchema(record, record.Value, false, "", false, nil, true)
	if skipped.ValueBase64 != "" {
		t.Fatal("skipValueBase64 must leave valueBase64 empty")
	}
	if skipped.ValueText != defaultMessage.ValueText {
		t.Fatal("valueText must be identical with skipValueBase64")
	}
	if skipped.Topic != "t" || skipped.Partition != 1 || skipped.Offset != 7 || skipped.Timestamp != 42 {
		t.Fatal("locator fields must be preserved")
	}
	if skipped.Key != "k" {
		t.Fatal("key channel must be preserved")
	}

	// 截断标记不受 skip 影响：超限 value 仍标 truncated（valueText 截断在
	// boundedValue 内完成）。
	big := make([]byte, maxMessageBytes+1024)
	bigMessage := messageFromRecordWithSchema(&kgo.Record{Topic: "t", Value: big}, big, false, "", false, nil, true)
	if !bigMessage.Truncated {
		t.Fatal("truncated flag must survive skipValueBase64")
	}
	if len(bigMessage.ValueText) > maxMessageBytes+8 {
		t.Fatal("valueText must stay bounded at maxMessageBytes")
	}
}
