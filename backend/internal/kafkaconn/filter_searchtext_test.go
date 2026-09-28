package kafkaconn

// filter_searchtext_test.go：filter 通道检索文本的逐字节等价性（评审 L-3）。

import (
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAppendRecordSearchTextMatchesLegacyJoin(t *testing.T) {
	record := &kgo.Record{
		Topic:     "orders",
		Partition: 2,
		Offset:    9,
		Timestamp: time.UnixMilli(123),
		Key:       []byte("k1"),
		Value:     []byte("v1"),
		Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte("v")}},
	}
	want := strings.Join([]string{
		"orders", "2", "9", "123", "k1", "v1", "h=v",
	}, " ")
	got := string(appendRecordSearchText(nil, record))
	if got != want {
		t.Fatalf("search text = %q, want %q", got, want)
	}
	if empty := string(appendRecordSearchText(nil, nil)); empty != "" {
		t.Fatalf("nil record text = %q, want empty", empty)
	}
}
