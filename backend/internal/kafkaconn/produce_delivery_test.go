package kafkaconn

// produce_delivery_test.go：produce 投递参数（acks / enableIdempotence）
// 单测——取值面归一与 kgo producer opts 组装（纯函数 + kgo.NewClient 校验，
// 不连网）。

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNormalizeProduceAcks(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{"empty defaults to all", "", "all", false},
		{"all", "all", "all", false},
		{"leader alias 1", "1", "1", false},
		{"leader alias word", "leader", "1", false},
		{"none unsupported", "0", "", true},
		{"none word unsupported", "none", "", true},
		{"garbage", "two", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeProduceAcks(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("normalizeProduceAcks(%q) error = nil, want error", tc.value)
				}
				if tc.value == "0" || tc.value == "none" {
					if !strings.Contains(err.Error(), "acks=0") {
						t.Fatalf("error should explain acks=0 incompatibility: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeProduceAcks(%q) error = %v", tc.value, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeProduceAcks(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestProduceDeliveryOpts(t *testing.T) {
	idempotence := func(v bool) *bool { return &v }
	cases := []struct {
		name        string
		acks        string
		idempotence *bool
		wantOpts    int
		wantErr     bool
		wantErrText string
	}{
		{"default: franz-go idempotent acks=all, no override", "", nil, 0, false, ""},
		{"all + idempotence true is default behavior", "all", idempotence(true), 0, false, ""},
		{"all + idempotence false disables idempotent write", "all", idempotence(false), 2, false, ""},
		{"leader requires explicit idempotence=false", "1", nil, 0, true, "idempotent producer requires acks=all"},
		{"leader + idempotence=true conflicts", "1", idempotence(true), 0, true, "idempotent producer requires acks=all"},
		{"leader + idempotence=false", "1", idempotence(false), 2, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, err := produceDeliveryOpts(tc.acks, tc.idempotence)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("produceDeliveryOpts() error = nil, want error %q", tc.wantErrText)
				}
				if !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("error = %v, want contains %q", err, tc.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("produceDeliveryOpts() error = %v", err)
			}
			if len(opts) != tc.wantOpts {
				t.Fatalf("opts count = %d, want %d", len(opts), tc.wantOpts)
			}
			// 能被 kgo 接受（NewClient 校验发生在拨号期外，这里仅编译期形状）。
			_ = opts
		})
	}
}

// TestProduceDeliveryOptsAcceptedByKgo：组装出的 opts 能通过 kgo.NewClient
// 校验（无 seed broker 会报 seed 错误而非 opts 冲突错误，借以验证 opts 合法）。
func TestProduceDeliveryOptsAcceptedByKgo(t *testing.T) {
	idempotenceFalse := false
	cases := [][]kgo.Opt{
		mustProduceDeliveryOpts(t, "all", idempotenceFalse),
		mustProduceDeliveryOpts(t, "1", idempotenceFalse),
	}
	for i, opts := range cases {
		if _, err := kgo.NewClient(append(opts, kgo.SeedBrokers("127.0.0.1:1"))...); err != nil {
			t.Fatalf("case %d: kgo.NewClient() rejected delivery opts: %v", i, err)
		}
	}
}

func mustProduceDeliveryOpts(t *testing.T, acks string, idempotence bool) []kgo.Opt {
	t.Helper()
	opts, err := produceDeliveryOpts(acks, &idempotence)
	if err != nil {
		t.Fatalf("produceDeliveryOpts(%q) error = %v", acks, err)
	}
	return opts
}
