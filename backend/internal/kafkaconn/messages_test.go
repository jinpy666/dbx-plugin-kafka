package kafkaconn

// messages_test.go：过滤匹配器与留存预算的离线单测（不触真实 broker）。

import "testing"

// S-MATCHBYTES valueFilter 通道的字节级匹配（评审 M）：语义与 textMatcher.match
// 完全一致（大小写不敏感 contains/prefix/exact、regex、空 query 恒真），但
// 不做 string(record.Value) 整串拷贝——大 value × 高扫描量下每记录一份拷贝
// 是留存预算之外的第二个内存放大器。
func TestTextMatcherMatchBytesMatchesStringPath(t *testing.T) {
	cases := []struct {
		mode  string
		value string
		query string
	}{
		{"contains", "HELLO World", "hello w"},
		{"contains", "hello world", "WORLD"},
		{"contains", "héllo Wörld", "wörld"},
		{"contains", "plain lowercase payload", "missing"},
		{"contains", string([]byte{0x00, 'a', 'b', 0xff}), "ab"},
		{"prefix", "HELLO World", "hello"},
		{"prefix", "hello world", "HELLO"},
		{"prefix", "héllo", "hél"},
		{"exact", "MiXeD", "mixed"},
		{"exact", "уже", "УЖЕ"},
		{"regex", "abc123", `a.c\d+`},
		{"regex", "abc", `[A-Z]+`},
	}
	for _, tc := range cases {
		matcher, err := newConsumeTextMatcher(ConsumeParams{MatchMode: tc.mode})
		if err != nil {
			t.Fatalf("matcher %s: %v", tc.mode, err)
		}
		want := matcher.match(tc.value, tc.query)
		got := matcher.matchBytes([]byte(tc.value), tc.query)
		if got != want {
			t.Fatalf("mode=%s value=%q query=%q: matchBytes=%v != match=%v", tc.mode, tc.value, tc.query, got, want)
		}
	}
	// 空 query 恒真（未设通道过滤）。
	matcher, _ := newConsumeTextMatcher(ConsumeParams{})
	if !matcher.matchBytes([]byte("anything"), "") {
		t.Fatal("empty query must always match")
	}
}
