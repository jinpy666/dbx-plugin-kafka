package kafkaconn

// codec_test.go：解码/解压 roundtrip 与消息二进制保真（§5.3 valueBase64
// 恒完整；tinyrdm string(record.Value) 二进制损坏 bug 的回归测试）。

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buffer.Bytes()
}

func TestDecodeConsumeValueRoundtrip(t *testing.T) {
	payload := []byte("kafka payload 0123456789")

	// gzip。
	got, decoded, decodeErr := decodeConsumeValue(gzipBytes(t, payload), "none", "gzip")
	if decodeErr != "" || !decoded || !bytes.Equal(got, payload) {
		t.Errorf("gzip roundtrip = %q, decoded=%v, err=%q", got, decoded, decodeErr)
	}

	// lz4。
	var lz4Buffer bytes.Buffer
	lz4Writer := lz4.NewWriter(&lz4Buffer)
	lz4Writer.Write(payload)
	lz4Writer.Close()
	got, _, decodeErr = decodeConsumeValue(lz4Buffer.Bytes(), "none", "lz4")
	if decodeErr != "" || !bytes.Equal(got, payload) {
		t.Errorf("lz4 roundtrip err=%q", decodeErr)
	}

	// zstd。
	encoder, _ := zstd.NewWriter(nil)
	zstdBytes := encoder.EncodeAll(payload, nil)
	encoder.Close()
	got, _, decodeErr = decodeConsumeValue(zstdBytes, "none", "zstd")
	if decodeErr != "" || !bytes.Equal(got, payload) {
		t.Errorf("zstd roundtrip err=%q", decodeErr)
	}

	// snappy block。
	snappyBytes := snappy.Encode(nil, payload)
	got, _, decodeErr = decodeConsumeValue(snappyBytes, "none", "snappy")
	if decodeErr != "" || !bytes.Equal(got, payload) {
		t.Errorf("snappy roundtrip err=%q", decodeErr)
	}

	// base64 二次解码 + gzip。
	combined := base64.StdEncoding.EncodeToString(gzipBytes(t, payload))
	got, decoded, decodeErr = decodeConsumeValue([]byte(combined), "base64", "gzip")
	if decodeErr != "" || !decoded || !bytes.Equal(got, payload) {
		t.Errorf("base64+gzip roundtrip err=%q", decodeErr)
	}

	// 无处理。
	got, decoded, decodeErr = decodeConsumeValue(payload, "none", "none")
	if decodeErr != "" || decoded || !bytes.Equal(got, payload) {
		t.Errorf("passthrough err=%q", decodeErr)
	}

	// 非法 base64 → 保留原值 + 错误信息。
	got, decoded, decodeErr = decodeConsumeValue(payload, "base64", "none")
	if decodeErr == "" || decoded || !bytes.Equal(got, payload) {
		t.Errorf("invalid base64 should keep raw: err=%q decoded=%v", decodeErr, decoded)
	}

	// 未知解压 → 拒绝。
	if _, err := normalizeDecompressMethod("brotli"); err == nil {
		t.Error("bogus decompression expected error")
	}
	if _, err := normalizeDecodeMethod("hex"); err == nil {
		t.Error("bogus decode expected error")
	}
}

// 恶意二进制样本：含非法 UTF-8 序列与 NUL。
var binarySample = []byte{0x00, 0x01, 0xff, 0xfe, 'k', 'd', 'b', 0x80, 0x81}

func TestMessageBinaryFidelity(t *testing.T) {
	record := &kgo.Record{
		Topic:     "bin",
		Partition: 0,
		Offset:    7,
		Timestamp: time.UnixMilli(1700000000000),
		Key:       binarySample,
		Value:     binarySample,
	}
	message := messageFromRecord(record, record.Value, false, "", false)

	// valueText 是 UTF-8 安全预览（不 panic、无原始字节直转损坏风险由
	// valueBase64 兜底）。
	if message.ValueText == "" {
		t.Error("valueText should be non-empty (replacement chars)")
	}
	// valueBase64 恒完整：roundtrip 回原字节。
	decoded, err := base64.StdEncoding.DecodeString(message.ValueBase64)
	if err != nil {
		t.Fatalf("decode valueBase64: %v", err)
	}
	if !bytes.Equal(decoded, binarySample) {
		t.Errorf("valueBase64 roundtrip mismatch: got %x want %x", decoded, binarySample)
	}
	if message.Truncated {
		t.Error("small message should not be truncated")
	}
	// key 非法 UTF-8 → keyBase64。
	if message.Key != "" || message.KeyBase64 == "" {
		t.Errorf("binary key should go to keyBase64: key=%q keyBase64=%q", message.Key, message.KeyBase64)
	}
	keyBytes, _ := base64.StdEncoding.DecodeString(message.KeyBase64)
	if !bytes.Equal(keyBytes, binarySample) {
		t.Error("keyBase64 roundtrip mismatch")
	}
}

func TestMessageTextKeyUsesKeyField(t *testing.T) {
	record := &kgo.Record{
		Topic:     "txt",
		Key:       []byte("order-1"),
		Value:     []byte("hello"),
		Timestamp: time.UnixMilli(1),
	}
	message := messageFromRecord(record, record.Value, false, "", true)
	if message.Key != "order-1" || message.KeyBase64 != "" {
		t.Errorf("text key should stay in key field: %+v", message)
	}
	if message.ValueText != "hello" {
		t.Errorf("valueText = %q", message.ValueText)
	}
	if !message.Committed {
		t.Error("committed flag should pass through")
	}
}

func TestMessageTruncationAtLimit(t *testing.T) {
	big := bytes.Repeat([]byte("a"), maxMessageBytes+1024)
	record := &kgo.Record{Topic: "big", Value: big, Timestamp: time.UnixMilli(1)}
	message := messageFromRecord(record, big, false, "", false)
	if !message.Truncated {
		t.Error("oversized value should be marked truncated")
	}
	decoded, err := base64.StdEncoding.DecodeString(message.ValueBase64)
	if err != nil || len(decoded) != maxMessageBytes {
		t.Errorf("truncated length = %d, %v (want %d)", len(decoded), err, maxMessageBytes)
	}
	// valueText 与 valueBase64 同用 maxMessageBytes 截断（KAFKA-H1：双通道
	// 策略一致，digest 高扫描量不再 GB 级驻留）。
	if len(message.ValueText) != maxMessageBytes {
		t.Errorf("valueText length = %d, want %d", len(message.ValueText), maxMessageBytes)
	}
	// 上限内 valueText 完整（不截断）。
	small := messageFromRecord(&kgo.Record{Topic: "t", Value: []byte("hello"), Timestamp: time.UnixMilli(1)}, []byte("hello"), false, "", false)
	if small.Truncated || small.ValueText != "hello" {
		t.Errorf("within-limit valueText = %q truncated=%v", small.ValueText, small.Truncated)
	}
}

// 高压缩比载荷超限：全解压分支（gzip/lz4/zstd/snappy block/framed）都必须
// 报解压炸弹防护错，而不是把膨胀后的字节物化进内存（KAFKA-H1 回归）。
func TestDecompressBombGuard(t *testing.T) {
	payload := bytes.Repeat([]byte{0}, maxDecodedBytes+1024)
	// gzip。
	if _, err := decompressPayload(gzipBytes(t, payload), "gzip"); err == nil || !strings.Contains(err.Error(), "decompression bomb guard") {
		t.Errorf("gzip bomb error = %v, want bomb guard", err)
	}
	// lz4。
	var lz4Buffer bytes.Buffer
	lz4Writer := lz4.NewWriter(&lz4Buffer)
	lz4Writer.Write(payload)
	lz4Writer.Close()
	if _, err := decompressPayload(lz4Buffer.Bytes(), "lz4"); err == nil || !strings.Contains(err.Error(), "decompression bomb guard") {
		t.Errorf("lz4 bomb error = %v, want bomb guard", err)
	}
	// zstd（WithDecoderMaxMemory 先行兜底 / 显式长度校验同向，任一报限即通过）。
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	zstdBytes := encoder.EncodeAll(payload, nil)
	encoder.Close()
	if _, err := decompressPayload(zstdBytes, "zstd"); err == nil || !(strings.Contains(err.Error(), "decompression bomb guard") || strings.Contains(err.Error(), "exceeds")) {
		t.Errorf("zstd bomb error = %v, want limit error", err)
	}
	// snappy block（DecodedLen 声明长度预检）。
	snappyBlock := snappy.Encode(nil, payload)
	if _, err := decompressPayload(snappyBlock, "snappy"); err == nil || !strings.Contains(err.Error(), "decompression bomb guard") {
		t.Errorf("snappy block bomb error = %v, want bomb guard", err)
	}
	// snappy framed（流式 LimitReader）。
	var framedBuffer bytes.Buffer
	framedWriter := snappy.NewBufferedWriter(&framedBuffer)
	framedWriter.Write(payload)
	framedWriter.Close()
	if _, err := decompressPayload(framedBuffer.Bytes(), "snappy"); err == nil || !strings.Contains(err.Error(), "decompression bomb guard") {
		t.Errorf("snappy framed bomb error = %v, want bomb guard", err)
	}
	// 上限边界内正常解压不受影响。
	if got, err := decompressPayload(gzipBytes(t, []byte("ok payload")), "gzip"); err != nil || string(got) != "ok payload" {
		t.Errorf("within-limit gzip = %q, %v", got, err)
	}
	// decodeConsumeValue：超限错误按 DecodeError 语义进消息（保留原值=原始
	// 压缩字节，不中断消费）。
	compressed := gzipBytes(t, payload)
	got, decoded, decodeErr := decodeConsumeValue(compressed, "none", "gzip")
	if decodeErr == "" || !strings.Contains(decodeErr, "decompression bomb guard") || decoded || !bytes.Equal(got, compressed) {
		t.Errorf("bomb via decodeConsumeValue: err=%q decoded=%v kept=%v", decodeErr, decoded, bytes.Equal(got, compressed))
	}
}

func TestSafeUTF8Preview(t *testing.T) {
	if got := safeUTF8Preview([]byte("hello")); got != "hello" {
		t.Errorf("valid utf8 = %q", got)
	}
	got := safeUTF8Preview([]byte{'a', 0xff, 'b'})
	if !strings.Contains(got, "\uFFFD") {
		t.Errorf("invalid byte should be replaced: %q", got)
	}
}

func TestIsProbablyUTF8(t *testing.T) {
	if !isProbablyUTF8([]byte("plain")) {
		t.Error("plain text should be utf8")
	}
	if isProbablyUTF8([]byte{0xff}) {
		t.Error("invalid byte should not be utf8")
	}
	if isProbablyUTF8([]byte{0x00}) {
		t.Error("NUL byte should not be treated as text")
	}
}

func TestEncodeBase64WithLimit(t *testing.T) {
	data := []byte("0123456789")
	got, truncated := encodeBase64WithLimit(data, 5)
	decoded, _ := base64.StdEncoding.DecodeString(got)
	if !truncated || string(decoded) != "01234" {
		t.Errorf("limit encode = %q, truncated=%v (want 01234 + truncated)", decoded, truncated)
	}
	// 未超限不标记。
	got, truncated = encodeBase64WithLimit(data, 10)
	if truncated {
		t.Error("within-limit encode should not be truncated")
	}
	if decoded, _ = base64.StdEncoding.DecodeString(got); string(decoded) != "0123456789" {
		t.Errorf("full encode = %q", decoded)
	}
}
