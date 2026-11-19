package kafkaconn

// schema_decode_ctx_test.go：SR 取数预算与失败去重回归（线上「满屏
// context deadline exceeded」事故）。
//
// 现场：消息扫描窗口（consume timeoutMs）到点后，消费循环里仍在排队的消息
// 拿着已失效的扫描 context 去取 SR 元数据，于是每条消息都报
// `schema: Get ".../schemas/ids/37": context deadline exceeded`，看起来像
// 「所有消息都无法解码」，其实 SR 可能完全正常；SR 真不可用时又会把同一个
// 失败按消息条数放大成 N 次超时请求。

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestSchemaDecoderIgnoresExpiredScanContext 扫描窗口已到点，但会话根仍
// 有效时必须照常解码（SR 取数不该借用扫描窗口预算）。
func TestSchemaDecoderIgnoresExpiredScanContext(t *testing.T) {
	registry := newFakeRegistry()
	id, _ := registry.register("order-value", ordersFDSetB64(t), "PROTOBUF")
	client := newTestSRClient(t, registry)

	decoder := newSchemaDecoder(client, nil)
	decoder.bindContext(context.Background())

	// 扫描窗口已超时（真实消费循环在到点后仍会处理完手上的这一批）。
	scanCtx, cancel := context.WithCancel(context.Background())
	cancel()

	frame := encodeWireFrame(id, []byte{0x00, 0x0a, 0x01, 'a'}) // indexes=[0] + id="a"
	decoded, info, err := decoder.decode(scanCtx, frame)
	if err != nil {
		t.Fatalf("decode() with expired scan ctx error = %v, want success", err)
	}
	if !strings.Contains(string(decoded), `"a"`) {
		t.Errorf("decoded = %s, want id \"a\"", decoded)
	}
	if info.ID != id {
		t.Errorf("schema info id = %d, want %d", info.ID, id)
	}
}

// TestSchemaDecoderFailsFastPerSchema SR 不可用时同一 schema 只打一次请求：
// 一次消费内的后续消息直接复用失败结果，不再逐条触发超时。
func TestSchemaDecoderFailsFastPerSchema(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream connect failed"))
	}))
	defer server.Close()

	client, err := newSchemaRegistryClient(Profile{SRURL: server.URL}, connSecrets{})
	if err != nil {
		t.Fatalf("newSchemaRegistryClient() error = %v", err)
	}
	decoder := newSchemaDecoder(client, nil)
	decoder.bindContext(context.Background())

	frame := encodeWireFrame(37, []byte{0x00, 0x0a, 0x01, 'a'})
	var firstErr error
	for i := 0; i < 5; i++ {
		if _, _, err := decoder.decode(context.Background(), frame); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		t.Fatal("decode() accepted unreachable schema registry")
	}
	text := firstErr.Error()
	for _, want := range []string{"fetch schema id 37", "502"} {
		if !strings.Contains(text, want) {
			t.Errorf("error = %q, want it to mention %q", text, want)
		}
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("schema registry requests = %d, want 1 (failure must not be retried per message)", got)
	}
}

// TestProtobufFileNamesInBuildError 描述符构建失败时错误里带 .proto 文件名，
// 便于把报错对回 SR subject（畸形描述符常在兄弟 message 上报错）。
func TestProtobufFileNamesInBuildError(t *testing.T) {
	fdset := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("rcs/v1/broken.proto"),
		Package: proto.String("rcs.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Broken"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:     proto.String("missing"),
				Number:   proto.Int32(1),
				Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
				TypeName: proto.String(".rcs.v1.DoesNotExist"),
			}},
		}},
	}}}
	raw, err := proto.Marshal(fdset)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = protobufFilesFromSchema(base64.StdEncoding.EncodeToString(raw))
	if err == nil {
		t.Fatal("protobufFilesFromSchema() accepted an unresolvable type reference")
	}
	if !strings.Contains(err.Error(), "files: rcs/v1/broken.proto") {
		t.Errorf("error = %v, want it to list rcs/v1/broken.proto", err)
	}
}
