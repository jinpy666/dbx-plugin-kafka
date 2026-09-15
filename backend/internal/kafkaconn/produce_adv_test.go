package kafkaconn

// produce_adv_test.go：PROTOBUF Confluent wire framing message index 段单测
//（produce 补段 / consume 剥段 / 历史缺段回退；全部纯函数 + httptest 假
// Registry，不连网）。

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// nestedFDSetB64 构造含嵌套 message（com.dbx.test.Container{com.dbx.test.
// Container.Entry}）的 FDSet b64，用于 message index 嵌套路径测试。
func nestedFDSetB64(t *testing.T) string {
	t.Helper()
	strField := func(name string) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(1),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		}
	}
	fdset := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("nested.proto"),
		Package: proto.String("com.dbx.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Container"),
			Field: []*descriptorpb.FieldDescriptorProto{strField("name")},
			NestedType: []*descriptorpb.DescriptorProto{{
				Name:  proto.String("Entry"),
				Field: []*descriptorpb.FieldDescriptorProto{strField("id")},
			}},
		}},
	}}}
	raw, err := proto.Marshal(fdset)
	if err != nil {
		t.Fatalf("marshal FDSet: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func newTestSRClient(t *testing.T, registry *fakeRegistry) *schemaRegistryClient {
	t.Helper()
	server := newTestHTTPServer(t, registry.handler)
	client, err := newSchemaRegistryClient(Profile{SRURL: server.URL}, "")
	if err != nil {
		t.Fatalf("newSchemaRegistryClient() error = %v", err)
	}
	return client
}

// TestProtobufMessageIndexPath 覆盖 index 路径计算（顶层/多 message/嵌套）。
func TestProtobufMessageIndexPath(t *testing.T) {
	cases := []struct {
		name    string
		fdset   func(*testing.T) string
		subject string
		want    []int
	}{
		{"single top-level", ordersFDSetB64, "order-value", []int{0}},
		{"first of two", multiMessageFDSetB64, "order-value", []int{0}},
		{"second of two", multiMessageFDSetB64, "shipment-value", []int{1}},
		{"nested target", nestedFDSetB64, "entry-value", []int{0, 0}},
		{"container top-level", nestedFDSetB64, "container-value", []int{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := protobufMessageIndexesFromSchema(tc.fdset(t), tc.subject)
			if err != nil {
				t.Fatalf("protobufMessageIndexesFromSchema() error = %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("indexes = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("indexes = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestStripProtobufMessageIndexes 表驱动覆盖 varint 剥离（含截断/溢出）。
func TestStripProtobufMessageIndexes(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		depth   int
		want    []byte
		wantOK  bool
	}{
		{"depth 0 passthrough", []byte{0x01}, 0, []byte{0x01}, true},
		{"single zero index", []byte{0x00, 0x0a, 0x01, 'a'}, 1, []byte{0x0a, 0x01, 'a'}, true},
		{"multi varint", []byte{0x00, 0x01, 0xff, 0x7f}, 3, []byte{}, true},
		{"truncated varint", []byte{0x00, 0x80}, 2, nil, false},
		{"empty payload", []byte{}, 1, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := stripProtobufMessageIndexes(tc.payload, tc.depth)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && string(got) != string(tc.want) {
				t.Fatalf("rest = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestProtobufProduceWireFramingIncludesMessageIndexes：produce 侧 wire frame
// 在 schemaID 后补 message index 段，consume 侧剥段 roundtrip。
func TestProtobufProduceWireFramingIncludesMessageIndexes(t *testing.T) {
	cases := []struct {
		name      string
		fdset     string
		subject   string
		payload   string
		wantWire5 []byte // wire frame 第 6 字节起的 index 段期望值
	}{
		{"single message indexes [0]", ordersFDSetB64(t), "order-value", `{"id":"o-3","amount":"9","item":"nut"}`, []byte{0x00}},
		{"second message indexes [1]", multiMessageFDSetB64(t), "shipment-value", `{"tracking":"t-1"}`, []byte{0x01}},
		{"nested indexes [0,0]", nestedFDSetB64(t), "entry-value", `{"id":"e-1"}`, []byte{0x00, 0x00}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := newFakeRegistry()
			registry.register(tc.subject, tc.fdset, "PROTOBUF")
			client := newTestSRClient(t, registry)
			encoded, got, err := encodeForProduce(context.Background(), client, &SchemaRef{Subject: tc.subject, Format: "protobuf"}, []byte(tc.payload))
			if err != nil {
				t.Fatalf("encodeForProduce() error = %v", err)
			}
			if got.Format != "PROTOBUF" {
				t.Fatalf("format = %s, want PROTOBUF", got.Format)
			}
			if string(encoded[5:5+len(tc.wantWire5)]) != string(tc.wantWire5) {
				t.Fatalf("wire[5:] index segment = %v, want %v", encoded[5:10], tc.wantWire5)
			}
			// consume 侧：剥段后能 roundtrip 回 JSON。
			decoder := newSchemaDecoder(client, &SchemaRef{Subject: tc.subject, Format: "protobuf"})
			decoded, _, err := decoder.decode(context.Background(), encoded)
			if err != nil {
				t.Fatalf("decode() error = %v", err)
			}
			if !strings.Contains(string(decoded), `"`) {
				t.Fatalf("decoded = %q, want JSON text", decoded)
			}
		})
	}
}

// TestProtobufDecodeLegacyMissingIndexes：历史版本 produce 漏写 index 段的
// wire frame，消费侧剥段失败时按原始载荷回退解码成功。
func TestProtobufDecodeLegacyMissingIndexes(t *testing.T) {
	registry := newFakeRegistry()
	id, _ := registry.register("order-value", ordersFDSetB64(t), "PROTOBUF")
	client := newTestSRClient(t, registry)
	payload, err := encodeSchemaPayload([]byte(`{"id":"o-4","amount":"1","item":"bolt"}`), ordersFDSetB64(t), "PROTOBUF", "order-value")
	if err != nil {
		t.Fatalf("encodeSchemaPayload() error = %v", err)
	}
	legacy := encodeWireFrame(id, payload) // 无 index 段（历史形状）
	decoder := newSchemaDecoder(client, &SchemaRef{Subject: "order-value", Format: "protobuf"})
	decoded, info, err := decoder.decode(context.Background(), legacy)
	if err != nil {
		t.Fatalf("decode(legacy wire) error = %v", err)
	}
	if info.ID != id {
		t.Fatalf("info.ID = %d, want %d", info.ID, id)
	}
	if !strings.Contains(string(decoded), `"id":"o-4"`) {
		t.Fatalf("decoded = %s", decoded)
	}
}
