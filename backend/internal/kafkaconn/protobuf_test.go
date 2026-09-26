package kafkaconn

// protobuf_test.go：PROTOBUF 载荷编解码单测（Phase 3 §12.2.2 / §12.4 G 矩阵）。
// 向量直接复用 kafka-seed fixture orders_fdset.b64（com.dbx.test.Order，
// 由 cmd/gen-protobuf-fixture 程序化生成入库，运行时无需 protoc）；消歧
// 三分支测试在测试内程序化构造多 message FDSet。全部纯函数路径，不连网。

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ordersFDSetB64 读取 seed fixture（与 orders.proto 逐字段一致的 FDSet b64）。
func ordersFDSetB64(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "scripts", "kafka-seed", "protobuf", "orders_fdset.b64"))
	if err != nil {
		t.Fatalf("read orders_fdset.b64: %v", err)
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		t.Fatal("orders_fdset.b64 is empty")
	}
	return trimmed
}

// multiMessageFDSetB64 构造含两个 message（Order/Shipment）的 FDSet b64，
// 用于消歧分支测试。
func multiMessageFDSetB64(t *testing.T) string {
	t.Helper()
	field := func(name string, number int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(number),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:   typ.Enum(),
		}
	}
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	fdset := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("multi.proto"),
		Package: proto.String("com.dbx.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Order"), Field: []*descriptorpb.FieldDescriptorProto{field("id", 1, str)}},
			{Name: proto.String("Shipment"), Field: []*descriptorpb.FieldDescriptorProto{field("tracking", 1, str)}},
		},
	}}}
	raw, err := proto.Marshal(fdset)
	if err != nil {
		t.Fatalf("marshal FDSet: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestProtobufPayloadRoundtrip(t *testing.T) {
	fdset := ordersFDSetB64(t)
	text := []byte(`{"id":"o-1","amount":"5","item":"widget"}`)
	encoded, err := encodeSchemaPayload(text, fdset, "PROTOBUF", "order-value")
	if err != nil {
		t.Fatalf("encodeSchemaPayload(protobuf) error = %v", err)
	}
	if len(encoded) == 0 || encoded[0] == '{' {
		t.Fatalf("encoded payload is not protobuf binary: %q", encoded)
	}
	// proto3 无字段时的空消息也是合法编码；此处三个字段都应非空。
	decoded, err := decodeSchemaPayload(encoded, fdset, "PROTOBUF", "order-value")
	if err != nil {
		t.Fatalf("decodeSchemaPayload(protobuf) error = %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(decoded, &back); err != nil {
		t.Fatalf("decoded payload is not JSON: %v", err)
	}
	// protojson 输出 proto 字段名；int64 以字符串表示（§12.7 已知差异，
	// 与 Confluent 序列化器 JSON 渲染不强行对齐）。
	if back["id"] != "o-1" || back["item"] != "widget" {
		t.Errorf("decoded = %s", decoded)
	}
	if back["amount"] != "5" {
		t.Errorf("int64 amount = %v (%s), want string \"5\"", back["amount"], decoded)
	}
}

func TestProtobufDecodeViaWireFrameAndRegistry(t *testing.T) {
	// 全链路（S14 同款）：SR 元数据（schemaType=PROTOBUF，schema=b64 FDSet）
	// → produce 挂载编码 wire frame → decode 反查元数据解码。
	registry := newFakeRegistry()
	id, _ := registry.register("order-value", ordersFDSetB64(t), "PROTOBUF")
	server := newTestHTTPServer(t, registry.handler)
	client, err := newSchemaRegistryClient(Profile{SRURL: server.URL}, connSecrets{})
	if err != nil {
		t.Fatalf("newSchemaRegistryClient() error = %v", err)
	}
	ref := &SchemaRef{Subject: "order-value", Format: "protobuf"}
	encoded, got, err := encodeForProduce(context.Background(), client, ref, []byte(`{"id":"o-2","amount":"7","item":"bolt"}`))
	if err != nil {
		t.Fatalf("encodeForProduce(protobuf) error = %v", err)
	}
	if got.ID != id || got.Format != "PROTOBUF" {
		t.Errorf("encode result = %+v (want id=%d format=PROTOBUF)", got, id)
	}
	decoder := newSchemaDecoder(client, nil)
	decoded, info, err := decoder.decode(context.Background(), encoded)
	if err != nil {
		t.Fatalf("decode(protobuf wire) error = %v", err)
	}
	if info.ID != id {
		t.Errorf("info.ID = %d, want %d", info.ID, id)
	}
	if !strings.Contains(string(decoded), `"id":"o-2"`) {
		t.Errorf("decoded = %s", decoded)
	}
}

func TestProtobufMessageDisambiguation(t *testing.T) {
	fdset := multiMessageFDSetB64(t)
	payload := []byte{0x0a, 0x02, 'a', 'b'} // field1 len-delim "ab"（两 message 各自 field1）

	// 分支 2：subject 剥 -value/-key 后缀 + PascalCase 尾段唯一命中
	//（注意：PascalCase 是严格相等——复数 topic 名 "orders" 归一为 "Orders"，
	// 不会命中单数 message "Order"；subject 应与 message 名对齐）。
	for _, subject := range []string{"order-value", "order-key", "order", "com.dbx.test.order-value"} {
		decoded, err := decodeSchemaPayload(payload, fdset, "PROTOBUF", subject)
		if err != nil {
			t.Errorf("decode(subject=%q) error = %v", subject, err)
			continue
		}
		if !strings.Contains(string(decoded), `"id":"ab"`) {
			t.Errorf("decode(subject=%q) = %s, want Order render", subject, decoded)
		}
	}
	if _, err := decodeSchemaPayload(payload, fdset, "PROTOBUF", "shipment-value"); err != nil {
		t.Errorf("decode(shipment-value) error = %v", err)
	}

	// 分支 1：即使 subject 对不上，单 message FDSet 也唯一可用。
	single := ordersFDSetB64(t)
	if _, err := decodeSchemaPayload(payload, single, "PROTOBUF", "unrelated-subject"); err != nil {
		t.Errorf("single-message FDSet decode with unrelated subject error = %v", err)
	}

	// 分支 3：多 message + 无法唯一 → 报错列出候选全名。
	_, err := decodeSchemaPayload(payload, fdset, "PROTOBUF", "unrelated-subject")
	if err == nil {
		t.Fatal("ambiguous decode expected error")
	}
	for _, candidate := range []string{"com.dbx.test.Order", "com.dbx.test.Shipment"} {
		if !strings.Contains(err.Error(), candidate) {
			t.Errorf("error %q missing candidate %q", err, candidate)
		}
	}
}

func TestProtobufBadFDSet(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{"empty", ""},
		{"not base64", "not-base64!!"},
		{"base64 garbage", base64.StdEncoding.EncodeToString([]byte{0xde, 0xad, 0xbe, 0xef})},
		{"fdset without message", func() string {
			raw, _ := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
				Name: proto.String("empty.proto"), Package: proto.String("x"), Syntax: proto.String("proto3"),
			}}})
			return base64.StdEncoding.EncodeToString(raw)
		}()},
	}
	for _, tc := range cases {
		if _, err := decodeSchemaPayload([]byte{0x01}, tc.schema, "PROTOBUF", ""); err == nil {
			t.Errorf("%s: decode accepted invalid FDSet", tc.name)
		}
		if _, err := encodeSchemaPayload([]byte(`{}`), tc.schema, "PROTOBUF", ""); err == nil {
			t.Errorf("%s: encode accepted invalid FDSet", tc.name)
		}
	}
}

func TestProtobufPayloadNotWireFormat(t *testing.T) {
	// FDSet 合法但载荷不是合法 protobuf wire 数据（截断的 length-delim 字段）。
	fdset := ordersFDSetB64(t)
	if _, err := decodeSchemaPayload([]byte{0x0a, 0x05, 'x'}, fdset, "PROTOBUF", "order-value"); err == nil {
		t.Error("truncated protobuf payload accepted")
	}
	// JSON 载荷不符合 protojson 语义（id 非字符串）→ 编码报错。
	if _, err := encodeSchemaPayload([]byte(`{"id":123}`), fdset, "PROTOBUF", "order-value"); err == nil {
		t.Error("protojson-incompatible payload accepted")
	}
	// 非 wire format 帧（decodeWireFrame 层）依旧明确报错。
	if _, _, err := decodeWireFrame([]byte("plain-text")); err == nil {
		t.Error("plain value expected wire format error")
	}
}

func TestProtobufSubjectTailHelpers(t *testing.T) {
	cases := []struct {
		subject, want string
	}{
		{"orders-value", "orders"},
		{"order-value", "order"},
		{"order-key", "order"},
		{"orders", "orders"},
		{"com.dbx.test.orders-value", "orders"},
		{"dbx/orders-value", "orders"},
		{"plain-orders", "plain-orders"}, // 非 -key/-value 后缀不剥
		{"", ""},
	}
	for _, tc := range cases {
		if got := protobufSubjectTail(tc.subject); got != tc.want {
			t.Errorf("protobufSubjectTail(%q) = %q, want %q", tc.subject, got, tc.want)
		}
	}
	if got := pascalCase("order_items"); got != "OrderItems" {
		t.Errorf("pascalCase(order_items) = %q", got)
	}
	if got := pascalCase("orders"); got != "Orders" {
		t.Errorf("pascalCase(orders) = %q", got)
	}
	if got := messageFullNameTail("com.dbx.test.Order"); got != "Order" {
		t.Errorf("messageFullNameTail = %q", got)
	}
}
