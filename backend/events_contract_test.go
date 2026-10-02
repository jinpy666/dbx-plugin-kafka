package main

// events_contract_test.go：事件载荷契约注册表对拍（shared/contracts/events.json
// 是单一真相）。注册表覆盖三个 sidecar 事件：stream/messages（结构体反射
// 对拍）、stream/error（载荷构造函数）、audit（反射 json tag + result 折算
// 枚举）。新增/修改事件载荷时本测试红灯，强制同步注册表——防止再次出现
// bufferSize / audit result 那类「mock 全绿、生产漂移」的事故。

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"io.dbx.kafka.plugin/internal/kafkaconn"
)

type eventContract struct {
	Events map[string]struct {
		Keys         []string `json:"keys"`
		OptionalKeys []string `json:"optionalKeys"`
		ResultEnum   []string `json:"resultEnum"`
	} `json:"events"`
}

func loadEventContract(t *testing.T) eventContract {
	t.Helper()
	raw, err := os.ReadFile("../shared/contracts/events.json")
	if err != nil {
		t.Fatalf("read events.json: %v", err)
	}
	var contract eventContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("parse events.json: %v", err)
	}
	if len(contract.Events) == 0 {
		t.Fatal("events.json registry is empty")
	}
	return contract
}

// jsonTagSet 反射取结构体 json tag 集；omitempty 归入 optional。
func jsonTagSet(v any) (required, optional map[string]bool) {
	required = map[string]bool{}
	optional = map[string]bool{}
	elem := reflect.TypeOf(v)
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	for i := 0; i < elem.NumField(); i++ {
		parts := strings.Split(elem.Field(i).Tag.Get("json"), ",")
		name := parts[0]
		if name == "" || name == "-" {
			continue
		}
		omit := false
		for _, opt := range parts[1:] {
			if opt == "omitempty" {
				omit = true
				break
			}
		}
		if omit {
			optional[name] = true
		} else {
			required[name] = true
		}
	}
	return required, optional
}

func assertKeysMatch(t *testing.T, event string, got map[string]bool, want []string) {
	t.Helper()
	wantSet := map[string]bool{}
	for _, key := range want {
		wantSet[key] = true
		if !got[key] {
			t.Errorf("%s: registry key %q missing from payload", event, key)
		}
	}
	for key := range got {
		if !wantSet[key] {
			t.Errorf("%s: payload key %q not in registry (drift)", event, key)
		}
	}
}

func TestEventContractRegistry(t *testing.T) {
	contract := loadEventContract(t)

	// kafka/stream/messages：与 StreamMessageBatch 反射 json tag 全等。
	stream, ok := contract.Events["kafka/stream/messages"]
	if !ok {
		t.Fatal("kafka/stream/messages missing from registry")
	}
	required, _ := jsonTagSet(kafkaconn.StreamMessageBatch{})
	assertKeysMatch(t, "kafka/stream/messages", required, stream.Keys)

	// kafka/stream/error：载荷构造函数键面全等。
	errEvent, ok := contract.Events["kafka/stream/error"]
	if !ok {
		t.Fatal("kafka/stream/error missing from registry")
	}
	payload := streamErrorEventPayload("s1", "boom")
	got := map[string]bool{}
	for key := range payload {
		got[key] = true
	}
	assertKeysMatch(t, "kafka/stream/error", got, errEvent.Keys)

	// kafka/audit：AuditRecord json tag 全等（detail/source 为 omitempty 可选）。
	audit, ok := contract.Events["kafka/audit"]
	if !ok {
		t.Fatal("kafka/audit missing from registry")
	}
	auditRequired, auditOptional := jsonTagSet(kafkaconn.AuditRecord{})
	auditAll := map[string]bool{}
	for key := range auditRequired {
		auditAll[key] = true
	}
	for key := range auditOptional {
		auditAll[key] = true
	}
	assertKeysMatch(t, "kafka/audit", auditAll, audit.Keys)
	optionalSet := map[string]bool{}
	for _, key := range audit.OptionalKeys {
		optionalSet[key] = true
	}
	for key := range auditOptional {
		if !optionalSet[key] {
			t.Errorf("kafka/audit: struct omitempty key %q not marked optionalKeys in registry", key)
		}
	}
	// result 折算枚举：store/事件两通道同面（success→ok、blocked→denied）。
	wantEnum := map[string]bool{}
	for _, value := range audit.ResultEnum {
		wantEnum[value] = true
	}
	for _, raw := range []string{"success", "blocked", "error"} {
		converted := auditResultForStore(raw)
		if !wantEnum[converted] {
			t.Errorf("auditResultForStore(%q) = %q not in registry resultEnum %v", raw, converted, audit.ResultEnum)
		}
		if got := auditEventRecord(kafkaconn.AuditRecord{Result: raw}).Result; got != converted {
			t.Errorf("auditEventRecord(%q).Result = %q, want %q", raw, got, converted)
		}
	}
}
