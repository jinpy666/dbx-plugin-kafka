package mcp

// util.go：mcp 包内参数解析 / 投影小工具（纯函数；与 ldap/files 版同构）。

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// getContext 请求级 context（jsonl SDK 无 ctx 传递；超时由 kafkaconn 层
// 按操作控制，与 main.go 同语义）。
func getContext() context.Context {
	return context.Background()
}

// stringField 读取字符串字段（缺失/类型不符返回空串）。
func stringField(params map[string]any, key string) string {
	if value, ok := params[key].(string); ok {
		return value
	}
	return ""
}

// missingRequired 一次枚举全部缺失的 required 参数（ssh 同款语义，
// MCP_ACCEPTANCE §3.9）：`Missing required parameters: a, b`——LLM 调用方
// 一轮补齐所有缺口，而不是逐个 fail-fast 往返。keys 按 schema required
// 顺序传入，报错顺序即该顺序。缺失判定 = 键不存在或显式 null；present-but
// 类型错误（空串/类型不符）不在此点名，由后续各参数的精确校验单独报出。
func missingRequired(args map[string]any, keys ...string) error {
	missing := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, present := args[key]; !present || value == nil {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("Missing required parameters: %s", strings.Join(missing, ", "))
}

// coerceInt 整数参数宽容解析：JSON number、整数字符串（LLM 常见把整数写成
// 字符串的变体，如 partition:"0"）都接受；无法解析返回 (0,false)，由调用方
// 决定报错还是走缺省——绝不静默按 0 执行。小数 float 显式拒绝（评审 M：
// partition:1.9 静默截成 1 违反「绝不静默折算」红线；与文件内 timestampMs/
// schema.version 的拒绝语义对齐）。
func coerceInt(raw any) (int, bool) {
	value, ok := coerceInt64(raw)
	if !ok || value < math.MinInt || value > math.MaxInt {
		return 0, false
	}
	return int(value), true
}

// coerceInt64 coerceInt 的 int64 版（timestampMs / 时间窗等大整数）。
// 小数 float64 与超出 int64 表示范围的值都拒绝（回绕/截断都会静默改变语义）。
func coerceInt64(raw any) (int64, bool) {
	switch value := raw.(type) {
	case float64:
		if value != math.Trunc(value) || value < -9.223372036854776e18 || value >= 9.223372036854776e18 {
			return 0, false
		}
		return int64(value), true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

// coerceBool 布尔参数宽容解析：bool、字符串 "true"/"false"/"1"/"0"/
// "yes"/"no"/"on"/"off"（大小写不敏感；变体面与 ssh arg_bool 同族一致）；
// 其余返回 (false,false)。
func coerceBool(raw any) (bool, bool) {
	switch value := raw.(type) {
	case bool:
		return value, true
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "1", "yes", "on":
			return true, true
		case "false", "0", "no", "off":
			return false, true
		}
		return false, false
	default:
		return false, false
	}
}

// intArg 读取整数参数（JSON number 或整数字符串；缺失/非法返回 0）。
func intArg(raw any) int {
	value, _ := coerceInt(raw)
	return value
}

// boolArg 读取布尔参数（缺省 false）。
func boolArg(raw any) bool {
	value, _ := raw.(bool)
	return value
}

// offsetArg 读取分页 offset：字段缺失 = -1（续读会话内游标）；显式给出
// （含 0）时按调用方指定的起点。
func offsetArg(args map[string]any) int {
	if _, present := args["offset"]; !present {
		return -1
	}
	return intArg(args["offset"])
}

// stringSlice 读取字符串数组参数。
func stringSlice(raw any) []string {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	return out
}

// clampStrings 名称清单截断（上限内保留）。
func clampStrings(names []string, limit int) []string {
	if limit <= 0 || len(names) <= limit {
		return names
	}
	return names[:limit]
}

// payloadSize 序列化体积（响应上限判定用）。
func payloadSize(value any) int {
	data, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return len(data)
}

// cloneMap 浅复制（响应截断前先复制，不污染调用方数据）。
func cloneMap(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}
