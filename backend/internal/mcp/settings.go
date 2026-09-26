// Package mcp 是 dbx-kafka-plugin 的 MCP 工具面（M3，照 ldap M1 Go 版骨架
// 同构移植）：`mcp/tools`（工具注册 + JSON Schema）、`mcp/call`（分派）、
// `mcp/settings/get|set`（可调参数）。UI intent 通道、digest/cursor 本地读、
// 两阶段写分文件实现（intent.go / cursor.go / confirm.go / digest.go）。
//
// 设计来源：shared/IMPL_PLAN_PLUGIN_MCP.zh-CN.md（v2）§1–§4/§6.3；
// 命名 <域>_<动作>、字段 camelCase（AGENTS.md 硬性规则 2）。
package mcp

import (
	"fmt"

	"io.dbx.kafka.plugin/internal/store"
)

// MCP 响应硬上限缺省 16 KiB（设计 §3：单工具响应上限 16 KiB，超出截断置
// truncated:true）；mcp/settings/set 可在 1 KiB..1 MiB 内调整。
const defaultResponseLimitBytes = 16 * 1024

// Settings 是 MCP 工具面的可调参数（持久化 <dataDir>/mcp-settings.json，
// 对齐 ssh/ldap mcp-settings.json 模式；损坏/缺字段逐项回落默认值）。
//
// 仅承载非敏感的尺寸/时长参数；digest 参数上限即设计硬上限
// （组数 ≤20、topN ≤10、样本 ≤5、rows ≤20、时间桶 ≤12），设置只可在
// 上限内取值。字段名与 ldap/files 版一致 + kafka 域内扩展。
type Settings struct {
	// ReportWaitMs UI intent report 等待时长（§1：默认 5s 可调）。
	ReportWaitMs int `json:"reportWaitMs"`
	// CellWidth 单元格截断宽度（§3：每单元格 120 字符；topic-partition-offset
	// 定位字段不截断）。
	CellWidth int `json:"cellWidth"`
	// DigestGroupLimit groupBy 组数上限（设计硬上限 20）。
	DigestGroupLimit int `json:"digestGroupLimit"`
	// DigestTopN distinct/topN 取样上限（设计硬上限 10）。
	DigestTopN int `json:"digestTopN"`
	// DigestSampleRows digest 样本行数（设计硬上限 5）。
	DigestSampleRows int `json:"digestSampleRows"`
	// DigestRowLimit format:"rows" 单次行数（设计硬上限 20）。
	DigestRowLimit int `json:"digestRowLimit"`
	// DigestScanLimit kafka 域内扩展：digest 扫描上限（maxScanRecords 语义，
	// 默认 1000；本地聚合在 sidecar 完成，上限只约束集群扫描量）。
	DigestScanLimit int `json:"digestScanLimit"`
	// CursorTtlSecs digest 会话翻页游标 TTL（缺省 600s；同族 files
	// cursorTtlSecs 同款语义，过期报文携带实际生效值）。
	CursorTtlSecs int `json:"cursorTtlSecs"`
	// MaxCursorSessions cursor 会话 LRU 容量（缺省 8；同族 files
	// maxCursorSessions 同款语义）。
	MaxCursorSessions int `json:"maxCursorSessions"`
	// ConfirmTtlSecs 两阶段 confirmToken TTL（缺省 60s；mcp/settings/set
	// 可调 10–600，files/ldap 同名同范围——已签发令牌的过期点不追溯）。
	ConfirmTtlSecs int `json:"confirmTtlSecs"`
	// ResponseLimitBytes 单工具响应上限（缺省 16 KiB）。
	ResponseLimitBytes int `json:"responseLimitBytes"`
}

// DefaultSettings 返回默认设置（数值与设计文档一致）。
func DefaultSettings() Settings {
	return Settings{
		ReportWaitMs:       5000,
		CellWidth:          120,
		DigestGroupLimit:   20,
		DigestTopN:         10,
		DigestSampleRows:   5,
		DigestRowLimit:     20,
		DigestScanLimit:    1000,
		CursorTtlSecs:      600,
		MaxCursorSessions:  8,
		ConfirmTtlSecs:     60,
		ResponseLimitBytes: defaultResponseLimitBytes,
	}
}

// settingsField 声明一个可 set 字段的取值边界（min..ceiling；min 缺省 1）。
type settingsField struct {
	name    string
	ceiling int
	min     int
}

// settingsFields mcp/settings/set 的白名单字段表：不在表内的字段一律拒绝
// （白名单而非黑名单，对齐 ssh settings_set 语义）。
var settingsFields = []settingsField{
	{"reportWaitMs", 30000, 1},
	{"cellWidth", 2000, 1},
	{"digestGroupLimit", 20, 1},
	{"digestTopN", 10, 1},
	{"digestSampleRows", 5, 1},
	{"digestRowLimit", 20, 1},
	{"digestScanLimit", 100000, 1},
	{"cursorTtlSecs", 3600, 10},
	{"maxCursorSessions", 32, 1},
	{"confirmTtlSecs", 600, 10},
	{"responseLimitBytes", 1024 * 1024, 1},
}

// Sanitized 把每个字段收敛进 1..上限（加载与 set 后都执行，越界配置不可达）。
func (s Settings) Sanitized() Settings {
	clamp := func(value, ceiling int) int {
		if value < 1 {
			return 1
		}
		if value > ceiling {
			return ceiling
		}
		return value
	}
	s.ReportWaitMs = clamp(s.ReportWaitMs, 30000)
	s.CellWidth = clamp(s.CellWidth, 2000)
	s.DigestGroupLimit = clamp(s.DigestGroupLimit, 20)
	s.DigestTopN = clamp(s.DigestTopN, 10)
	s.DigestSampleRows = clamp(s.DigestSampleRows, 5)
	s.DigestRowLimit = clamp(s.DigestRowLimit, 20)
	s.DigestScanLimit = clamp(s.DigestScanLimit, 100000)
	s.CursorTtlSecs = clamp(s.CursorTtlSecs, 3600)
	if s.CursorTtlSecs < 10 {
		s.CursorTtlSecs = 10
	}
	s.MaxCursorSessions = clamp(s.MaxCursorSessions, 32)
	s.ConfirmTtlSecs = clamp(s.ConfirmTtlSecs, 600)
	if s.ConfirmTtlSecs < 10 {
		s.ConfirmTtlSecs = 10
	}
	s.ResponseLimitBytes = clamp(s.ResponseLimitBytes, 1024*1024)
	return s
}

// settingsFileName MCP 设置文件名（数据目录内，明文、非敏感）。
const settingsFileName = "mcp-settings.json"

// LoadSettings 读取持久化设置；文件缺失或损坏时逐项回落默认值（不失败，
// 对齐 ssh McpLimits::load）。st 为 nil（数据目录不可用降级）时返回默认值。
func LoadSettings(st *store.Store) Settings {
	settings := DefaultSettings()
	if st == nil {
		return settings
	}
	var persisted Settings
	exists, err := st.LoadJSON(settingsFileName, &persisted)
	if err != nil || !exists {
		return settings
	}
	if persisted.ReportWaitMs > 0 {
		settings.ReportWaitMs = persisted.ReportWaitMs
	}
	if persisted.CellWidth > 0 {
		settings.CellWidth = persisted.CellWidth
	}
	if persisted.DigestGroupLimit > 0 {
		settings.DigestGroupLimit = persisted.DigestGroupLimit
	}
	if persisted.DigestTopN > 0 {
		settings.DigestTopN = persisted.DigestTopN
	}
	if persisted.DigestSampleRows > 0 {
		settings.DigestSampleRows = persisted.DigestSampleRows
	}
	if persisted.DigestRowLimit > 0 {
		settings.DigestRowLimit = persisted.DigestRowLimit
	}
	if persisted.DigestScanLimit > 0 {
		settings.DigestScanLimit = persisted.DigestScanLimit
	}
	if persisted.CursorTtlSecs > 0 {
		settings.CursorTtlSecs = persisted.CursorTtlSecs
	}
	if persisted.MaxCursorSessions > 0 {
		settings.MaxCursorSessions = persisted.MaxCursorSessions
	}
	if persisted.ResponseLimitBytes > 0 {
		settings.ResponseLimitBytes = persisted.ResponseLimitBytes
	}
	if persisted.ConfirmTtlSecs > 0 {
		settings.ConfirmTtlSecs = persisted.ConfirmTtlSecs
	}
	return settings.Sanitized()
}

// SaveSettings 原子写入设置文件；st 为 nil 时静默跳过（进程内仍然生效）。
func SaveSettings(st *store.Store, settings Settings) error {
	if st == nil {
		return nil
	}
	return st.SaveJSON(settingsFileName, settings.Sanitized())
}

// applySettingsUpdate 白名单逐字段校验并更新（部分更新：未出现的字段不动）。
func applySettingsUpdate(settings Settings, updates map[string]any) (Settings, error) {
	for _, field := range settingsFields {
		raw, present := updates[field.name]
		if !present {
			continue
		}
		number, ok := raw.(float64)
		if !ok {
			return settings, fmt.Errorf("%s must be a positive integer", field.name)
		}
		value := int(number)
		if value < field.min || value > field.ceiling {
			return settings, fmt.Errorf("%s must be between %d and %d", field.name, field.min, field.ceiling)
		}
		switch field.name {
		case "reportWaitMs":
			settings.ReportWaitMs = value
		case "cellWidth":
			settings.CellWidth = value
		case "digestGroupLimit":
			settings.DigestGroupLimit = value
		case "digestTopN":
			settings.DigestTopN = value
		case "digestSampleRows":
			settings.DigestSampleRows = value
		case "digestRowLimit":
			settings.DigestRowLimit = value
		case "digestScanLimit":
			settings.DigestScanLimit = value
		case "cursorTtlSecs":
			settings.CursorTtlSecs = value
		case "maxCursorSessions":
			settings.MaxCursorSessions = value
		case "confirmTtlSecs":
			settings.ConfirmTtlSecs = value
		case "responseLimitBytes":
			settings.ResponseLimitBytes = value
		}
	}
	return settings.Sanitized(), nil
}
