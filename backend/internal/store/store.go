// Package store 管理插件数据目录下的本地数据与审计
// （shared/IMPL_PLAN_M0_COMMON.zh-CN.md §4）。
//
//	<数据目录>/                       解析顺序见 resolveDataDir
//	├── prefs.json                   UI 偏好（非敏感）
//	├── presets.json                 Kafka 消费/过滤预设（明文，不含凭据）
//	└── audit.jsonl                  写操作审计（append-only）
//
// 数据目录必须是持久化路径（保存重启后仍在的偏好/审计）：macOS 的
// $TMPDIR 重启即清空，因此 fallback 走平台标准用户数据目录，os.TempDir()
// 仅作为环境全缺的最后兜底。
//
// 凭据红线：任何文件不落密码/私钥/token；审计记录 target 只记 topic/group
// 等资源名，不记消息值。
//
// 目录解析顺序是 ssh/files/ldap/kafka 四插件统一规范（M0 §4），各插件
// 实现同构，仅 DefaultDirName 不同。
package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// DefaultDirName 是解析出的数据目录的末级目录名。
const DefaultDirName = "io.dbx.kafka"

// EnvDataDir 是宿主注入的插件数据目录环境变量。
const EnvDataDir = "DBX_PLUGIN_DATA_DIR"

// EnvDataRoot 是宿主便携/web 模式注入的数据根环境变量（子进程继承）。
const EnvDataRoot = "DBX_DATA_DIR"

// Store 绑定一个数据目录。
type Store struct {
	dir string
}

// resolveDataDir 解析插件数据目录，按顺序取第一个可用项
// （"可用" = 环境变量存在且 TrimSpace 后非空）：
//
//  1. DBX_PLUGIN_DATA_DIR → 原样使用（宿主显式注入，未来方案 A 接入点）。
//  2. DBX_DATA_DIR → <DBX_DATA_DIR>/plugin-data/io.dbx.kafka
//     （宿主便携/web 模式；用 plugin-data/ 而非 plugins/ 避开安装器注册树）。
//  3. 平台标准用户数据目录下 dbx-plugin-data/io.dbx.kafka：
//     darwin：$HOME/Library/Application Support/...；
//     其他 unix：${XDG_DATA_HOME:-$HOME/.local/share}/...；
//     windows：%APPDATA%\...。
//  4. 以上全缺（HOME 未设等）→ $TMPDIR/dbx-plugin-data/io.dbx.kafka
//     最后兜底，永不失败。
//
// 纯函数：getenv 与 goos 注入，便于单测平台分支（不依赖全局环境）。
// 注意不用 os.UserConfigDir()（Linux 上对应 XDG_CONFIG_HOME，语义是
// config 不是 data），保证四插件路径一致。
func resolveDataDir(getenv func(string) string, goos string) string {
	if v := strings.TrimSpace(getenv(EnvDataDir)); v != "" {
		return v
	}
	if v := strings.TrimSpace(getenv(EnvDataRoot)); v != "" {
		return filepath.Join(v, "plugin-data", DefaultDirName)
	}
	switch goos {
	case "darwin":
		if home := strings.TrimSpace(getenv("HOME")); home != "" {
			return filepath.Join(home, "Library", "Application Support", "dbx-plugin-data", DefaultDirName)
		}
	case "windows":
		if appdata := strings.TrimSpace(getenv("APPDATA")); appdata != "" {
			return filepath.Join(appdata, "dbx-plugin-data", DefaultDirName)
		}
	default:
		if xdg := strings.TrimSpace(getenv("XDG_DATA_HOME")); xdg != "" {
			return filepath.Join(xdg, "dbx-plugin-data", DefaultDirName)
		}
		if home := strings.TrimSpace(getenv("HOME")); home != "" {
			return filepath.Join(home, ".local", "share", "dbx-plugin-data", DefaultDirName)
		}
	}
	return filepath.Join(os.TempDir(), "dbx-plugin-data", DefaultDirName)
}

// Open 打开插件数据目录：按 resolveDataDir 顺序解析（宿主注入变量 →
// DBX_DATA_DIR → 平台标准用户数据目录 → TempDir 最后兜底）；
// 目录不存在则创建。
func Open() (*Store, error) {
	return OpenAt(resolveDataDir(os.Getenv, runtime.GOOS))
}

// OpenAt 显式指定数据目录（测试/工具用）。
func OpenAt(dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("store: data dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: create data dir: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Dir 返回数据目录绝对路径。
func (s *Store) Dir() string {
	return s.dir
}

// --- 通用 JSON 文件（prefs.json / presets.json） ---

// LoadJSON 读取 dataDir/<name> 到 out；文件不存在时不修改 out 并返回 false。
func (s *Store) LoadJSON(name string, out any) (bool, error) {
	path := filepath.Join(s.dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("store: read %s: %w", name, err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return false, fmt.Errorf("store: parse %s: %w", name, err)
	}
	return true, nil
}

// SaveJSON 原子写入 dataDir/<name>（临时文件 + rename，权限 0600）。
func (s *Store) SaveJSON(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode %s: %w", name, err)
	}
	data = append(data, '\n')
	path := filepath.Join(s.dir, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("store: write %s: %w", name, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("store: rename %s: %w", name, err)
	}
	return nil
}

// --- prefs.json ---

// LoadPrefs 读取 prefs.json；不存在时返回空 map。
func (s *Store) LoadPrefs() (map[string]any, error) {
	prefs := map[string]any{}
	if _, err := s.LoadJSON("prefs.json", &prefs); err != nil {
		return nil, err
	}
	return prefs, nil
}

// SavePrefs 覆盖写入 prefs.json。
func (s *Store) SavePrefs(prefs map[string]any) error {
	if prefs == nil {
		prefs = map[string]any{}
	}
	return s.SaveJSON("prefs.json", prefs)
}

// --- audit.jsonl ---

// AuditRecord 是 audit.jsonl 的一行（M0 §4 格式，字段名与 ssh-sftp 对齐）。
type AuditRecord struct {
	Time         string `json:"time"`         // RFC3339
	ConnectionID string `json:"connectionId"` // 宿主连接 id
	Action       string `json:"action"`       // 如 kafka/topics/delete
	Target       string `json:"target"`       // 资源名（topic/group/acl 描述，不记值）
	Result       string `json:"result"`       // ok | denied | error
	// Source 操作来源（MCP 设计 §4：MCP 写路径 "mcp"；工作台路径缺省不写，
	// additive 字段，旧读取方忽略未知键）。
	Source string `json:"source,omitempty"`
}

// AppendAudit 追加一条审计记录；rec.Time 为空时取当前时间（RFC3339）。
// append-only：每次独立 open + 单次 O_APPEND write + close，内核保证单条
// write 不与其他写者撕裂（并发写者间行序不定；进程内不额外加锁）。
func (s *Store) AppendAudit(rec AuditRecord) error {
	if strings.TrimSpace(rec.Time) == "" {
		rec.Time = time.Now().Format(time.RFC3339)
	}
	if strings.TrimSpace(rec.Result) == "" {
		rec.Result = "ok"
	}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("store: encode audit record: %w", err)
	}
	path := filepath.Join(s.dir, "audit.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("store: open audit.jsonl: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("store: append audit.jsonl: %w", err)
	}
	return nil
}

// ReadAuditLines 读取全部审计行（测试/排障用，主流程不使用）。
func (s *Store) ReadAuditLines() ([]AuditRecord, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, "audit.jsonl"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	out := []AuditRecord{}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec AuditRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return out, fmt.Errorf("store: parse audit line: %w", err)
		}
		out = append(out, rec)
	}
	return out, nil
}
