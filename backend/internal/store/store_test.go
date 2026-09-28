package store

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := OpenAt(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("OpenAt() error = %v", err)
	}
	return st
}

func TestOpenCreatesDir(t *testing.T) {
	base := t.TempDir()
	st, err := OpenAt(filepath.Join(base, "nested", "data"))
	if err != nil {
		t.Fatalf("OpenAt() error = %v", err)
	}
	info, err := os.Stat(st.Dir())
	if err != nil || !info.IsDir() {
		t.Fatalf("data dir not created: %v", err)
	}
}

// TestOpenResolvesDataDir 集成冒烟：无 DBX_PLUGIN_DATA_DIR 时 Open() 也解析到
// 持久化目录（basename 固定），不再依赖会被重启清空的 os.TempDir()。平台分支
// 细节由 TestResolveDataDir 用 fake getenv 覆盖，此处不做环境断言。
func TestOpenResolvesDataDir(t *testing.T) {
	t.Setenv(EnvDataDir, "")
	st, err := Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if st.Dir() == "" || filepath.Base(st.Dir()) != DefaultDirName {
		t.Errorf("resolved dir = %q", st.Dir())
	}
	if DefaultDirName != "io.dbx.kafka" {
		t.Errorf("DefaultDirName = %q, want io.dbx.kafka", DefaultDirName)
	}
}

// TestResolveDataDir 覆盖数据目录解析全部分支。平台分支用显式 goos +
// fake getenv 测试，不改全局环境（不用 t.Setenv）。
func TestResolveDataDir(t *testing.T) {
	env := func(kv map[string]string) func(string) string {
		return func(key string) string { return kv[key] }
	}
	tmpFallback := filepath.Join(os.TempDir(), "dbx-plugin-data", DefaultDirName)

	cases := []struct {
		name string
		goos string
		kv   map[string]string
		want string
	}{
		{
			// ① DBX_PLUGIN_DATA_DIR 优先，原样使用。
			name: "plugin data dir wins",
			goos: "darwin",
			kv:   map[string]string{"DBX_PLUGIN_DATA_DIR": "/host/injected", "DBX_DATA_DIR": "/root", "HOME": "/home/u", "XDG_DATA_HOME": "/xdg"},
			want: "/host/injected",
		},
		{
			// ② 空白字符串视为未设 → 落到 DBX_DATA_DIR。
			name: "blank plugin data dir falls through",
			goos: "linux",
			kv:   map[string]string{"DBX_PLUGIN_DATA_DIR": "  \t ", "DBX_DATA_DIR": "/data/root"},
			want: filepath.Join("/data/root", "plugin-data", DefaultDirName),
		},
		{
			// ③ DBX_DATA_DIR 生效，拼 plugin-data 子目录。
			name: "dbx data dir",
			goos: "darwin",
			kv:   map[string]string{"DBX_DATA_DIR": "/data/root", "HOME": "/home/u"},
			want: filepath.Join("/data/root", "plugin-data", DefaultDirName),
		},
		{
			// ④ darwin：HOME/Library/Application Support。
			name: "darwin home",
			goos: "darwin",
			kv:   map[string]string{"HOME": "/Users/jinpy"},
			want: filepath.Join("/Users/jinpy", "Library", "Application Support", "dbx-plugin-data", DefaultDirName),
		},
		{
			// ⑤ linux：XDG_DATA_HOME 已设。
			name: "linux xdg data home set",
			goos: "linux",
			kv:   map[string]string{"XDG_DATA_HOME": "/xdg/data", "HOME": "/home/u"},
			want: filepath.Join("/xdg/data", "dbx-plugin-data", DefaultDirName),
		},
		{
			// ⑤ linux：XDG_DATA_HOME 未设 → $HOME/.local/share。
			name: "linux xdg data home unset",
			goos: "linux",
			kv:   map[string]string{"HOME": "/home/u"},
			want: filepath.Join("/home/u", ".local", "share", "dbx-plugin-data", DefaultDirName),
		},
		{
			// ⑤ linux：XDG_DATA_HOME 空白视为未设。
			name: "linux blank xdg data home uses home",
			goos: "linux",
			kv:   map[string]string{"XDG_DATA_HOME": "  ", "HOME": "/home/u"},
			want: filepath.Join("/home/u", ".local", "share", "dbx-plugin-data", DefaultDirName),
		},
		{
			// ⑥ windows：APPDATA。
			name: "windows appdata",
			goos: "windows",
			kv:   map[string]string{"APPDATA": `C:\Users\j\AppData\Roaming`},
			want: filepath.Join(`C:\Users\j\AppData\Roaming`, "dbx-plugin-data", DefaultDirName),
		},
		{
			// ⑦ 全缺（HOME 未设等）→ TempDir 最后兜底。
			name: "linux all missing falls back to temp dir",
			goos: "linux",
			kv:   map[string]string{},
			want: tmpFallback,
		},
		{
			name: "windows without appdata falls back to temp dir",
			goos: "windows",
			kv:   map[string]string{},
			want: tmpFallback,
		},
		{
			name: "darwin blank home falls back to temp dir",
			goos: "darwin",
			kv:   map[string]string{"HOME": "   "},
			want: tmpFallback,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveDataDir(env(tc.kv), tc.goos); got != tc.want {
				t.Errorf("resolveDataDir(kv=%v, goos=%q) = %q, want %q", tc.kv, tc.goos, got, tc.want)
			}
		})
	}
}

func TestOpenEnvOverride(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(EnvDataDir, dir)
	st, err := Open()
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if st.Dir() != dir {
		t.Errorf("dir = %q, want %q", st.Dir(), dir)
	}
}

func TestPrefsRoundTrip(t *testing.T) {
	st := openTestStore(t)

	// 不存在 → 空 map，无错误。
	prefs, err := st.LoadPrefs()
	if err != nil {
		t.Fatalf("LoadPrefs() error = %v", err)
	}
	if len(prefs) != 0 {
		t.Fatalf("LoadPrefs() = %v, want empty", prefs)
	}

	prefs["tree.sort"] = "name"
	prefs["stream.autoscroll"] = true
	prefs["page.size"] = float64(50)
	if err := st.SavePrefs(prefs); err != nil {
		t.Fatalf("SavePrefs() error = %v", err)
	}

	loaded, err := st.LoadPrefs()
	if err != nil {
		t.Fatalf("LoadPrefs() error = %v", err)
	}
	if loaded["tree.sort"] != "name" || loaded["stream.autoscroll"] != true || loaded["page.size"] != float64(50) {
		t.Errorf("LoadPrefs() round-trip mismatch: %v", loaded)
	}

	// SavePrefs(nil) 写空对象而非报错。
	if err := st.SavePrefs(nil); err != nil {
		t.Fatalf("SavePrefs(nil) error = %v", err)
	}
}

func TestSaveJSONIsAtomicAndPrivate(t *testing.T) {
	st := openTestStore(t)
	if err := st.SaveJSON("presets.json", map[string]any{"v": 1}); err != nil {
		t.Fatalf("SaveJSON() error = %v", err)
	}
	info, err := os.Stat(filepath.Join(st.Dir(), "presets.json"))
	if err != nil {
		t.Fatalf("stat presets.json: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("presets.json perm = %v, want 0600", perm)
	}
	// 无临时文件残留。
	if _, err := os.Stat(filepath.Join(st.Dir(), "presets.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("tmp file leftover: %v", err)
	}
}

func TestAppendAuditFormat(t *testing.T) {
	st := openTestStore(t)

	rec := AuditRecord{
		ConnectionID: "conn-1",
		Action:       "kafka/topics/delete",
		Target:       "orders",
		Result:       "ok",
	}
	if err := st.AppendAudit(rec); err != nil {
		t.Fatalf("AppendAudit() error = %v", err)
	}
	// 第二条：Time/Result 留空走兜底。
	if err := st.AppendAudit(AuditRecord{ConnectionID: "conn-1", Action: "kafka/messages/produce", Target: "orders", Result: "denied"}); err != nil {
		t.Fatalf("AppendAudit() #2 error = %v", err)
	}

	lines, err := st.ReadAuditLines()
	if err != nil {
		t.Fatalf("ReadAuditLines() error = %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("audit lines = %d, want 2", len(lines))
	}
	if _, err := time.Parse(time.RFC3339, lines[0].Time); err != nil {
		t.Errorf("audit time not RFC3339: %q (%v)", lines[0].Time, err)
	}
	if lines[0].Action != "kafka/topics/delete" || lines[0].Target != "orders" || lines[0].Result != "ok" {
		t.Errorf("line0 = %+v", lines[0])
	}
	if lines[1].Result != "denied" {
		t.Errorf("line1 result = %q, want denied", lines[1].Result)
	}
}

func TestReadAuditLinesEmpty(t *testing.T) {
	st := openTestStore(t)
	lines, err := st.ReadAuditLines()
	if err != nil {
		t.Fatalf("ReadAuditLines() error = %v", err)
	}
	if lines != nil && len(lines) != 0 {
		t.Errorf("ReadAuditLines() = %v, want nil/empty", lines)
	}
}

// S-SAVE-RACE（评审 M-2）：SaveJSON 此前用固定 <name>.tmp + 无 fsync——
// 并发写同一文件时不同长度的交错可产生损坏的正式文件。唯一临时名 +
// Sync + Rename 后，最终文件必须始终是某一次完整写入且无临时文件残留。
func TestSaveJSONConcurrentValidFile(t *testing.T) {
	st := openTestStore(t)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				payload := map[string]any{"g": g, "pad": strings.Repeat("x", g*97)}
				if err := st.SaveJSON("race.json", payload); err != nil {
					t.Errorf("SaveJSON: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	var out map[string]any
	if _, err := st.LoadJSON("race.json", &out); err != nil {
		t.Fatalf("final file corrupt: %v", err)
	}
	entries, err := os.ReadDir(st.Dir())
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("leftover temp file: %s", entry.Name())
		}
	}
}
