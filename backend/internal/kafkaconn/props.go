package kafkaconn

// props.go：「粘贴 Kafka properties 导入」（Lane 3 conn-properties）。
//
// 宿主连接对话框新增可选 manifest 字段 properties_import（textarea，
// binding: secret）：用户把 Kafka 客户端 properties 片段直接粘进对话框保存，
// 本文件在 connection/connect|test 组装 Profile 时解析并合并进结构化字段。
//
// 红线：粘贴文本整体走 secret binding（宿主加密存储、仅经
// connection_secrets 下发明文到 sidecar），绝不以明文持久化、不进日志/审计；
// 解析摘要（PropertiesImportSummary）只含计数与键名，任何值都不回传。
//
// 解析语法为 java.util.Properties 语义的实用子集：
//   - 注释行：首个非空白字符为 '#' 或 '!'；
//   - 分隔符：第一个未转义的 '=' / ':' / 空白（Java 语义，'key value' 合法）；
//   - 续行：行尾奇数个反斜杠 = 续行（偶数个是转义的字面反斜杠），续行的
//     前导空白被跳过；
//   - 转义：\t \n \r \f \\ \uXXXX（非法 \u 序列保守保留原文），其余 \X → X；
//   - 重复键后者覆盖（Java 语义）；值为空（key=）的键跳过、不覆盖表单值。
//
// 合并语义（paste wins）：粘贴非空值覆盖表单值；取值面外的值（如未知
// security.protocol）整键进忽略清单、表单值保持不变。映射表以现有 manifest
// 字段为准：Java keystore 路径类键（ssl.truststore.location 等）没有对应
// 字段（TLS 走 PEM 内联模型），进忽略清单并在 manifest 字段描述中引导
// PEM 内联键（ssl.truststore.certificates / ssl.keystore.certificate.chain /
// ssl.keystore.key）。

import (
	"regexp"
	"sort"
	"strings"
)

// PropertiesImportSummary 是 properties 导入的解析摘要：只含计数与键名，
// 值一律不进摘要（凭据红线）。经 ConnectionStatus.propertiesImport 透出。
type PropertiesImportSummary struct {
	Mapped      int      `json:"mapped"`
	MappedKeys  []string `json:"mappedKeys,omitempty"`
	Ignored     int      `json:"ignored"`
	IgnoredKeys []string `json:"ignoredKeys,omitempty"`
}

// parseJavaProperties 按 Java Properties 语义把文本解析为键值表
// （重复键后者覆盖；空值保留空串，由调用方决定跳过语义）。
func parseJavaProperties(text string) map[string]string {
	result := map[string]string{}
	for _, line := range splitLogicalLines(text) {
		trimmed := strings.TrimLeft(line, " \t\f")
		if trimmed == "" || trimmed[0] == '#' || trimmed[0] == '!' {
			continue
		}
		key, value, ok := splitPropertyLine(trimmed)
		if !ok || key == "" {
			continue
		}
		result[key] = value
	}
	return result
}

// splitLogicalLines 按行尾奇数反斜杠合并物理行为逻辑行（合并时去掉该
// 反斜杠并跳过续行前导空白，Java 语义）。
func splitLogicalLines(text string) []string {
	logical := make([]string, 0, 16)
	pending := ""
	for _, raw := range strings.Split(text, "\n") {
		raw = strings.TrimRight(raw, "\r")
		if pending != "" {
			// 续行：跳过本物理行的前导空白后再拼接。
			raw = strings.TrimLeft(raw, " \t\f")
		}
		line := pending + raw
		trailing := 0
		for trailing < len(line) && line[len(line)-1-trailing] == '\\' {
			trailing++
		}
		if trailing%2 == 1 {
			pending = line[:len(line)-1]
			continue
		}
		pending = ""
		logical = append(logical, line)
	}
	if pending != "" {
		logical = append(logical, pending)
	}
	return logical
}

// splitPropertyLine 拆出键值：键止于第一个未转义的 '=' / ':' / 空白；
// 键后跳过空白，若遇 '=' / ':' 则消费并跳过其后一段空白，余下为值。
// 键内转义在扫描时解码；值在拆出后统一解码；值尾随空白按粘贴容错去除。
func splitPropertyLine(line string) (key, value string, ok bool) {
	var keyBuf strings.Builder
	i := 0
	for ; i < len(line); i++ {
		c := line[i]
		if c == '\\' && i+1 < len(line) {
			decoded, size := decodeEscape(line[i:])
			keyBuf.WriteString(decoded)
			i += size - 1
			continue
		}
		if c == '=' || c == ':' || c == ' ' || c == '\t' || c == '\f' {
			break
		}
		keyBuf.WriteByte(c)
	}
	key = keyBuf.String()
	// 键后空白跳过；下一个字符若为 '=' / ':' 则消费并跳过其后空白。
	for i < len(line) && (line[i] == ' ' || line[i] == '\t' || line[i] == '\f') {
		i++
	}
	if i < len(line) && (line[i] == '=' || line[i] == ':') {
		i++
		for i < len(line) && (line[i] == ' ' || line[i] == '\t' || line[i] == '\f') {
			i++
		}
	}
	if i > len(line) {
		i = len(line)
	}
	return key, decodeEscapedString(strings.TrimRight(line[i:], " \t\f")), true
}

// decodeEscapedString 对整段值做反斜杠转义解码（\t \n \r \f \uXXXX，
// 其余 \X → X；无反斜杠时零拷贝原样返回）。
func decodeEscapedString(value string) string {
	if !strings.ContainsRune(value, '\\') {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' && i+1 < len(value) {
			decoded, size := decodeEscape(value[i:])
			out.WriteString(decoded)
			i += size - 1
			continue
		}
		out.WriteByte(value[i])
	}
	return out.String()
}

// decodeEscape 处理 line 开头的反斜杠转义（line 以 '\\' 开头），返回
// （解码结果, 消耗字节数）。\uXXXX 非法时保守保留原文两字节（\u）。
func decodeEscape(line string) (string, int) {
	if len(line) < 2 {
		return "\\", 1
	}
	switch line[1] {
	case 't':
		return "\t", 2
	case 'n':
		return "\n", 2
	case 'r':
		return "\r", 2
	case 'f':
		return "\f", 2
	case 'u':
		if len(line) >= 6 && isHex4(line[2:6]) {
			return string(rune(hex4(line[2:6]))), 6
		}
		return line[:2], 2
	default:
		return string(line[1]), 2
	}
}

func isHex4(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func hex4(s string) int {
	value := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			value = value*16 + int(c-'0')
		case c >= 'a' && c <= 'f':
			value = value*16 + int(c-'a') + 10
		default:
			value = value*16 + int(c-'A') + 10
		}
	}
	return value
}

// applyPropertiesImport 解析粘贴文本并把可识别键合并进 profile/secrets
// （原地修改；在 NormalizeProfile/Validate 之前调用，粘贴驱动的 SASL_SSL +
// jaas 凭据组合由此通过 required_when 兜底校验）。返回解析摘要（仅键名）。
func applyPropertiesImport(profile *Profile, secrets *connSecrets, text string) *PropertiesImportSummary {
	props := parseJavaProperties(text)
	summary := &PropertiesImportSummary{}
	for _, key := range sortedKeys(props) {
		value := props[key]
		if strings.TrimSpace(value) == "" {
			continue // 空值：不覆盖表单值，不计数
		}
		if key == "sasl.jaas.config" {
			continue // jaas 依赖机制结果，最后单独处理
		}
		if mapProperty(props, profile, secrets, key, value) {
			summary.Mapped++
			summary.MappedKeys = append(summary.MappedKeys, key)
		} else {
			summary.Ignored++
			summary.IgnoredKeys = append(summary.IgnoredKeys, key)
		}
	}
	if jaas := strings.TrimSpace(props["sasl.jaas.config"]); jaas != "" {
		if mapJAASConfig(profile, secrets, jaas) {
			summary.Mapped++
			summary.MappedKeys = append(summary.MappedKeys, "sasl.jaas.config")
		} else {
			summary.Ignored++
			summary.IgnoredKeys = append(summary.IgnoredKeys, "sasl.jaas.config")
		}
	}
	// 键名列表排序保证摘要确定性（map 迭代序随机）。
	sort.Strings(summary.MappedKeys)
	sort.Strings(summary.IgnoredKeys)
	return summary
}

// mapProperty 是单键映射表：返回 true = 已映射。取值面外的值返回 false
// （整键忽略，表单值保持不变）。键比较大小写不敏感（容错粘贴）。
func mapProperty(props map[string]string, profile *Profile, secrets *connSecrets, key, value string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "bootstrap.servers":
		profile.BootstrapServers = splitPropsList(value)
		return true
	case "security.protocol":
		protocol := NormalizeSecurityProtocol(value)
		if protocol == SecurityProtocolPlaintext && !strings.EqualFold(strings.TrimSpace(value), SecurityProtocolPlaintext) {
			return false // 取值面外（Normalize 会静默回退 PLAINTEXT，这里显式忽略）
		}
		profile.SecurityProtocol = protocol
		return true
	case "sasl.mechanism":
		mechanism := NormalizeSASLMechanism(value)
		if mechanism == "" {
			return false
		}
		profile.SASLMechanism = mechanism
		return true
	case "sasl.kerberos.service.name":
		profile.KerberosServiceName = strings.TrimSpace(value)
		return true
	case "ssl.endpoint.identification.algorithm":
		// none/空 = 关闭证书主机名校验 → skip verify；其余算法保持校验。
		profile.TLSInsecureSkipVerify = strings.EqualFold(strings.TrimSpace(value), "none")
		return true
	case "ssl.truststore.certificates":
		profile.TLSCACert = strings.TrimSpace(value)
		return true
	case "ssl.keystore.certificate.chain":
		profile.TLSClientCert = strings.TrimSpace(value)
		return true
	case "ssl.keystore.key":
		secrets.TLSClientKey = strings.TrimSpace(value) // 凭据红线：只进 secrets
		return true
	case "schema.registry.url":
		profile.SRURL = strings.TrimSpace(value)
		profile.SchemaRegistry = SchemaRegistryConfluent
		return true
	case "basic.auth.credentials.source":
		// 仅 USER_INFO 支持 SR basic auth 直填；其余来源（URL/SASL_INHERIT/
		// OAUTHBEARER…）没有映射目标。自身映射成功与否 = 取值是否受支持。
		return strings.EqualFold(strings.TrimSpace(value), "USER_INFO")
	case "basic.auth.user.info", "schema.registry.basic.auth.user.info":
		// Confluent 语义：显式 source 且非 USER_INFO 时 user.info 不参与认证。
		if source := strings.TrimSpace(props["basic.auth.credentials.source"]); source != "" && !strings.EqualFold(source, "USER_INFO") {
			return false
		}
		username, password, ok := splitUserInfo(value)
		if !ok {
			return false
		}
		profile.SRUsername = username
		secrets.SRPassword = password // 凭据红线：只进 secrets
		return true
	case "client.id":
		profile.ClientID = strings.TrimSpace(value)
		return true
	default:
		return false
	}
}

// splitUserInfo 按【第一个】冒号拆 user:pass（密码可含冒号）。
func splitUserInfo(value string) (username, password string, ok bool) {
	index := strings.Index(value, ":")
	if index <= 0 {
		return "", "", false
	}
	username = strings.TrimSpace(value[:index])
	password = value[index+1:]
	return username, password, username != ""
}

// jaasFieldPattern 提取 JAAS 登录模块串中的 name="value"（兼容单引号）。
var jaasFieldPattern = func(name string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)(?:^|[\s;])` + regexp.QuoteMeta(name) + `\s*=\s*("([^"]*)"|'([^']*)')`)
}

// mapJAASConfig 从 sasl.jaas.config 提取凭据：PLAIN/SCRAM → username/
// password；GSSAPI → principal/keyTab；OAUTHBEARER 无映射目标（token 须经
// 表单 oauth_token_source 补齐）。至少提取到一项才算已映射。
func mapJAASConfig(profile *Profile, secrets *connSecrets, jaas string) bool {
	switch profile.SASLMechanism {
	case SASLMechanismGSSAPI:
		principal := jaasField(jaas, "principal")
		keytab := jaasField(jaas, "keyTab")
		if principal == "" && keytab == "" {
			return false
		}
		if principal != "" {
			profile.KerberosPrincipal = principal
		}
		if keytab != "" {
			profile.KerberosKeytabPath = keytab
		}
		return true
	case SASLMechanismOAUTHBEARER:
		return false
	default:
		// PLAIN / SCRAM / 未设机制：username/password 提取（机制未设时字段
		// 处于休眠分支，后端既有语义本就忽略非激活分支的值）。
		username := jaasField(jaas, "username")
		password := jaasField(jaas, "password")
		if username == "" && password == "" {
			return false
		}
		if username != "" {
			profile.Username = username
		}
		if password != "" {
			secrets.SASLPassword = password // 凭据红线：只进 secrets
		}
		return true
	}
}

// jaasField 从 JAAS 登录模块串提取 name="value"（或单引号包裹）。
func jaasField(jaas, name string) string {
	match := jaasFieldPattern(name).FindStringSubmatch(jaas)
	if match == nil {
		return ""
	}
	if match[3] != "" {
		return match[3] // 单引号值
	}
	return match[2] // 双引号值
}

// splitPropsList 拆 bootstrap 列表：换行 + 逗号分隔（manifest textarea 同款）。
func splitPropsList(value string) []string {
	out := []string{}
	for _, line := range strings.Split(value, "\n") {
		for _, part := range strings.Split(line, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	return out
}

func sortedKeys(props map[string]string) []string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
