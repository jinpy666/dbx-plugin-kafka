package kafkaconn

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

const connectRedacted = "[REDACTED]"
const connectBodyLimit = 4 << 20

var connectErrorClass = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$.]*(Exception|Error)$`)

// ConnectCluster contains public settings only. Credentials never enter Profile.
type ConnectCluster struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	Auth string `json:"auth,omitempty"`
}

type ConnectCredentials struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	BearerToken   string `json:"bearerToken,omitempty"`
	TLSCACert     string `json:"tlsCaCert,omitempty"`
	TLSClientCert string `json:"tlsClientCert,omitempty"`
	TLSClientKey  string `json:"tlsClientKey,omitempty"`
}

type ConnectRequest struct {
	ConnectionID     string            `json:"connectionId"`
	Cluster          string            `json:"cluster"`
	Connector        string            `json:"connector"`
	Plugin           string            `json:"plugin"`
	Config           map[string]string `json:"config"`
	Action           string            `json:"action"`
	TaskID           *int              `json:"taskId"`
	ConfirmConnector string            `json:"confirmConnector"`
}

type ConnectTask struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	Trace    string `json:"trace,omitempty"`
}

// connectTaskStatus decodes the worker's snake_case wire fields.
type connectTaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

func (task connectTaskStatus) public() ConnectTask {
	return ConnectTask{ID: task.ID, State: task.State, WorkerID: task.WorkerID, Trace: task.Trace}
}

type ConnectConnector struct {
	Name          string        `json:"name"`
	Type          string        `json:"type,omitempty"`
	State         string        `json:"state,omitempty"`
	WorkerID      string        `json:"workerId,omitempty"`
	Trace         string        `json:"trace,omitempty"`
	Tasks         []ConnectTask `json:"tasks"`
	Topics        []string      `json:"topics"`
	ConsumerGroup string        `json:"consumerGroup,omitempty"`
	Error         string        `json:"error,omitempty"`
}

type connectClient struct {
	cluster     ConnectCluster
	credentials ConnectCredentials
	http        *http.Client
}

type connectHTTPError struct{ status int }

func (e *connectHTTPError) Error() string {
	switch e.status {
	case 401:
		return "Kafka Connect authentication failed (HTTP 401)"
	case 403:
		return "Kafka Connect access denied (HTTP 403)"
	case 404:
		return "Kafka Connect resource or endpoint not found (HTTP 404)"
	case 409:
		return "Kafka Connect operation conflicts with the current connector state (HTTP 409)"
	default:
		return fmt.Sprintf("Kafka Connect request failed (HTTP %d)", e.status)
	}
}

var connectNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var connectSensitiveKey = regexp.MustCompile(`(?i)password|passwd|secret|token|credential|jaas|private[._-]?key|connection[._-]?url`)

func parseConnectSettings(raw, rawSecrets string) ([]ConnectCluster, map[string]ConnectCredentials, error) {
	clusters := []ConnectCluster{}
	credentials := map[string]ConnectCredentials{}
	invalid := func(message string) ([]ConnectCluster, map[string]ConnectCredentials, error) {
		return nil, nil, &InvalidParamsError{Msg: message}
	}
	if len(raw) > 256<<10 || len(rawSecrets) > 1<<20 {
		return invalid("Kafka Connect configuration is too large")
	}
	if strings.TrimSpace(raw) != "" {
		d := json.NewDecoder(strings.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&clusters); err != nil {
			return invalid("connect_clusters must be a JSON array of {name,url,auth}")
		}
		if d.Decode(new(any)) != io.EOF {
			return invalid("connect_clusters must contain one JSON value")
		}
	}
	if len(clusters) > 16 {
		return invalid("At most 16 Kafka Connect clusters are allowed per connection")
	}
	seen := map[string]bool{}
	for i := range clusters {
		c := &clusters[i]
		c.Name = strings.TrimSpace(c.Name)
		c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
		if !connectNamePattern.MatchString(c.Name) || seen[c.Name] {
			return invalid("Kafka Connect cluster names must be unique and use letters, digits, dots, underscores or hyphens")
		}
		seen[c.Name] = true
		u, err := url.Parse(c.URL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(c.URL) > 2048 || strings.Contains(u.Path, "..") {
			return invalid("Kafka Connect URLs must be HTTP(S) URLs without credentials, query, fragment or traversal")
		}
		if c.Auth == "" {
			c.Auth = "none"
		}
		if c.Auth != "none" && c.Auth != "basic" && c.Auth != "bearer" {
			return invalid("Kafka Connect auth must be none, basic or bearer")
		}
	}
	if strings.TrimSpace(rawSecrets) != "" {
		d := json.NewDecoder(strings.NewReader(rawSecrets))
		d.DisallowUnknownFields()
		if err := d.Decode(&credentials); err != nil || d.Decode(new(any)) != io.EOF {
			return invalid("connect_credentials must be a JSON object keyed by cluster name")
		}
	}
	for name := range credentials {
		if !seen[name] {
			return invalid("connect_credentials contains an unknown cluster name")
		}
	}
	for _, c := range clusters {
		secret := credentials[c.Name]
		if c.Auth == "basic" && (secret.Username == "" || secret.Password == "") {
			return invalid("Kafka Connect basic authentication requires username and password in connect_credentials")
		}
		if c.Auth == "bearer" && (secret.BearerToken == "" || strings.ContainsAny(secret.BearerToken, "\r\n")) {
			return invalid("Kafka Connect bearer authentication requires a valid token in connect_credentials")
		}
		if (secret.TLSClientCert == "") != (secret.TLSClientKey == "") {
			return invalid("Kafka Connect mTLS requires both tlsClientCert and tlsClientKey")
		}
		client, err := newConnectClient(c, secret)
		if err != nil {
			return invalid(err.Error())
		}
		client.http.CloseIdleConnections()
	}
	return clusters, credentials, nil
}

func newConnectClient(cluster ConnectCluster, credentials ConnectCredentials) (*connectClient, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if credentials.TLSCACert != "" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(credentials.TLSCACert)) {
			return nil, errors.New("Kafka Connect tlsCaCert must contain valid PEM certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if credentials.TLSClientCert != "" {
		cert, err := tls.X509KeyPair([]byte(credentials.TLSClientCert), []byte(credentials.TLSClientKey))
		if err != nil {
			return nil, errors.New("Kafka Connect mTLS certificate/key are invalid")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Avoid silently forwarding hydrated credentials through environment proxies.
	transport.Proxy = nil
	transport.TLSClientConfig = tlsConfig
	transport.MaxConnsPerHost = 8
	return &connectClient{cluster, credentials, &http.Client{
		Transport: transport, Timeout: adminTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// request never exposes upstream error bodies or transport errors: both can echo credentials/config.
func (c *connectClient) request(ctx context.Context, method, path string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil || len(data) > connectBodyLimit {
			return &InvalidParamsError{Msg: "Kafka Connect request is invalid or too large"}
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cluster.URL+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("Kafka Connect request could not be constructed")
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	switch c.cluster.Auth {
	case "basic":
		req.SetBasicAuth(c.credentials.Username, c.credentials.Password)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+c.credentials.BearerToken)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return errors.New("Kafka Connect request cancelled or timed out")
		}
		return errors.New("Kafka Connect connection failed; check network, TLS and endpoint settings")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &connectHTTPError{resp.StatusCode}
	}
	if out == nil {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, connectBodyLimit+1))
	if err != nil || len(raw) > connectBodyLimit {
		return errors.New("Kafka Connect response is unreadable or exceeds 4 MiB")
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return errors.New("Kafka Connect returned invalid JSON")
	}
	return nil
}

func connectSegment(value string) (string, error) {
	if value == "" || value == "." || value == ".." || len(value) > 512 || strings.ContainsAny(value, "/\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", &InvalidParamsError{Msg: "A valid Kafka Connect resource name is required"}
	}
	return url.PathEscape(value), nil
}

type connectDefinition struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Required      bool   `json:"required"`
	DefaultValue  any    `json:"default_value,omitempty"`
	Documentation string `json:"documentation,omitempty"`
}

func (c *connectClient) definitions(ctx context.Context, plugin string) ([]connectDefinition, error) {
	segment, err := connectSegment(plugin)
	if err != nil {
		return nil, err
	}
	definitions := []connectDefinition{}
	err = c.request(ctx, "GET", "/connector-plugins/"+segment+"/config", nil, &definitions)
	for i := range definitions {
		if strings.EqualFold(definitions[i].Type, "PASSWORD") || connectSensitiveKey.MatchString(definitions[i].Name) {
			definitions[i].DefaultValue = nil
		}
	}
	return definitions, err
}

func (c *connectClient) rawConfig(ctx context.Context, name string) (map[string]string, error) {
	segment, err := connectSegment(name)
	if err != nil {
		return nil, err
	}
	config := map[string]string{}
	err = c.request(ctx, "GET", "/connectors/"+segment+"/config", nil, &config)
	return config, err
}

// Unknown plugin definitions fail closed: all values are masked until metadata is available.
func (c *connectClient) sensitiveKeys(ctx context.Context, config map[string]string) map[string]bool {
	definitions, err := c.definitions(ctx, config["connector.class"])
	sensitive := map[string]bool{}
	for key := range config {
		if err != nil || connectSensitiveKey.MatchString(key) {
			sensitive[key] = true
		}
	}
	for _, d := range definitions {
		if strings.EqualFold(d.Type, "PASSWORD") {
			sensitive[d.Name] = true
		}
	}
	return sensitive
}

func (c *connectClient) scrub(text string, config map[string]string, sensitive map[string]bool) string {
	values := []string{c.credentials.Password, c.credentials.BearerToken, c.credentials.TLSClientKey}
	for key, value := range config {
		if sensitive[key] {
			values = append(values, value)
		}
	}
	// Replace long values first so overlapping credentials cannot leave a suffix behind.
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	for _, value := range values {
		if value != "" {
			text = strings.ReplaceAll(text, value, connectRedacted)
		}
	}
	if len(text) > 32768 {
		text = text[:32768] + "…"
	}
	return text
}

func (c *connectClient) connector(ctx context.Context, name string) ConnectConnector {
	row := ConnectConnector{Name: name, Tasks: []ConnectTask{}, Topics: []string{}}
	segment, err := connectSegment(name)
	if err != nil {
		row.Error = "Invalid connector resource name"
		return row
	}
	var status struct {
		Type      string              `json:"type"`
		Connector connectTaskStatus   `json:"connector"`
		Tasks     []connectTaskStatus `json:"tasks"`
	}
	if err := c.request(ctx, "GET", "/connectors/"+segment+"/status", nil, &status); err != nil {
		row.Error = err.Error()
		return row
	}
	row.Type, row.State, row.WorkerID = status.Type, status.Connector.State, status.Connector.WorkerID
	config, err := c.rawConfig(ctx, name)
	if err != nil {
		row.Error = err.Error()
		// Do not return traces unless their connector credentials can be scrubbed.
		for _, task := range status.Tasks {
			task.Trace = ""
			row.Tasks = append(row.Tasks, task.public())
		}
		return row
	}
	sensitive := c.sensitiveKeys(ctx, config)
	// A failure trace can include secrets derived by ConfigProviders rather than literal config.
	// Keep the error class/first line only; mask it if it resembles credential-bearing text.
	safeTrace := func(trace string) string {
		if strings.TrimSpace(trace) == "" {
			return ""
		}
		line := strings.SplitN(trace, "\n", 2)[0]
		if colon := strings.Index(line, ":"); colon >= 0 {
			line = line[:colon]
		}
		if !connectErrorClass.MatchString(line) || connectSensitiveKey.MatchString(line) {
			return "Connector task failed; inspect restricted worker logs for details"
		}
		return c.scrub(line, config, sensitive)
	}
	row.Trace = safeTrace(status.Connector.Trace)
	for _, task := range status.Tasks {
		task.Trace = safeTrace(task.Trace)
		row.Tasks = append(row.Tasks, task.public())
	}
	if row.Type == "sink" {
		row.ConsumerGroup = config["consumer.override.group.id"]
		if row.ConsumerGroup == "" {
			row.ConsumerGroup = "connect-" + name
		}
	}
	var topics map[string]struct {
		Topics []string `json:"topics"`
	}
	if err := c.request(ctx, "GET", "/connectors/"+segment+"/topics", nil, &topics); err == nil {
		row.Topics = topics[name].Topics
		if row.Topics == nil {
			row.Topics = []string{}
		}
	}
	// Configured topics remain relevant before a failed/new task processes data.
	seenTopics := map[string]bool{}
	linkedTopics := append([]string{}, row.Topics...)
	linkedTopics = append(linkedTopics, strings.Split(config["topics"], ",")...)
	linkedTopics = append(linkedTopics, config["topic"])
	row.Topics = []string{}
	for _, topic := range linkedTopics {
		topic = strings.TrimSpace(topic)
		if topic != "" && !seenTopics[topic] {
			seenTopics[topic] = true
			row.Topics = append(row.Topics, topic)
		}
	}
	sort.Strings(row.Topics)
	return row
}

// mergeConfig restores masked values only from this connector, never from the UI.
func (c *connectClient) mergeConfig(ctx context.Context, name string, incoming map[string]string) (map[string]string, error) {
	current, err := c.rawConfig(ctx, name)
	if err != nil {
		return nil, err
	}
	sensitive := c.sensitiveKeys(ctx, current)
	config := map[string]string{}
	for key, value := range incoming {
		config[key] = value
	}
	for key, value := range current {
		candidate, present := config[key]
		if sensitive[key] && (!present || candidate == connectRedacted) {
			config[key] = value
		}
	}
	for _, value := range config {
		if value == connectRedacted {
			return nil, &InvalidParamsError{Msg: "Unknown redacted connector config placeholder"}
		}
	}
	return config, nil
}

// ConnectCall operates only on host-hydrated, already registered connection settings.
func (s *Service) ConnectCall(ctx context.Context, method string, req ConnectRequest) (result any, resultErr error) {
	entry := s.lookup(req.ConnectionID)
	if entry == nil {
		return nil, errors.New("Kafka connection is not active")
	}
	profile := entry.profile
	if method == "kafka/connect/clusters/list" {
		return map[string]any{"clusters": profile.ConnectClusters}, nil
	}
	var cluster ConnectCluster
	found := false
	for _, candidate := range profile.ConnectClusters {
		if candidate.Name == req.Cluster {
			cluster = candidate
			found = true
			break
		}
	}
	if !found {
		return nil, &InvalidParamsError{Msg: "Unknown Kafka Connect cluster for this connection"}
	}
	write := method == "kafka/connect/config/set" || method == "kafka/connect/connectors/create" || method == "kafka/connect/connectors/delete" || method == "kafka/connect/connectors/action" || method == "kafka/connect/tasks/restart" || method == "kafka/connect/offsets/reset"
	if write {
		denied := false
		defer func() {
			state := "success"
			if resultErr != nil {
				state = "error"
			}
			if denied {
				state = "blocked"
			}
			if m, ok := result.(map[string]any); ok && m["supported"] == false {
				state = "error"
			}
			// Deliberately omit config, error messages and credentials from audit.
			action := strings.TrimPrefix(method, "kafka/")
			if method == "kafka/connect/connectors/action" {
				action += "/" + req.Action
			}
			target := cluster.Name + "/" + req.Connector
			if method == "kafka/connect/tasks/restart" && req.TaskID != nil {
				target += "/tasks/" + strconv.Itoa(*req.TaskID)
			}
			s.emitAudit(req.ConnectionID, action, target, state, "")
		}()
		if err := ensureWriteAllowed(profile, "connect"); err != nil {
			denied = true
			return nil, err
		}
		if method == "kafka/connect/connectors/delete" || method == "kafka/connect/offsets/reset" {
			if err := ensureDeleteAllowed(profile, "connect"); err != nil {
				denied = true
				return nil, err
			}
			if req.Connector == "" || req.ConfirmConnector != req.Connector {
				return nil, &InvalidParamsError{Msg: "confirmConnector must exactly match the connector name"}
			}
		}
	}
	client, err := newConnectClient(cluster, entry.secrets.ConnectCredentials[cluster.Name])
	if err != nil {
		return nil, err
	}
	defer client.http.CloseIdleConnections()
	if entry.target.Proxy != nil {
		dialer, dialErr := entry.runtimeProxyDialer(nil)
		if dialErr != nil {
			return nil, errors.New("Kafka Connect proxy route is invalid")
		}
		client.http.Transport.(*http.Transport).DialContext = dialer
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
	defer cancel()
	switch method {
	case "kafka/connect/test":
		var version struct {
			Version        string `json:"version"`
			Commit         string `json:"commit"`
			KafkaClusterID string `json:"kafka_cluster_id"`
		}
		if err := client.request(ctx, "GET", "/", nil, &version); err != nil {
			return nil, err
		}
		return map[string]any{"version": version.Version, "commit": version.Commit, "kafkaClusterId": version.KafkaClusterID}, nil
	case "kafka/connect/connectors/list":
		names := []string{}
		if err := client.request(ctx, "GET", "/connectors", nil, &names); err != nil {
			return nil, err
		}
		if len(names) > 2000 {
			return nil, errors.New("Kafka Connect has more than 2000 connectors; narrow the configured cluster")
		}
		rows := make([]ConnectConnector, len(names))
		var wg sync.WaitGroup
		jobs := make(chan int)
		for i := 0; i < min(8, len(names)); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for index := range jobs {
					rows[index] = client.connector(ctx, names[index])
				}
			}()
		}
		for i := range names {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
		return map[string]any{"connectors": rows}, nil
	case "kafka/connect/connectors/get":
		if _, err := connectSegment(req.Connector); err != nil {
			return nil, err
		}
		return map[string]any{"connector": client.connector(ctx, req.Connector)}, nil
	case "kafka/connect/config/get":
		config, err := client.rawConfig(ctx, req.Connector)
		if err != nil {
			return nil, err
		}
		sensitive := client.sensitiveKeys(ctx, config)
		keys := []string{}
		for key := range config {
			if sensitive[key] {
				config[key] = connectRedacted
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		return map[string]any{"config": config, "sensitiveKeys": keys}, nil
	case "kafka/connect/plugins/list":
		var plugins []struct {
			Class   string `json:"class"`
			Type    string `json:"type"`
			Version string `json:"version"`
		}
		plugins = make([]struct {
			Class   string `json:"class"`
			Type    string `json:"type"`
			Version string `json:"version"`
		}, 0)
		err := client.request(ctx, "GET", "/connector-plugins", nil, &plugins)
		return map[string]any{"plugins": plugins}, err
	case "kafka/connect/plugins/config":
		defs, err := client.definitions(ctx, req.Plugin)
		return map[string]any{"definitions": defs}, err
	case "kafka/connect/config/validate":
		config := map[string]string{}
		for key, value := range req.Config {
			config[key] = value
		}
		if req.Connector != "" {
			config, err = client.mergeConfig(ctx, req.Connector, config)
			if err != nil {
				return nil, err
			}
		}
		pluginName := req.Plugin
		if pluginName == "" || pluginName == connectRedacted {
			pluginName = config["connector.class"]
		}
		if existing := config["connector.class"]; existing != "" && existing != pluginName {
			return nil, &InvalidParamsError{Msg: "Validation plugin must match connector.class"}
		}
		config["connector.class"] = pluginName
		plugin, err := connectSegment(pluginName)
		if err != nil {
			return nil, err
		}
		var response struct {
			ErrorCount int `json:"error_count"`
			Configs    []struct {
				Definition connectDefinition `json:"definition"`
				Value      struct {
					Name   string   `json:"name"`
					Errors []string `json:"errors"`
				} `json:"value"`
			} `json:"configs"`
		}
		if err := client.request(ctx, "PUT", "/connector-plugins/"+plugin+"/config/validate", config, &response); err != nil {
			return nil, err
		}
		fields := []map[string]any{}
		for _, field := range response.Configs {
			fields = append(fields, map[string]any{"name": field.Definition.Name, "type": field.Definition.Type, "required": field.Definition.Required, "errorCount": len(field.Value.Errors)})
		}
		return map[string]any{"errorCount": response.ErrorCount, "fields": fields}, nil
	}
	segment, err := connectSegment(req.Connector)
	if err != nil {
		return nil, err
	}
	path := "/connectors/" + segment
	verb := ""
	var body any
	switch method {
	case "kafka/connect/connectors/create":
		if len(req.Config) == 0 {
			return nil, &InvalidParamsError{Msg: "Connector config is required"}
		}
		for _, value := range req.Config {
			if value == connectRedacted {
				return nil, &InvalidParamsError{Msg: "New connector config cannot contain redacted placeholders"}
			}
		}
		verb, path, body = "POST", "/connectors", map[string]any{"name": req.Connector, "config": req.Config}
	case "kafka/connect/config/set":
		if req.Config == nil {
			return nil, &InvalidParamsError{Msg: "Connector config is required"}
		}
		config, err := client.mergeConfig(ctx, req.Connector, req.Config)
		if err != nil {
			return nil, err
		}
		verb, path, body = "PUT", path+"/config", config
	case "kafka/connect/connectors/delete":
		verb = "DELETE"
	case "kafka/connect/offsets/reset":
		verb, path = "DELETE", path+"/offsets"
	case "kafka/connect/tasks/restart":
		if req.TaskID == nil || *req.TaskID < 0 {
			return nil, &InvalidParamsError{Msg: "Non-negative taskId is required"}
		}
		verb, path = "POST", path+"/tasks/"+strconv.Itoa(*req.TaskID)+"/restart"
	case "kafka/connect/connectors/action":
		switch req.Action {
		case "pause", "resume", "stop":
			verb, path = "PUT", path+"/"+req.Action
		case "restart":
			verb, path = "POST", path+"/restart"
		case "restart-all":
			verb, path = "POST", path+"/restart?includeTasks=true&onlyFailed=false"
		case "restart-failed":
			verb, path = "POST", path+"/restart?includeTasks=true&onlyFailed=true"
		default:
			return nil, &InvalidParamsError{Msg: "Unsupported Kafka Connect action"}
		}
	default:
		return nil, &InvalidParamsError{Msg: "Unknown Kafka Connect method"}
	}
	err = client.request(ctx, verb, path, body, nil)
	var httpErr *connectHTTPError
	if errors.As(err, &httpErr) && (httpErr.status == 405 || httpErr.status == 501 || httpErr.status == 404 && (req.Action == "stop" || method == "kafka/connect/offsets/reset")) {
		return map[string]any{"success": false, "supported": false, "message": "This Kafka Connect endpoint is unavailable on this worker/version"}, nil
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"success": true, "supported": true}, nil
}
