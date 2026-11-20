package kafkaconn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

func connectService(t *testing.T, endpoint string, readOnly, allowDelete bool) *Service {
	t.Helper()
	s := NewService()
	t.Cleanup(s.CloseAll)
	clusters, _ := json.Marshal([]ConnectCluster{{Name: "local", URL: endpoint, Auth: "none"}})
	err := s.Connect(&lifecycle.Params{Connection: lifecycle.Connection{ID: "test", ExternalConfig: map[string]any{
		"bootstrap_servers": "localhost:9092", "connect_clusters": string(clusters), "read_only": readOnly, "allow_delete": allowDelete,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func invokeConnect(t *testing.T, s *Service, method string, req ConnectRequest) any {
	t.Helper()
	req.ConnectionID = "test"
	req.Cluster = "local"
	result, err := s.ConnectCall(context.Background(), "kafka/connect/"+method, req)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestConnectConfigurationValidationAndSecretBoundary(t *testing.T) {
	for _, test := range []struct{ config, secret string }{
		{`[{"name":"a","url":"http://localhost:8083","password":"leak"}]`, ""},
		{`[{"name":"a","url":"http://user:password@localhost:8083"}]`, ""},
		{`[{"name":"a","url":"http://localhost:8083?token=leak"}]`, ""},
		{`[{"name":"a","url":"file:///etc/passwd"}]`, ""},
		{`[{"name":"a","url":"http://localhost/%2e%2e/admin"}]`, ""},
		{`[{"name":"a","url":"http://localhost"},{"name":"a","url":"http://localhost"}]`, ""},
		{`[{"name":"a","url":"http://localhost","auth":"basic"}]`, ""},
		{`[{"name":"a","url":"http://localhost"}]`, `{"unknown":{"password":"hidden"}}`},
		{`[{"name":"a","url":"http://localhost"}]`, `{"a":{"tlsClientCert":"invalid"}}`},
		{`[{"name":"a","url":"http://localhost"}] {}`, ""},
		{`[{"name":"a","url":"http://localhost"}]`, `{"a":{"tlsCaCert":"invalid"}}`},
	} {
		if _, _, err := parseConnectSettings(test.config, test.secret); err == nil {
			t.Errorf("accepted invalid configuration: %s", test.config)
		}
	}
	p, secrets, err := NewProfileFromLifecycle(&lifecycle.Params{Connection: lifecycle.Connection{ID: "test", ExternalConfig: map[string]any{
		"bootstrap_servers": "localhost:9092", "connect_clusters": `[{"name":"a","url":"https://localhost/connect","auth":"basic"}]`,
	}, Secrets: map[string]any{"connect_credentials": `{"a":{"username":"smoke","password":"ephemeral-value"}}`}}})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(p)
	if strings.Contains(string(data), "ephemeral-value") || strings.Contains(string(data), "smoke") {
		t.Fatal("credentials entered Profile")
	}
	if secrets.ConnectCredentials["a"].Password != "ephemeral-value" {
		t.Fatal("missing host-bound secret")
	}
}

func TestConnectWriteAndDeleteGatesNeverReachWorker(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(204) }))
	defer server.Close()
	methods := []string{"config/set", "connectors/create", "connectors/delete", "connectors/action", "tasks/restart", "offsets/reset"}
	for _, readonly := range []bool{true, false} {
		s := connectService(t, server.URL, readonly, false)
		for _, method := range methods {
			if !readonly && method != "connectors/delete" && method != "offsets/reset" {
				continue
			}
			_, err := s.ConnectCall(context.Background(), "kafka/connect/"+method, ConnectRequest{ConnectionID: "test", Cluster: "local", Connector: "demo", ConfirmConnector: "demo", Action: "restart"})
			if err == nil {
				t.Errorf("%s bypassed gate", method)
			}
		}
	}
	s := connectService(t, server.URL, false, true)
	for _, method := range []string{"connectors/delete", "offsets/reset"} {
		_, err := s.ConnectCall(context.Background(), "kafka/connect/"+method, ConnectRequest{ConnectionID: "test", Cluster: "local", Connector: "demo", ConfirmConnector: "wrong"})
		if err == nil {
			t.Fatal("missing destructive confirmation")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("blocked operation reached worker")
	}
}

func TestConnectRoutingOperationsAndAudit(t *testing.T) {
	var mu sync.Mutex
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer server.Close()
	s := connectService(t, server.URL+"/base", false, true)
	audits := []AuditRecord{}
	s.Audit = func(r AuditRecord) { audits = append(audits, r) }
	for _, action := range []string{"pause", "resume", "stop", "restart", "restart-all", "restart-failed"} {
		invokeConnect(t, s, "connectors/action", ConnectRequest{Connector: "demo space", Action: action})
	}
	task := 3
	invokeConnect(t, s, "tasks/restart", ConnectRequest{Connector: "demo", TaskID: &task})
	invokeConnect(t, s, "connectors/delete", ConnectRequest{Connector: "demo", ConfirmConnector: "demo"})
	invokeConnect(t, s, "offsets/reset", ConnectRequest{Connector: "demo", ConfirmConnector: "demo"})
	expected := []string{"PUT /base/connectors/demo%20space/pause", "PUT /base/connectors/demo%20space/resume", "PUT /base/connectors/demo%20space/stop", "POST /base/connectors/demo%20space/restart", "POST /base/connectors/demo%20space/restart?includeTasks=true&onlyFailed=false", "POST /base/connectors/demo%20space/restart?includeTasks=true&onlyFailed=true", "POST /base/connectors/demo/tasks/3/restart", "DELETE /base/connectors/demo", "DELETE /base/connectors/demo/offsets"}
	if fmt.Sprint(paths) != fmt.Sprint(expected) {
		t.Fatalf("paths=%v", paths)
	}
	if len(audits) != len(expected) {
		t.Fatalf("missing audits: %d", len(audits))
	}
	for _, audit := range audits {
		if audit.Result != "success" || audit.Detail != "" {
			t.Errorf("unexpected audit: %+v", audit)
		}
	}
}

func TestConnectConfigRedactionAndSecretPreservingUpdate(t *testing.T) {
	var saved map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/config") && r.Method == "PUT":
			_ = json.NewDecoder(r.Body).Decode(&saved)
			w.WriteHeader(200)
		case strings.HasPrefix(r.URL.Path, "/connector-plugins/"):
			fmt.Fprint(w, `[{"name":"custom.field","type":"PASSWORD","default_value":"default-secret"},{"name":"topics","type":"STRING"}]`)
		default:
			fmt.Fprint(w, `{"connector.class":"FileSink","topics":"orders","password":"hidden-pass","custom.field":"hidden-custom"}`)
		}
	}))
	defer server.Close()
	s := connectService(t, server.URL, false, true)
	result := invokeConnect(t, s, "config/get", ConnectRequest{Connector: "demo"})
	data, _ := json.Marshal(result)
	for _, secret := range []string{"hidden-pass", "hidden-custom", "default-secret"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("config leaked secret")
		}
	}
	config := result.(map[string]any)["config"].(map[string]string)
	if config["topics"] != "orders" || config["custom.field"] != connectRedacted {
		t.Fatalf("config=%v", config)
	}
	config["topics"] = "new-topic"
	delete(config, "password")
	invokeConnect(t, s, "config/set", ConnectRequest{Connector: "demo", Config: config})
	if saved["password"] != "hidden-pass" || saved["custom.field"] != "hidden-custom" || saved["topics"] != "new-topic" {
		t.Fatal("masked secrets were lost on update")
	}
}

func TestConnectPartialFailureAndTraceRedaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connectors":
			fmt.Fprint(w, `["healthy","broken","forbidden"]`)
		case "/connectors/forbidden/status":
			w.WriteHeader(403)
			fmt.Fprint(w, `{"message":"secret upstream payload"}`)
		case "/connectors/healthy/status":
			fmt.Fprint(w, `{"type":"sink","connector":{"state":"RUNNING","worker_id":"worker"},"tasks":[{"id":0,"state":"RUNNING"}]}`)
		case "/connectors/broken/status":
			fmt.Fprint(w, `{"type":"sink","connector":{"state":"FAILED","trace":"java.lang.RuntimeException: hidden-pass\\n at sensitive stack"},"tasks":[{"id":0,"state":"FAILED","trace":"unstructured hidden-secret"}]}`)
		case "/connector-plugins/FileSink/config":
			fmt.Fprint(w, `[{"name":"password","type":"PASSWORD"}]`)
		default:
			if strings.HasSuffix(r.URL.Path, "/config") {
				fmt.Fprint(w, `{"connector.class":"FileSink","topics":"orders","password":"hidden-pass","consumer.override.group.id":"custom-group"}`)
			} else {
				w.WriteHeader(404)
			}
		}
	}))
	defer server.Close()
	s := connectService(t, server.URL, true, false)
	result := invokeConnect(t, s, "connectors/list", ConnectRequest{})
	data, _ := json.Marshal(result)
	for _, secret := range []string{"hidden-pass", "hidden-secret", "sensitive stack", "secret upstream payload"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
	if strings.Contains(string(data), "worker_id") || !strings.Contains(string(data), "workerId") {
		t.Fatal("Connect task worker IDs must use the public camelCase contract")
	}
	rows := result.(map[string]any)["connectors"].([]ConnectConnector)
	if rows[2].Trace != "" || rows[2].Tasks[0].Trace != "" {
		t.Fatal("healthy connector reported a fabricated failure")
	}
	if len(rows) != 3 || rows[0].Trace != "java.lang.RuntimeException" || rows[1].Error == "" || rows[2].ConsumerGroup != "custom-group" {
		t.Fatalf("partial rows=%+v", rows)
	}
}

func TestConnectAuthenticationRedirectTLSAndCancellation(t *testing.T) {
	token := "throwaway-token"
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(401)
			return
		}
		http.Redirect(w, r, target.URL, 302)
	}))
	defer server.Close()
	client, err := newConnectClient(ConnectCluster{URL: server.URL, Auth: "bearer"}, ConnectCredentials{BearerToken: token})
	if err != nil {
		t.Fatal(err)
	}
	defer client.http.CloseIdleConnections()
	err = client.request(context.Background(), "GET", "/", nil, nil)
	if err == nil || targetCalls.Load() != 0 {
		t.Fatal("followed authenticated redirect")
	}
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"version":"test"}`) }))
	defer tlsServer.Close()
	untrusted, _ := newConnectClient(ConnectCluster{URL: tlsServer.URL}, ConnectCredentials{})
	defer untrusted.http.CloseIdleConnections()
	if err := untrusted.request(context.Background(), "GET", "/", nil, nil); err == nil {
		t.Fatal("accepted untrusted TLS")
	}
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsServer.Certificate().Raw}))
	trusted, _ := newConnectClient(ConnectCluster{URL: tlsServer.URL}, ConnectCredentials{TLSCACert: certPEM})
	defer trusted.http.CloseIdleConnections()
	if err := trusted.request(context.Background(), "GET", "/", nil, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := trusted.request(ctx, "GET", "/", nil, nil); err == nil {
		t.Fatal("ignored cancellation")
	}
	basic := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "test" || p != "ephemeral" {
			w.WriteHeader(401)
		}
	}))
	defer basic.Close()
	basicClient, _ := newConnectClient(ConnectCluster{URL: basic.URL, Auth: "basic"}, ConnectCredentials{Username: "test", Password: "ephemeral"})
	defer basicClient.http.CloseIdleConnections()
	if err := basicClient.request(context.Background(), "GET", "/", nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConnectMTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ephemeral-client"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	clientCertificate, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(clientCertificate)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	serverPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	clientPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	unauthenticated, _ := newConnectClient(ConnectCluster{URL: server.URL}, ConnectCredentials{TLSCACert: serverPEM})
	defer unauthenticated.http.CloseIdleConnections()
	if err := unauthenticated.request(context.Background(), "GET", "/", nil, nil); err == nil {
		t.Fatal("mTLS accepted a client without a certificate")
	}
	client, err := newConnectClient(ConnectCluster{URL: server.URL}, ConnectCredentials{TLSCACert: serverPEM, TLSClientCert: clientPEM, TLSClientKey: keyPEM})
	if err != nil {
		t.Fatal(err)
	}
	defer client.http.CloseIdleConnections()
	if err := client.request(context.Background(), "GET", "/", nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConnectIsolationReconnectUnsupportedAndBounds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(405)
		fmt.Fprint(w, `{"message":"private-config"}`)
	}))
	defer server.Close()
	s := connectService(t, server.URL, false, true)
	result := invokeConnect(t, s, "connectors/action", ConnectRequest{Connector: "demo", Action: "stop"}).(map[string]any)
	if result["supported"] != false || result["success"] != false {
		t.Fatal("unsupported endpoint reported success")
	}
	before := calls.Load()
	for _, req := range []ConnectRequest{{ConnectionID: "missing", Cluster: "local"}, {ConnectionID: "test", Cluster: "other"}, {ConnectionID: "test", Cluster: "local", Connector: "../escape", Action: "stop"}} {
		if _, err := s.ConnectCall(context.Background(), "kafka/connect/connectors/action", req); err == nil {
			t.Fatal("invalid connection or resource accepted")
		}
	}
	if calls.Load() != before {
		t.Fatal("invalid resource reached worker")
	}
	s.Disconnect("test")
	if _, err := s.ConnectCall(context.Background(), "kafka/connect/test", ConnectRequest{ConnectionID: "test", Cluster: "local"}); err == nil {
		t.Fatal("disconnected connection remained usable")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer slow.Close()
	client, _ := newConnectClient(ConnectCluster{URL: slow.URL}, ConnectCredentials{})
	defer client.http.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.request(ctx, "GET", "/", nil, nil); err == nil {
		t.Fatal("timeout was ignored")
	}
	oversized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, strings.Repeat(" ", connectBodyLimit+1)) }))
	defer oversized.Close()
	bounded, _ := newConnectClient(ConnectCluster{URL: oversized.URL}, ConnectCredentials{})
	defer bounded.http.CloseIdleConnections()
	if err := bounded.request(context.Background(), "GET", "/", nil, new(any)); err == nil {
		t.Fatal("oversized response accepted")
	}
}

func TestConnectCredentialsAreIsolatedAcrossConnectionsAndReconnect(t *testing.T) {
	worker := func(label, password string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, supplied, ok := r.BasicAuth()
			if !ok || supplied != password {
				w.WriteHeader(401)
				return
			}
			fmt.Fprintf(w, `{"version":%q}`, label)
		}))
	}
	first := worker("first", "ephemeral-a")
	defer first.Close()
	second := worker("second", "ephemeral-b")
	defer second.Close()
	s := NewService()
	defer s.CloseAll()
	configure := func(id, endpoint, password string) {
		clusters, _ := json.Marshal([]ConnectCluster{{Name: "same-name", URL: endpoint, Auth: "basic"}})
		credentials, _ := json.Marshal(map[string]ConnectCredentials{"same-name": {Username: "fixture", Password: password}})
		if err := s.Connect(&lifecycle.Params{Connection: lifecycle.Connection{ID: id, ExternalConfig: map[string]any{"bootstrap_servers": "localhost:9092", "connect_clusters": string(clusters)}, Secrets: map[string]any{"connect_credentials": string(credentials)}}}); err != nil {
			t.Fatal(err)
		}
	}
	configure("a", first.URL, "ephemeral-a")
	configure("b", second.URL, "ephemeral-b")
	for id, expected := range map[string]string{"a": "first", "b": "second"} {
		result, err := s.ConnectCall(context.Background(), "kafka/connect/test", ConnectRequest{ConnectionID: id, Cluster: "same-name"})
		if err != nil || result.(map[string]any)["version"] != expected {
			t.Fatalf("wrong worker/credential for connection %s: %v", id, err)
		}
	}
	configure("a", second.URL, "ephemeral-b")
	result, err := s.ConnectCall(context.Background(), "kafka/connect/test", ConnectRequest{ConnectionID: "a", Cluster: "same-name"})
	if err != nil || result.(map[string]any)["version"] != "second" {
		t.Fatalf("reconnect retained old endpoint/credentials: %v", err)
	}
}

func TestConnectValidationHydratesSecretsWithoutEchoingWorkerErrors(t *testing.T) {
	var received map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/connectors/demo/config":
			fmt.Fprint(w, `{"connector.class":"FileSink","name":"demo","password":"ephemeral-password"}`)
		case "/connector-plugins/FileSink/config":
			fmt.Fprint(w, `[{"name":"password","type":"PASSWORD"}]`)
		case "/connector-plugins/FileSink/config/validate":
			_ = json.NewDecoder(r.Body).Decode(&received)
			fmt.Fprint(w, `{"error_count":1,"configs":[{"definition":{"name":"password","type":"PASSWORD","required":true,"default_value":"sensitive-default"},"value":{"name":"password","errors":["echoed ephemeral-password"]}}]}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	s := connectService(t, server.URL, true, false)
	result := invokeConnect(t, s, "config/validate", ConnectRequest{Connector: "demo", Plugin: "FileSink", Config: map[string]string{"connector.class": "FileSink", "password": connectRedacted}})
	if received["password"] != "ephemeral-password" {
		t.Fatal("validation sent a placeholder instead of the current secret")
	}
	data, _ := json.Marshal(result)
	if strings.Contains(string(data), "ephemeral-password") || strings.Contains(string(data), "sensitive-default") {
		t.Fatal("validation response leaked a worker value")
	}
	if result.(map[string]any)["errorCount"] != 1 {
		t.Fatal("validation error count was lost")
	}
}

func TestConnectUnknownMetadataMasksEveryConfigurationValue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/connector-plugins/") {
			w.WriteHeader(404)
			return
		}
		fmt.Fprint(w, `{"connector.class":"Unknown","unusual.property":"sensitive-value","topics":"orders"}`)
	}))
	defer server.Close()
	s := connectService(t, server.URL, true, false)
	result := invokeConnect(t, s, "config/get", ConnectRequest{Connector: "demo"}).(map[string]any)
	for _, value := range result["config"].(map[string]string) {
		if value != connectRedacted {
			t.Fatal("unknown metadata did not fail closed")
		}
	}
}
