package kafkaconn

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrokerTLSFiles(t *testing.T) {
	stack := newProxyTLSStack(t, "broker.test")
	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: stack.leaf.Certificate[0]})
	key, err := x509.MarshalPKCS8PrivateKey(stack.leaf.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	materials := map[string][]byte{"ca.crt": []byte(stack.caPEM), "user.crt": cert, "user.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key})}
	for name, value := range materials {
		if err := os.WriteFile(filepath.Join(dir, name), value, 0600); err != nil {
			t.Fatal(err)
		}
	}
	config, err := buildTLSConfig(Profile{TLSCACert: filepath.Join(dir, "ca.crt"), TLSClientCert: filepath.Join(dir, "user.crt")}, connSecrets{TLSClientKey: filepath.Join(dir, "user.key")})
	if err != nil {
		t.Fatal(err)
	}
	if config.InsecureSkipVerify || config.RootCAs == nil || len(config.Certificates) != 1 {
		t.Fatal("file-backed TLS lost CA, client identity or hostname verification")
	}
	if _, err := buildTLSConfig(Profile{TLSCACert: filepath.Join(dir, "missing.crt")}, connSecrets{}); err == nil {
		t.Fatal("missing CA file silently used system roots")
	}
	if err := os.WriteFile(filepath.Join(dir, "user.key"), []byte("invalid-rotated-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildTLSConfig(Profile{TLSClientCert: filepath.Join(dir, "user.crt")}, connSecrets{TLSClientKey: filepath.Join(dir, "user.key")}); err == nil {
		t.Fatal("reconnection did not reread rotated key")
	}
}

func TestTLSMaterialBoundsAndSafeErrors(t *testing.T) {
	dir := t.TempDir()
	large := filepath.Join(dir, "sensitive-name.key")
	if err := os.WriteFile(large, []byte(strings.Repeat("private-content", maxTLSMaterialBytes/14+1)), 0600); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{large, filepath.Join(dir, "missing-private.key"), dir, strings.Repeat("private-content", maxTLSMaterialBytes/14+1)} {
		_, err := readTLSMaterial(value, "tlsClientKey")
		if err == nil || strings.Contains(err.Error(), dir) || strings.Contains(err.Error(), "private-content") || strings.Contains(err.Error(), "sensitive-name") {
			t.Fatalf("expected a bounded, sanitized error; got %v", err)
		}
	}
	// Inline PEM retains existing behavior, including surrounding whitespace.
	if got, err := readTLSMaterial("  inline-pem\n", "tlsCaCert"); err != nil || string(got) != "inline-pem" {
		t.Fatal("inline material changed")
	}
}
