package kafkaconn

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxTLSMaterialBytes = 1 << 20

// readTLSMaterial accepts inline PEM or an absolute path on the sidecar host.
// File contents stay in the backend. Errors never include paths or contents.
func readTLSMaterial(value, field string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if !filepath.IsAbs(value) {
		if len(value) > maxTLSMaterialBytes {
			return nil, fmt.Errorf("%s exceeds the TLS material size limit", field)
		}
		return []byte(value), nil
	}
	info, err := os.Lstat(value)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must reference a readable regular server-side file", field)
	}
	file, err := os.Open(value)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s server-side file", field)
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxTLSMaterialBytes {
		return nil, fmt.Errorf("%s must reference a regular file within the TLS material size limit", field)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxTLSMaterialBytes+1))
	if err != nil || len(data) > maxTLSMaterialBytes {
		clear(data)
		return nil, fmt.Errorf("cannot read %s within the TLS material size limit", field)
	}
	return data, nil
}
