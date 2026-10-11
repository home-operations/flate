package oci

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCredentials_ValidJSONLoads(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadCredentials(config)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if store == nil {
		t.Error("expected non-nil store for valid config")
	}
}

func TestLoadCredentials_EmptyPathFallsBackToDocker(t *testing.T) {
	_, err := loadCredentials("")
	if err != nil {
		t.Errorf("empty path should never error; got %v", err)
	}
}
