package cli

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	cache, err := os.MkdirTemp("", "flate-cli-cache-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = os.Setenv("XDG_CACHE_HOME", cache)
	_ = os.Unsetenv("FLATE_CACHE_DIR")
	code := m.Run()
	_ = os.RemoveAll(cache)
	os.Exit(code)
}
