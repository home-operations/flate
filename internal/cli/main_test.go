package cli

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Unsetenv("FLATE_CACHE_DIR")
	os.Exit(m.Run())
}
