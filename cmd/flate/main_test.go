package main

import (
	"os"
	"runtime/debug"
	"testing"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/home-operations/flate/internal/assert"
)

func TestTuneGC_Overrides(t *testing.T) {
	for _, tt := range []struct {
		name          string
		gogc          string
		gomemlimit    string
		wantCalls     int
		wantGCPercent int
	}{
		{name: "empty overrides", wantCalls: 1, wantGCPercent: 400},
		{name: "GOGC numeric", gogc: "100", wantGCPercent: 100},
		{name: "GOGC off", gogc: "off", wantGCPercent: 100},
		{name: "GOMEMLIMIT", gomemlimit: "800MiB", wantGCPercent: 100},
		{name: "both", gogc: "100", gomemlimit: "800MiB", wantGCPercent: 100},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gcPercent := debug.SetGCPercent(100)
			t.Cleanup(func() { debug.SetGCPercent(gcPercent) })
			t.Setenv("GOGC", tt.gogc)
			t.Setenv("GOMEMLIMIT", tt.gomemlimit)
			calls := 0
			tuneGC(func(...memlimit.Option) (int64, error) {
				calls++
				return 0, memlimit.ErrCgroupsNotSupported
			})
			assert.Equal(t, calls, tt.wantCalls)
			assert.Equal(t, debug.SetGCPercent(100), tt.wantGCPercent)
		})
	}
}

func TestTuneGC_SoftLimit(t *testing.T) {
	gcPercent := debug.SetGCPercent(100)
	memoryLimit := debug.SetMemoryLimit(2 << 30)
	t.Cleanup(func() {
		debug.SetGCPercent(gcPercent)
		debug.SetMemoryLimit(memoryLimit)
	})
	for _, key := range []string{"GOGC", "GOMEMLIMIT", "AUTOMEMLIMIT"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	tuneGC(func(options ...memlimit.Option) (int64, error) {
		limit, err := memlimit.Set(append(options, memlimit.WithProvider(memlimit.Limit(1<<30)))...)
		if err != nil {
			t.Fatal(err)
		}
		return limit, err
	})
	assert.Equal(t, debug.SetMemoryLimit(-1), int64(858993459))
}
