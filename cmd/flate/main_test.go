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
		wantGCPercent int
		wantLimit     int64
	}{
		{name: "empty overrides", wantGCPercent: 400, wantLimit: 858993459},
		{name: "GOGC numeric", gogc: "100", wantGCPercent: 100, wantLimit: 858993459},
		{name: "GOGC off", gogc: "off", wantGCPercent: 100, wantLimit: 858993459},
		{name: "GOMEMLIMIT", gomemlimit: "800MiB", wantGCPercent: 400, wantLimit: 2 << 30},
		{name: "both", gogc: "100", gomemlimit: "800MiB", wantGCPercent: 100, wantLimit: 2 << 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gcPercent := debug.SetGCPercent(100)
			memoryLimit := debug.SetMemoryLimit(2 << 30)
			t.Cleanup(func() {
				debug.SetGCPercent(gcPercent)
				debug.SetMemoryLimit(memoryLimit)
			})
			t.Setenv("GOGC", tt.gogc)
			t.Setenv("GOMEMLIMIT", tt.gomemlimit)
			if tt.gomemlimit == "" {
				if err := os.Unsetenv("GOMEMLIMIT"); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("AUTOMEMLIMIT", "")
			if err := os.Unsetenv("AUTOMEMLIMIT"); err != nil {
				t.Fatal(err)
			}
			tuneGC(func(options ...memlimit.Option) (int64, error) {
				limit, err := memlimit.Set(append(options, memlimit.WithProvider(memlimit.Limit(1<<30)))...)
				if err != nil {
					t.Fatal(err)
				}
				return limit, err
			})
			assert.Equal(t, debug.SetGCPercent(100), tt.wantGCPercent)
			assert.Equal(t, debug.SetMemoryLimit(-1), tt.wantLimit)
		})
	}
}
