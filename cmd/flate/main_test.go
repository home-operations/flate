package main

import (
	"runtime/debug"
	"testing"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/home-operations/flate/internal/assert"
)

func TestTuneGC_Overrides(t *testing.T) {
	gcPercent := debug.SetGCPercent(400)
	t.Cleanup(func() { debug.SetGCPercent(gcPercent) })

	for _, tt := range []struct {
		name       string
		gogc       string
		gomemlimit string
		wantCalls  int
	}{
		{name: "empty overrides", wantCalls: 1},
		{name: "GOGC numeric", gogc: "100"},
		{name: "GOGC off", gogc: "off"},
		{name: "GOMEMLIMIT", gomemlimit: "800MiB"},
		{name: "both", gogc: "100", gomemlimit: "800MiB"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GOGC", tt.gogc)
			t.Setenv("GOMEMLIMIT", tt.gomemlimit)
			calls := 0
			tuneGC(func(...memlimit.Option) (int64, error) {
				calls++
				return 0, memlimit.ErrCgroupsNotSupported
			})
			assert.Equal(t, calls, tt.wantCalls)
		})
	}
}
