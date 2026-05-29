package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

// startProfile turns on the runtime profile selected by mode and
// returns a stop function the caller must defer. mode == "" returns
// a no-op stop function and nil error so callers can wire it
// unconditionally.
//
// CPU and trace profiles record events while running; the stop fn
// finalizes the file. mem profile is sampled at stop time; block
// and mutex profiles are also dumped at stop time after enabling
// runtime sampling.
func startProfile(mode, outDir string) (stop func(), err error) {
	if mode == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return nil, fmt.Errorf("profile-out: %w", err)
	}
	path := filepath.Join(outDir, mode+".pprof")
	switch mode {
	case "cpu":
		f, err := os.Create(path) //nolint:gosec // path is composed from the user-supplied --profile-out plus a fixed mode suffix
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		return func() { pprof.StopCPUProfile(); _ = f.Close() }, nil
	case "mem":
		return func() {
			f, err := os.Create(path) //nolint:gosec // path is composed from the user-supplied --profile-out plus a fixed mode suffix
			if err != nil {
				return
			}
			runtime.GC()
			_ = pprof.WriteHeapProfile(f)
			_ = f.Close()
		}, nil
	case "block":
		runtime.SetBlockProfileRate(1)
		return func() {
			f, err := os.Create(path) //nolint:gosec // path is composed from the user-supplied --profile-out plus a fixed mode suffix
			if err != nil {
				return
			}
			_ = pprof.Lookup("block").WriteTo(f, 0)
			runtime.SetBlockProfileRate(0)
			_ = f.Close()
		}, nil
	case "mutex":
		runtime.SetMutexProfileFraction(1)
		return func() {
			f, err := os.Create(path) //nolint:gosec // path is composed from the user-supplied --profile-out plus a fixed mode suffix
			if err != nil {
				return
			}
			_ = pprof.Lookup("mutex").WriteTo(f, 0)
			runtime.SetMutexProfileFraction(0)
			_ = f.Close()
		}, nil
	case "trace":
		f, err := os.Create(filepath.Join(outDir, "trace.out")) //nolint:gosec // path is composed from the user-supplied --profile-out plus a fixed filename
		if err != nil {
			return nil, err
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		return func() { trace.Stop(); _ = f.Close() }, nil
	}
	return nil, errors.New("--profile must be one of: cpu, mem, block, mutex, trace")
}
