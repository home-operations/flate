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
		f, err := createProfileFile(path)
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
			f, err := createProfileFile(path)
			if err != nil {
				return
			}
			runtime.GC()
			_ = pprof.WriteHeapProfile(f)
			_ = f.Close()
		}, nil
	case "block":
		runtime.SetBlockProfileRate(1)
		return dumpProfileAt(path, "block", func() { runtime.SetBlockProfileRate(0) }), nil
	case "mutex":
		runtime.SetMutexProfileFraction(1)
		return dumpProfileAt(path, "mutex", func() { runtime.SetMutexProfileFraction(0) }), nil
	case "trace":
		f, err := createProfileFile(filepath.Join(outDir, "trace.out"))
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

// dumpProfileAt returns a stop func that writes the named pprof profile
// to path, then runs disable to turn sampling back off. Shared by the
// block and mutex modes, which differ only in profile name and the
// sampling toggle.
func dumpProfileAt(path, name string, disable func()) func() {
	return func() {
		f, err := createProfileFile(path)
		if err != nil {
			return
		}
		_ = pprof.Lookup(name).WriteTo(f, 0)
		disable()
		_ = f.Close()
	}
}

// createProfileFile opens path for writing a profile. The G304 nolint
// is centralized here: every caller composes path from the trusted
// --profile-out directory plus a fixed mode/filename suffix, so it is
// not an attacker-controlled path.
func createProfileFile(path string) (*os.File, error) {
	return os.Create(path) //nolint:gosec // path = trusted --profile-out + fixed suffix
}
