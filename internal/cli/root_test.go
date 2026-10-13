package cli

import (
	"bytes"
	"io"
	"log"
	"log/slog"
	"os"
	"testing"
)

func TestRun_ProgressSinkResetsPerCommand(t *testing.T) {
	savedBar, savedBuffer := barSink, logBuffer
	savedLogger, savedLogWriter := slog.Default(), log.Writer()
	t.Cleanup(func() {
		barSink, logBuffer = savedBar, savedBuffer
		slog.SetDefault(savedLogger)
		log.SetOutput(savedLogWriter)
	})

	// The TTY detector accepts character devices; cache gc never starts a renderer.
	terminal, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Close()

	for _, tc := range []struct {
		name   string
		args   []string
		stderr io.Writer
	}{
		{name: "no-progress", args: []string{"--no-progress"}, stderr: terminal},
		{name: "redirected stderr", stderr: &bytes.Buffer{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"cache", "gc", "--dry-run", "--cache-dir", t.TempDir()}
			if code := Run(append(args, "--no-progress=false"), io.Discard, terminal); code != 0 {
				t.Fatalf("first command exited %d", code)
			}
			if barSink == nil {
				t.Fatal("first command did not enable progress")
			}
			if code := Run(append(args, tc.args...), io.Discard, tc.stderr); code != 0 {
				t.Fatalf("second command exited %d", code)
			}
			if barSink != nil {
				t.Error("second command retained the first command's progress sink")
			}
		})
	}
}
