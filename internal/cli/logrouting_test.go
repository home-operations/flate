package cli

import (
	"bytes"
	"log"
	"log/slog"
	"strings"
	"testing"
)

func TestSetLogLevel_StdlibLogRouting(t *testing.T) {
	saved, writer := slog.Default(), log.Writer()
	t.Cleanup(func() { slog.SetDefault(saved); log.SetOutput(writer) })
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			var sink bytes.Buffer
			if err := setLogLevel(level, &sink); err != nil {
				t.Fatal(err)
			}
			log.Printf("dependency diagnostic")
			if got := strings.Contains(sink.String(), "dependency diagnostic"); got != (level == "debug") {
				t.Fatalf("stdlib destination at %s: %q", level, sink.String())
			}
		})
	}
}

func TestSetLogLevel_ExplicitLevelStreamsLive(t *testing.T) {
	saved, writer := slog.Default(), log.Writer()
	t.Cleanup(func() { slog.SetDefault(saved); log.SetOutput(writer) })
	for _, level := range []string{"default", "debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			var sink bytes.Buffer
			value := level
			if level == "default" {
				defaultLevel, err := New("test").PersistentFlags().GetString("log-level")
				if err != nil {
					t.Fatal(err)
				}
				value = defaultLevel
				if value != "error" {
					t.Fatalf("advertised log default = %q, want error", value)
				}
			}
			if err := setLogLevel(value, &sink); err != nil {
				t.Fatal(err)
			}
			slog.Error("recovered panic", "panic", "failure")
			if !strings.Contains(sink.String(), "level=ERROR msg=\"recovered panic\"") {
				t.Fatalf("error record was not delivered immediately: %q", sink.String())
			}
			sink.Reset()
			slog.Info("operation complete")
			if got := strings.Contains(sink.String(), "operation complete"); got != (level == "debug" || level == "info") {
				t.Fatalf("info threshold: %q", sink.String())
			}
			sink.Reset()
			slog.Debug("operation detail")
			if got := strings.Contains(sink.String(), "operation detail"); got != (level == "debug") {
				t.Fatalf("debug threshold: %q", sink.String())
			}
		})
	}
}
