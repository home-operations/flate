package cli

import (
	"bytes"
	"log" //nolint:depguard // Standard-log routing must capture or suppress dependency log output.
	"log/slog"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // test changes process-wide logging.
func TestSetLogLevel_StdlibLogRouting(t *testing.T) {
	saved, writer := slog.Default(), log.Writer()
	t.Cleanup(func() { slog.SetDefault(saved); log.SetOutput(writer) })
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			var sink bytes.Buffer
			assert.NoError(t, setLogLevel(level, &sink))
			log.Printf("dependency diagnostic")
			{
				got := strings.Contains(sink.String(), "dependency diagnostic")
				assert.FatalIf(t, got != (level == "debug"), "stdlib destination at %s: %q", level, sink.String())
			}
		})
	}
}

//nolint:paralleltest // test changes process-wide logging.
func TestSetLogLevel_ExplicitLevelStreamsLive(t *testing.T) {
	saved, writer := slog.Default(), log.Writer()
	t.Cleanup(func() { slog.SetDefault(saved); log.SetOutput(writer) })
	for _, level := range []string{"default", "debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			var sink bytes.Buffer
			value := level
			if level == "default" {
				defaultLevel, err := New("test", RunConfig{}).PersistentFlags().GetString("log-level")
				assert.NoError(t, err)
				value = defaultLevel
				assert.FatalIf(t, value != "error", "advertised log default = %q, want error", value)
			}
			assert.NoError(t, setLogLevel(value, &sink))
			slog.Error("recovered panic", "panic", "failure")
			assert.FatalIf(
				t,
				!strings.Contains(sink.String(), "level=ERROR msg=\"recovered panic\""),
				"error record was not delivered immediately: %q",
				sink.String(),
			)
			sink.Reset()
			slog.Info("operation complete")
			{
				got := strings.Contains(
					sink.String(),
					"operation complete",
				)
				assert.FatalIf(t, got != (level == "debug" || level == "info"), "info threshold: %q", sink.String())
			}
			sink.Reset()
			slog.Debug("operation detail")
			{
				got := strings.Contains(sink.String(), "operation detail")
				assert.FatalIf(t, got != (level == "debug"), "debug threshold: %q", sink.String())
			}
		})
	}
}
