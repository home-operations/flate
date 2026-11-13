package cli

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_CommandContext(t *testing.T) {
	for _, command := range []string{"build all", "get all", "get hr", "get images", "test all", "diff all", "diff images"} {
		for _, embedded := range []bool{false, true} {
			t.Run(command+"/embedded="+strconv.FormatBool(embedded), func(t *testing.T) {
				root := New("test", RunConfig{})
				args := strings.Fields(command)
				if embedded {
					parent := &cobra.Command{Use: "parent", SilenceUsage: true, SilenceErrors: true}
					parent.AddCommand(root)
					root = parent
					args = append([]string{"flate"}, args...)
				}
				args = append(args, "--path", writeFixture(t), "--concurrency=2")
				if strings.HasPrefix(command, "diff") {
					args = append(args, "--path-orig", writeFixture(t))
				}
				root.SetArgs(args)
				root.SetOut(io.Discard)
				root.SetErr(io.Discard)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				{
					err := root.ExecuteContext(ctx)
					assert.FatalIf(t, !errors.Is(err, context.Canceled), "handler lost cancellation: %v", err)
				}
			})
		}
	}
}
