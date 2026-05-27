package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// envPrefix is prepended to every flag name to form its env-var key.
// `--path` ↦ FLATE_PATH, `--skip-kinds` ↦ FLATE_SKIP_KINDS, etc.
const envPrefix = "FLATE_"

// envSkipFlags lists flag names whose env-var binding would be useless
// or actively harmful. `help` and `version` short-circuit normal
// command execution when set, so `FLATE_HELP=true` would silently
// disable every other invocation — better to leave them CLI-only.
var envSkipFlags = map[string]bool{
	"help":    true,
	"version": true,
}

// envKey returns the env-var name corresponding to a cobra flag.
// Identity transform: kebab → snake, uppercase, FLATE_ prefix.
func envKey(flagName string) string {
	return envPrefix + strings.ReplaceAll(strings.ToUpper(flagName), "-", "_")
}

// applyEnvVars fills any flag on cmd that wasn't explicitly set on
// the command line from its FLATE_<UPPER_SNAKE> env var. CLI args
// always win — env vars only cover the gap. Slice/map/bool/duration
// values parse the same form pflag accepts on argv (CSV for slices,
// `k=v,k=v` for maps, `true/false/1/0` for bools, `30m` for durations).
// An invalid value fails loud with the offending env key named.
func applyEnvVars(cmd *cobra.Command) error {
	var firstErr error
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if firstErr != nil || f.Changed || envSkipFlags[f.Name] {
			return
		}
		key := envKey(f.Name)
		v, ok := os.LookupEnv(key)
		if !ok {
			return
		}
		if err := f.Value.Set(v); err != nil {
			firstErr = fmt.Errorf("invalid %s %q: %w", key, v, err)
			return
		}
		f.Changed = true
	})
	return firstErr
}

// annotateEnvUsage appends `[env: FLATE_X]` to every flag's Usage in
// the command tree so the env-var surface is discoverable through
// `--help`. cobra short-circuits on `--help` without running PreRun,
// so the annotation must be baked into the usage string at command-
// construction time.
//
// Walks LocalFlags() at every node: that's local-non-persistent +
// own-persistent, which covers each flag exactly once across the
// tree — root contributes its own persistent flags (e.g. log-level),
// subcommands contribute their local flags, and inherited persistent
// flags are picked up via cobra's shared *pflag.Flag pointer so
// rendering inherited flags shows the annotation too. Using Flags()
// here would miss root's own persistent flags (cobra only merges
// persistent flags from *parents*, and root has none).
func annotateEnvUsage(root *cobra.Command) {
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
			if envSkipFlags[f.Name] {
				return
			}
			f.Usage = strings.TrimRight(f.Usage, " ") + " [env: " + envKey(f.Name) + "]"
		})
		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(root)
}
