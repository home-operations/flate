package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

// newCacheCmd builds the cache-maintenance subcommand tree.
func newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Prune flate's persistent cache",
		Args:  cobra.NoArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(newCacheGCCmd())
	return cmd
}

// cacheGCFlags captures the GC verb's input.
type cacheGCFlags struct {
	cacheDir string
	maxAge   time.Duration
	dryRun   bool
}

// newCacheGCCmd retires tags and unreferenced Git generations.
func newCacheGCCmd() *cobra.Command {
	f := &cacheGCFlags{}
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Prune stale cache tags and unused Git generations",
		Long: `Retires named cache tags whose payloads were last used more than --max-age
ago, then collects unreachable OCI blobs and Git generations. Shared
payloads survive while reachable from a retained tag.

Requires an explicit --cache-dir or FLATE_CACHE_DIR.

Set --dry-run to see what would be removed without touching disk.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if f.maxAge < 0 {
				return errors.New("--max-age must be non-negative")
			}
			res, err := cache.Sweep(cmd.Context(), f.cacheDir, cache.SweepOpts{
				MaxAge: f.maxAge,
				DryRun: f.dryRun,
			})
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			prefix := ""
			if f.dryRun {
				prefix = "[dry-run] "
			}
			var writeErr error
			for _, p := range res.Removed {
				_, err := fmt.Fprintln(w, prefix+p)
				writeErr = errors.Join(writeErr, err)
			}
			_, err = fmt.Fprintf(w, "%s%d entries, %s in Git generations; blob reclamation is decided by OCI graph GC",
				prefix, len(res.Removed), formatBytes(res.Bytes))
			writeErr = errors.Join(writeErr, err)
			if len(res.Errors) > 0 {
				_, err = fmt.Fprintf(w, ", %d errors", len(res.Errors))
				writeErr = errors.Join(writeErr, err)
			}
			_, err = fmt.Fprintln(w)
			writeErr = errors.Join(writeErr, err)
			warnings := make([]manifest.Warning, len(res.Errors))
			for i, err := range res.Errors {
				warnings[i] = manifest.Warning{Category: manifest.WarnCacheMaintenance, Message: err.Error()}
			}
			stderr := cmd.ErrOrStderr()
			return errors.Join(writeErr, (output.Encoder{Writer: stderr, Color: output.ColorEnabled(stderr)}).Report(output.Model{Warnings: warnings}))
		},
	}
	cmd.Flags().DurationVar(&f.maxAge, "max-age", 30*24*time.Hour,
		"prune entries with mtime older than this duration (default 30d); 0 disables age pruning")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false,
		"report what would be removed without touching disk")
	bindCacheDir(cmd.Flags(), &f.cacheDir,
		"explicit cache root to sweep (or set FLATE_CACHE_DIR)")
	return cmd
}

func bindCacheDir(fs *pflag.FlagSet, dest *string, usage string) {
	fs.StringVar(dest, "cache-dir", "", usage)
}

// formatBytes renders byte counts as KiB/MiB/GiB strings. Approximate;
// suited for human-facing summaries.
func formatBytes(n int64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(GiB))
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(MiB))
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(KiB))
	}
	return fmt.Sprintf("%d B", n)
}
