package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/pkg/source"
)

// newCacheCmd builds the `flate cache` subcommand tree. Today there's
// one verb (gc); the parent exists so future cache-maintenance
// commands (size, locate, prune-by-key, etc.) plug in alongside.
func newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Inspect and prune flate's on-disk cache",
	}
	cmd.AddCommand(newCacheGCCmd())
	return cmd
}

// cacheGCFlags captures the GC verb's input.
type cacheGCFlags struct {
	maxAge         time.Duration
	includeMirrors bool
	dryRun         bool
}

// newCacheGCCmd wires `flate cache gc` — age-prunes per-cache subdirs
// (sources/, baselines/, blobs/sha256/), removes dangling refs, and
// optionally extends the prune to git mirrors.
func newCacheGCCmd() *cobra.Command {
	f := &cacheGCFlags{}
	c := &commonFlags{}
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Prune stale entries from flate's on-disk cache",
		Long: `Walks the cache root, removing entries whose mtime is older than
--max-age. Sources, baseline trees, and CAS blobs are pruned by age.
Dangling refs (digest pointers whose blob has been swept) are
removed regardless of age. Bare git mirrors are preserved by default
because re-hydrating them is expensive; pass --include-mirrors to
age-prune them too.

Set --dry-run to see what would be removed without touching disk.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if f.maxAge < 0 {
				return errors.New("--max-age must be non-negative")
			}
			root := c.resolveCacheRoot()
			res, err := source.Sweep(root, source.SweepOpts{
				MaxAge:         f.maxAge,
				IncludeMirrors: f.includeMirrors,
				DryRun:         f.dryRun,
			})
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			prefix := ""
			if f.dryRun {
				prefix = "[dry-run] "
			}
			for _, p := range res.Removed {
				_, _ = fmt.Fprintln(w, prefix+p)
			}
			_, _ = fmt.Fprintf(w, "%s%d entries, %s reclaimed",
				prefix, len(res.Removed), formatBytes(res.Bytes))
			if len(res.Errors) > 0 {
				_, _ = fmt.Fprintf(w, ", %d errors", len(res.Errors))
			}
			_, _ = fmt.Fprintln(w)
			for _, e := range res.Errors {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "warn: %v\n", e)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&f.maxAge, "max-age", 30*24*time.Hour,
		"prune entries with mtime older than this duration (default 30d); 0 disables age pruning")
	cmd.Flags().BoolVar(&f.includeMirrors, "include-mirrors", false,
		"also age-prune git mirrors (default off — mirrors are expensive to rebuild)")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false,
		"report what would be removed without touching disk")
	cmd.Flags().StringVar(&c.cacheDir, "cache-dir", "",
		"cache root to sweep (defaults to the same path flate uses for fetched artifacts)")
	return cmd
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
