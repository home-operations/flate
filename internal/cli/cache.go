package cli

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
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
	cmd.AddCommand(newCacheClearRenderCmd())
	return cmd
}

// newCacheClearRenderCmd wires `flate cache clear-render` — wipes the
// persisted helm template-output cache. Cheap to rebuild (every miss
// just re-runs action.Install) so we don't need an age filter or dry-
// run dance: it's a one-shot truncate of <root>/render/helm. Useful
// when a chart's render path changes shape (helm dependency bump,
// flate's filter pipeline updated) and the user wants to drop the
// stale entries without waiting for them to age out.
func newCacheClearRenderCmd() *cobra.Command {
	cf := &cacheFlags{}
	cmd := &cobra.Command{
		Use:   "clear-render",
		Short: "Delete the persistent helm template-output cache",
		Long: `Removes <cache-root>/render/helm in its entirety. The next render
re-populates the cache as templates are computed. Use when a helm
dependency upgrade or a flate post-render change makes the on-disk
entries semantically stale faster than the size cap would prune
them.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			layout := cacheroot.New(cf.resolveCacheRoot())
			dir := layout.RenderHelmCache()
			// os.RemoveAll on a non-existent path returns nil — the
			// "no cache to clear" idempotency is free. Any other
			// error is a real filesystem fault worth surfacing.
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("clear %s: %w", dir, err)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "cleared %s\n", dir)
			return nil
		},
	}
	cmd.Flags().StringVar(&cf.cacheDir, "cache-dir", "",
		"cache root containing the render cache (defaults to the same path flate uses for fetched artifacts)")
	return cmd
}

// cacheFlags holds the minimal set of flags needed by cache sub-commands.
// It intentionally avoids embedding commonFlags — cache gc doesn't need
// --path, --output, namespace filters, helm options, or any other
// reconcile-oriented flag.
type cacheFlags struct {
	cacheDir string
}

// resolveCacheRoot resolves the cache root, mirroring
// commonFlags.resolveCacheRoot so both flag types expose the same
// method name over the shared package-level helper.
func (f *cacheFlags) resolveCacheRoot() string {
	return resolveCacheRoot(&f.cacheDir)
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
	cf := &cacheFlags{}
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
			layout := cacheroot.New(cf.resolveCacheRoot())
			res, err := source.Sweep(layout, source.SweepOpts{
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
	cmd.Flags().StringVar(&cf.cacheDir, "cache-dir", "",
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
