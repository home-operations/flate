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

// newCacheCmd builds the cache-maintenance subcommand tree.
func newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Prune flate's on-disk cache directories",
	}
	cmd.AddCommand(newCacheGCCmd())
	cmd.AddCommand(newCacheClearRenderCmd())
	return cmd
}

// newCacheClearRenderCmd removes only the legacy Helm render directory.
func newCacheClearRenderCmd() *cobra.Command {
	cf := &cacheFlags{}
	cmd := &cobra.Command{
		Use:   "clear-render",
		Short: "Delete the legacy Helm render directory",
		Long: `Removes <cache-root>/render/helm in its entirety.
Completed Helm and Kustomize render frames share the OCI source cache;
this command does not remove those frames or shared source content.`,
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
		"cache root containing the legacy Helm render directory (defaults to the source cache root)")
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

// newCacheGCCmd prunes cache directories and unreferenced Git generations.
func newCacheGCCmd() *cobra.Command {
	f := &cacheGCFlags{}
	cf := &cacheFlags{}
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Prune stale cache directories and unused Git generations",
		Long: `Prunes source and baseline cache directories whose mtime is older than
--max-age, and removes unreferenced Git generations. Bare git mirrors
are preserved by default because re-hydrating them is expensive;
pass --include-mirrors to age-prune them too.

Shared OCI blob content and completed render frames are not pruned.

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
