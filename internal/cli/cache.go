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

func newCacheGCCmd() *cobra.Command {
	f := &cacheGCFlags{}
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Prune stale cache tags and unused Git generations",
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
				_, fprintlnErr := fmt.Fprintln(w, prefix+p)
				writeErr = errors.Join(writeErr, fprintlnErr)
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
			report := output.Report{Groups: []output.ReportGroup{{}}}
			for warningIndex := range warnings {
				warning := warnings[warningIndex]
				report.Warnings = append(report.Warnings, output.Warning{Warning: warning})
			}
			report.Prepare(nil)
			encoder := output.Encoder{Writer: stderr, Color: output.ColorEnabled(stderr)}
			return errors.Join(writeErr, encoder.Report(&report, encoder))
		},
	}
	cmd.Flags().
		DurationVar(&f.maxAge, "max-age", 30*24*time.Hour, "Prune cache entries unused for this duration (0 disables)")
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "List cache entries to prune without deleting them")
	bindCacheDir(cmd.Flags(), &f.cacheDir, "Prune this cache directory (required)")
	return cmd
}

func bindCacheDir(fs *pflag.FlagSet, dest *string, usage string) {
	fs.StringVar(dest, "cache-dir", "", usage)
}

// formatBytes renders byte counts as kib/mib/gib strings. Approximate;
// suited for human-facing summaries.
func formatBytes(n int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(gib))
	case n >= mib:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(mib))
	case n >= kib:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(kib))
	}
	return fmt.Sprintf("%d B", n)
}
