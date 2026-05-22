package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/pkg/manifest"
)

func newDiagCmd() *cobra.Command {
	var path string
	cmd := &cobra.Command{
		Use:   "diag",
		Short: "Sanity-check YAML manifests under a path",
		RunE: func(cmd *cobra.Command, _ []string) error {
			abs, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			ok := true
			err = filepath.WalkDir(abs, func(p string, d fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if d.IsDir() {
					if strings.HasPrefix(d.Name(), ".") && d.Name() != "." {
						return filepath.SkipDir
					}
					// Skip helm chart template fragment dirs which
					// hold Go-template YAML not valid as-is.
					if d.Name() == "templates" || d.Name() == "crds" {
						return filepath.SkipDir
					}
					return nil
				}
				ext := strings.ToLower(filepath.Ext(p))
				if ext != ".yaml" && ext != ".yml" {
					return nil
				}
				data, err := os.ReadFile(p) //nolint:gosec // p is a tree-walk result under the user-supplied --path
				if err != nil {
					return err
				}
				if len(bytes.TrimSpace(data)) == 0 {
					return nil
				}
				docs, err := manifest.SplitDocs(data)
				if err != nil {
					ok = false
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "[DIAGNOSTICS FAIL]: %s %v\n", p, err)
					return nil
				}
				if len(docs) == 0 {
					return nil
				}
				return nil
			})
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("diagnostics failed")
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "[DIAGNOSTICS OK]")
			return nil
		},
	}
	cmd.Flags().StringVar(&path, "path", ".", "path to scan")
	return cmd
}
