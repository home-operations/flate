// Package chartinput enumerates files admitted by Helm's chart ignore policy.
package chartinput

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	"helm.sh/helm/v4/pkg/ignore"
)

// Walk visits admitted regular files, borrowing already-read policy bytes for .helmignore.
func Walk(ctx context.Context, input fs.FS, visit func(string, fs.FileInfo, []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rules := ignore.Empty()
	var policy []byte
	if r, err := input.Open(ignore.HelmIgnore); err == nil {
		data, readErr := io.ReadAll(io.LimitReader(r, archive.MaxDecompressedChartSize+1))
		if err := errors.Join(readErr, r.Close()); err != nil {
			return fmt.Errorf("chart ignore rules: %w", err)
		}
		if int64(len(data)) > archive.MaxDecompressedChartSize {
			return fmt.Errorf("chart ignore rules exceed maximum decompressed size")
		}
		policy = data
		rules, err = ignore.Parse(bytes.NewReader(policy))
		if err != nil {
			return fmt.Errorf("chart ignore rules: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("chart ignore rules: %w", err)
	}
	rules.AddDefaults()
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if canceled := ctx.Err(); canceled != nil {
			return canceled
		}
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if rules.Ignore(name, info) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("cannot load irregular file %s as it has file mode type bits set", name)
		}
		var data []byte
		if name == ignore.HelmIgnore {
			data = policy
		}
		return visit(name, info, data)
	})
	if err != nil {
		return err
	}
	return ctx.Err()
}
