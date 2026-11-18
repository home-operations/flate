// Package chartinput enumerates regular files under Helm's input policies.
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
//
//nolint:wrapcheck // Helm directory-loader parity requires original traversal and cancellation errors.
func Walk(ctx context.Context, input fs.FS, directory bool, visit func(string, fs.FileInfo, []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var rules *ignore.Rules
	var policy []byte
	if directory {
		var err error
		rules, policy, err = loadIgnoreRules(ctx, input)
		if err != nil {
			return err
		}
	}
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if canceled := ctx.Err(); canceled != nil {
			return canceled
		}
		if err != nil {
			return err
		}
		info, admitted, err := chartFile(name, entry, rules)
		if !admitted {
			return err
		}
		var data []byte
		if directory && name == ignore.HelmIgnore {
			data = policy
		}
		return visit(name, info, data)
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func loadIgnoreRules(_ context.Context, input fs.FS) (*ignore.Rules, []byte, error) {
	rules := ignore.Empty()
	r, err := input.Open(ignore.HelmIgnore)
	if errors.Is(err, fs.ErrNotExist) {
		rules.AddDefaults()
		return rules, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("chart ignore rules: %w", err)
	}
	policy, readErr := io.ReadAll(io.LimitReader(r, archive.MaxDecompressedChartSize+1))
	if joinErr := errors.Join(readErr, r.Close()); joinErr != nil {
		return nil, nil, fmt.Errorf("chart ignore rules: %w", joinErr)
	}
	if int64(len(policy)) > archive.MaxDecompressedChartSize {
		return nil, nil, fmt.Errorf("chart ignore rules exceed maximum decompressed size")
	}
	rules, err = ignore.Parse(bytes.NewReader(policy))
	if err != nil {
		return nil, nil, fmt.Errorf("chart ignore rules: %w", err)
	}
	rules.AddDefaults()
	return rules, policy, nil
}

//nolint:wrapcheck // Helm directory-loader parity requires original file information errors.
func chartFile(name string, entry fs.DirEntry, rules *ignore.Rules) (fs.FileInfo, bool, error) {
	if name == "." {
		return nil, false, nil
	}
	info, err := entry.Info()
	if err != nil {
		return nil, false, err
	}
	if rules != nil && rules.Ignore(name, info) {
		if entry.IsDir() {
			return nil, false, fs.SkipDir
		}
		return nil, false, nil
	}
	if entry.IsDir() {
		return nil, false, nil
	}
	if !info.Mode().IsRegular() {
		return nil, false, fmt.Errorf("cannot load irregular file %s as it has file mode type bits set", name)
	}
	return info, true, nil
}
