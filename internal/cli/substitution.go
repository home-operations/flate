package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
)

func supplyEnv(input string) ([]string, error) {
	if input == "" {
		return nil, nil
	}
	paths := filepath.SplitList(input)
	for i, path := range paths {
		if path == "" {
			return nil, manifest.NewError(manifest.ErrInput, nil,
				fmt.Sprintf("%s path %d is empty", envKey("supply"), i+1))
		}
	}
	return paths, nil
}

func (c *commonFlags) loadInputs(ctx context.Context, stdin io.Reader) error {
	for _, kind := range c.allowMissingKinds {
		if kind != manifest.KindSecret && kind != manifest.KindConfigMap &&
			kind != manifest.KindCustomResourceDefinition {
			return manifest.NewError(manifest.ErrInput, nil,
				fmt.Sprintf("--allow-missing-kinds: unsupported kind %q", kind))
		}
	}
	if first := slices.Index(c.supply, "-"); first >= 0 && slices.Contains(c.supply[first+1:], "-") {
		return manifest.NewError(manifest.ErrInput, nil, "--supply: standard input may appear only once")
	}
	for _, path := range c.supply {
		if err := c.loadInput(ctx, path, stdin); err != nil {
			return err
		}
	}
	return loader.ValidateDeclarations(c.suppliedObjects)
}

func (c *commonFlags) loadInput(ctx context.Context, path string, stdin io.Reader) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("load supplied input: %w", err)
	}
	r := stdin
	var file *os.File
	if path != "-" {
		f, err := os.Open(path)
		if err != nil {
			return manifest.NewError(manifest.ErrInput, fmt.Errorf("--supply %q: %w", path, err), "")
		}
		r, file = f, f
	}
	children, err := loader.LoadDocuments(ctx, r, path)
	if file != nil {
		_ = file.Close()
	}
	if err != nil {
		return err
	}
	c.suppliedObjects = append(c.suppliedObjects, children...)
	return nil
}
