package helm

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/values"
	"github.com/home-operations/flate/pkg/manifest"
)

type valuesFiles struct {
	names      []string
	normalized bool
}

// Prepare clones a HelmRelease, resolves its chartRef and expands its values.
func Prepare(
	hr *manifest.HelmRelease,
	lookup func(manifest.NamedResource) manifest.BaseManifest,
	provider values.Provider,
	cache *values.Cache,
) (*manifest.HelmRelease, error) {
	hr = hr.Clone()
	if err := hr.ResolveChartSource(lookup); err != nil {
		return nil, err
	}
	err := values.ExpandValueReferences(hr, provider, cache)
	if err != nil {
		return nil, err
	}
	return hr, nil
}

func normalizeValuesFiles(names []string) (valuesFiles, error) {
	cleaned := names
	normalized := false
	for i, name := range names {
		clean := path.Clean(name)
		if name == "" || !fs.ValidPath(clean) || clean == "." || strings.IndexByte(clean, 0) >= 0 {
			return valuesFiles{names: nil, normalized: false}, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("read values file %q: %w", name, fs.ErrInvalid),
				"",
			)
		}
		if clean != name {
			if !normalized {
				cleaned = slices.Clone(names)
				normalized = true
			}
			cleaned[i] = clean
		}
	}
	return valuesFiles{names: cleaned, normalized: normalized}, nil
}
