package helm

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
)

type valuesFiles struct {
	names      []string
	normalized bool
}

// Prepare MUST preserve authored dependencies for accepted-history identity.
func Prepare(
	hr *manifest.HelmRelease,
	lookup func(manifest.NamedResource) manifest.BaseManifest,
	inputs []values.Result,
	cache *values.Cache,
) (*manifest.HelmRelease, error) {
	hr = hr.Clone()
	if err := hr.ResolveChartSource(lookup); err != nil {
		return nil, err
	}
	authored := hr.ValuesFrom
	err := values.ExpandValueReferences(hr, inputs, cache)
	if err != nil {
		return nil, err
	}
	hr.ValuesFrom = authored
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
