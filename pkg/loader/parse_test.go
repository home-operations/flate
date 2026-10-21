package loader

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestParseFile_ResultOwnership(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		cached  bool
		header  bool
		invalid bool
	}{
		{name: "Uncached"},
		{name: "CachedMissAndHit", cached: true},
		{name: "CachedDocumentFields", cached: true, header: true},
		{name: "UncachedPartialError", invalid: true},
		{name: "CachedPartialError", cached: true, invalid: true},
		{name: "CachedDocumentFieldsOnError", cached: true, header: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			path := filepath.Join(root, "objects.yaml")
			body := `apiVersion: v1
kind: ConfigMap
metadata: {name: first, namespace: apps}
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: apps}
spec: {target: {name: generated}}
---
`
			if tc.invalid {
				body += `apiVersion: source.toolkit.fluxcd.io/
kind: GitRepository
metadata: {name: invalid, namespace: apps}
---
`
			} else {
				body += `apiVersion: v1
kind: ConfigMap
metadata: {name: "${APP}", namespace: apps}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: template-namespace, namespace: "${NAMESPACE}"}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {namespace: apps}
---
`
			}
			body += "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: last, namespace: apps}\n"
			testutil.WriteFile(t, root, "objects.yaml", body)
			r := readerForTest(root)
			if !tc.cached {
				r.cache = nil
			}
			var header document
			if tc.header {
				header = document{body: &types.Kustomization{Namespace: "retained"}, file: "header.yaml", loaded: true}
				r.cache[path] = header
			}
			want := []string{"first", "last"}
			if tc.invalid {
				want = want[:1]
			}
			assertNames := func(objects []manifest.BaseManifest, names []string) {
				t.Helper()
				got := make([]string, 0, len(objects))
				for _, obj := range objects {
					if obj == nil {
						t.Fatal("nil object in parse result")
					}
					got = append(got, obj.Named().Name)
				}
				assert.Diff(t, got, names)
			}
			first, err := r.parseFile(path, manifest.ParseDocOptions{})
			assertNames(first, want)
			if tc.invalid {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("parse error = %v, want Flux input error", err)
				}
				invalid, ok := errors.AsType[*field.Error](err)
				if !ok {
					t.Fatalf("parse error = %v, want API-version admission error", err)
				}
				assert.Equal(t, invalid.Type, field.ErrorTypeInvalid)
				assert.Equal(t, invalid.Field, "apiVersion")
				assert.Equal(t, invalid.BadValue, any("source.toolkit.fluxcd.io/"))
				if !strings.HasPrefix(err.Error(), "parse "+path+": ") {
					t.Errorf("parse error lacks source coordinate %q: %v", path, err)
				}
				cached, exists := r.cache[path]
				assert.Equal(t, exists, tc.header)
				assert.Equal(t, cached.body, header.body)
				assert.Equal(t, cached.file, header.file)
				assert.Equal(t, cached.loaded, header.loaded)
				if cached.objects != nil {
					t.Fatal("partial decode was cached")
				}
				return
			}
			assert.NoError(t, err)
			slices.Reverse(first)
			first[0] = nil
			second, err := r.parseFile(path, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assertNames(second, want)
			slices.Reverse(second)
			second[0] = nil
			third, err := r.parseFile(path, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assertNames(third, want)
			if tc.cached {
				cached := r.cache[path]
				assertNames(cached.objects, []string{"first", "producer", "last"})
				if _, ok := cached.objects[1].(*manifest.RawObject); !ok {
					t.Errorf("cached producer = %T, want raw object", cached.objects[1])
				}
				assert.Equal(t, cached.body, header.body)
				assert.Equal(t, cached.file, header.file)
				assert.Equal(t, cached.loaded, header.loaded)
			}
		})
	}
}
