package cli

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoadSubstitutions_InputValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		ordinal    int
	}{
		{"scalar", "private-fixture-value\n", 1},
		{"sequence", "[private-fixture-value]\n", 1},
		{"ordinal after null", "---\nnull\n---\n\n---\n[private-fixture-value]\n", 3},
		{"decoder error", "data: [private-fixture-value\n", 1},
		{"duplicate YAML key", "data: {VALUE: private-fixture-value, VALUE: another}\n", 1},
		{"empty mapping", "{}\n", 1},
		{"invalid api", strings.Replace(externalCM, "apiVersion: v1", "apiVersion: example.io/v1/invalid", 1), 1},
		{
			"invalid Secret data",
			"apiVersion: v1\nkind: Secret\nmetadata: {name: auth, namespace: apps}\ndata: {passwor" +
				"d: private-fixture-value%%%}\n",
			1,
		},
		{"invalid data bag", strings.Replace(externalCM, "data: {VALUE: supplied}", "data: [private-fixture-value]", 1), 1},
		{"missing apiVersion", strings.Replace(externalCM, "apiVersion: v1\n", "", 1), 1},
		{"missing kind", strings.Replace(externalCM, "kind: ConfigMap\n", "", 1), 1},
		{
			"custom kind missing namespace",
			"apiVersion: example.io/v1\nkind: Widget\nmetadata: {name: custom}\nspec: {}\n---\napi" +
				"Version: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: wi" +
				"dgets.example.io}\nspec: {group: example.io, scope: Namespaced, names: {kind: Widget," +
				" plural: widgets}}\n",
			1,
		},
		{
			"typed decoding error",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: source" +
				", namespace: apps}\nspec: {interval: private-fixture-value}\n",
			1,
		},
		{"missing namespace", strings.Replace(externalCM, ", namespace: apps", "", 1), 1},
		{"missing name", strings.Replace(externalCM, "name: external, ", "", 1), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := substitutionInput(t, tc.body)
			c := commonFlags{supply: []string{path}}
			err := c.loadInputs(t.Context(), nil)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "expected input error: %v", err)
			for _, text := range []string{path, fmt.Sprintf("document %d", tc.ordinal)} {
				assert.FatalIf(t, !strings.Contains(err.Error(), text), "missing %q in %v", text, err)
			}
			assert.FatalIf(
				t,
				strings.Contains(err.Error(), "private-fixture-value"),
				"%v",
				"input value leaked in error",
			)
		})
	}
	t.Run("unreadable", func(t *testing.T) {
		t.Parallel()
		c := commonFlags{supply: []string{filepath.Join(t.TempDir(), "absent.yaml")}}
		if err := c.loadInputs(t.Context(), nil); !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("expected input error: %v", err)
		}
	})
	t.Run("canceled before unreadable file", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		c := commonFlags{supply: []string{filepath.Join(t.TempDir(), "absent.yaml")}}
		if err := c.loadInputs(ctx, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation before file I/O: %v", err)
		}
	})
	t.Run("empty and null", func(t *testing.T) {
		t.Parallel()
		c := commonFlags{supply: []string{substitutionInput(t, "---\nnull\n---\n\n")}}
		if err := c.loadInputs(t.Context(), nil); err != nil || len(c.suppliedObjects) != 0 {
			t.Fatalf("empty documents: %v", err)
		}
	})
}

func TestLoadSubstitutions_CustomScope(t *testing.T) {
	t.Parallel()
	object := "apiVersion: example.io/v1\nkind: Widget\nmetadata: {name: custom}\nspec: {}\n"
	cluster := "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name:" +
		" widgets.example.io}\nspec: {group: example.io, scope: Cluster, names: {kind: Widget," +
		" plural: widgets}}\n"
	namespaced := strings.Replace(cluster, "scope: Cluster", "scope: Namespaced", 1)
	for _, tc := range []struct {
		name      string
		inputs    []string
		namespace string
		wantErr   bool
	}{
		{name: "unknown scope", inputs: []string{object}},
		{name: "cluster CRD first", inputs: []string{cluster + "---\n" + object}},
		{name: "cluster CRD last", inputs: []string{object + "---\n" + cluster}},
		{name: "cluster CRD in another file", inputs: []string{object, cluster}},
		{name: "later CRD changes to cluster scope", inputs: []string{namespaced + "---\n" + object, cluster}},
		{name: "later CRD changes to namespaced scope", inputs: []string{cluster + "---\n" + object, namespaced}, wantErr: true},
		{name: "namespaced CRD in another file", inputs: []string{object, namespaced}, wantErr: true},
		{
			name: "authored namespace",
			inputs: []string{
				strings.Replace(object,
					"name: custom",
					"name: custom, namespace: apps",
					1),
				namespaced,
			},
			namespace: "apps",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var paths []string
			for _, input := range tc.inputs {
				paths = append(paths, substitutionInput(t, input))
			}
			c := commonFlags{supply: paths}
			err := c.loadInputs(t.Context(), nil)
			if tc.wantErr {
				assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "missing namespace error: %v", err)
				return
			}
			assert.NoError(t, err)
			for _, child := range c.suppliedObjects {
				id := child.Obj.Named()
				if id.Kind != "Widget" {
					continue
				}
				assert.FatalIf(
					t,
					id.Namespace != tc.namespace || id.Name != "custom",
					"custom identity changed: %+v",
					id,
				)
			}
		})
	}
}
