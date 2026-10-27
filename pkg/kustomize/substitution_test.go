package kustomize

import (
	"errors"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestSubstitute_StrictOperators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input, want string
		vars              map[string]string
		fail              bool
	}{
		{name: "undefined", input: "${VALUE}", fail: true},
		{name: "defined", input: "${VALUE}", vars: map[string]string{"VALUE": "provided"}, want: "provided"},
		{name: "defined empty", input: "${VALUE}", vars: map[string]string{"VALUE": ""}},
		{name: "colon default", input: "${VALUE:-fallback}", want: "fallback"},
		{name: "assignment default", input: "${VALUE:=fallback}", want: "fallback"},
		{name: "empty default", input: "${VALUE:=fallback}", vars: map[string]string{"VALUE": ""}, want: "fallback"},
		{name: "escape", input: "$${VALUE}", want: "${VALUE}"},
		{name: "missing transform", input: "${VALUE^^}", fail: true},
		{name: "defined transform", input: "${VALUE^^}", vars: map[string]string{"VALUE": "provided"}, want: "PROVIDED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Substitute([]byte(tc.input), tc.vars, true)
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
			if tc.fail {
				if !strings.Contains(err.Error(), "variable not set (strict mode)") || !strings.Contains(err.Error(), "VALUE") {
					t.Fatalf("engine diagnostic changed: %v", err)
				}
			} else if string(got) != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
	out, err := Substitute([]byte("${UNDEFINED}"), nil, false)
	if err != nil || len(out) != 0 {
		t.Fatalf("default exported caller changed: %q, %v", out, err)
	}
}

func TestPrepare_LayersAndImmutability(t *testing.T) {
	t.Parallel()
	docs, err := testutil.SplitDocs([]byte(testutil.KustomizationYAML("consumer", "apps", `  path: ./app
  postBuild:
    substitute: {VALUE: inline, EXTRA: "one\ntwo"}
    substituteFrom: [{kind: ConfigMap, name: first}, {kind: ConfigMap, name: second}]
`)))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	original := obj.(*manifest.Kustomization)
	snapshot := original.Clone()
	projected := original.WithNamespace("effective").(*manifest.Kustomization)
	st := store.New()
	for _, cm := range []*manifest.ConfigMap{
		{Name: "first", Namespace: "effective", Data: map[string]any{"VALUE": "first", "REF": "first", "NORMALIZED": "one\ntwo"}},
		{Name: "second", Namespace: "effective", Data: map[string]any{"VALUE": "second", "REF": "second"}},
	} {
		st.AddObject(cm)
	}
	provider := values.NewStoreProvider(st, true)
	prepared, err := Prepare(projected, provider)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"VALUE": "inline", "REF": "second", "NORMALIZED": "onetwo", "EXTRA": "one\ntwo"}
	if d := cmp.Diff(want, prepared.PostBuild.Substitute); d != "" {
		t.Fatalf("layers (-want +got): %s", d)
	}
	if got := prepared.Namespace; got != projected.Namespace {
		t.Fatalf("prepared namespace = %v, want %s", got, projected.Namespace)
	}
	if d := cmp.Diff(snapshot, original, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); d != "" {
		t.Fatalf("stored input mutated (-want +got): %s", d)
	}
	for _, tc := range []struct {
		name      string
		postBuild *kustomizev1.PostBuild
		want      bool
	}{
		{name: "absent"}, {name: "empty", postBuild: &kustomizev1.PostBuild{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "plain", Namespace: "apps"}
			ks.PostBuild = tc.postBuild
			got, err := Prepare(ks, provider)
			if err != nil {
				t.Fatal(err)
			}
			if (got.PostBuild != nil && len(got.PostBuild.Substitute) > 0) != tc.want {
				t.Fatal("empty authored postBuild enabled substitution")
			}
		})
	}
	bad := original.Clone()
	bad.PostBuild.Substitute = map[string]string{"z-invalid": "private", "a-invalid": "private"}
	invalid, err := Prepare(bad, provider)
	if invalid != nil || !errors.Is(err, manifest.ErrInvalidSubstituteReference) || !strings.Contains(err.Error(), "[a-invalid z-invalid]") {
		t.Fatalf("invalid keys not classified/sorted: %v", err)
	}
}
