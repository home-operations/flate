package kustomize

import (
	"errors"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestSubstituteStrict_Operators(t *testing.T) {
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
			got, err := SubstituteStrict([]byte(tc.input), tc.vars)
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
	out, err := Substitute([]byte("${UNDEFINED}"), nil)
	if err != nil || len(out) != 0 {
		t.Fatalf("default exported caller changed: %q, %v", out, err)
	}
}

func TestPrepareWithSubstitutions_LayersAndImmutability(t *testing.T) {
	docs, err := manifest.SplitDocs([]byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./app\n  postBuild:\n    substitute: {VALUE: inline}\n    substituteFrom: [{kind: ConfigMap, name: first}, {kind: ConfigMap, name: second}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	original := obj.(*manifest.Kustomization)
	snapshot := original.Clone()
	overlay := map[string]string{"VALUE": "overlay", "EXTRA": "one\ntwo"}
	st := store.New()
	for _, cm := range []*manifest.ConfigMap{
		{Name: "first", Namespace: "apps", Data: map[string]any{"VALUE": "first", "REF": "first", "NORMALIZED": "one\ntwo"}},
		{Name: "second", Namespace: "apps", Data: map[string]any{"VALUE": "second", "REF": "second"}},
	} {
		st.AddObject(cm)
	}
	provider := values.NewStoreProvider(st)
	prepared, err := PrepareWithSubstitutions(original, provider, overlay)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"VALUE": "overlay", "REF": "second", "NORMALIZED": "onetwo", "EXTRA": "one\ntwo"}
	if d := cmp.Diff(want, prepared.PostBuildSubstitute); d != "" {
		t.Fatalf("layers (-want +got): %s", d)
	}
	if d := cmp.Diff(snapshot, original); d != "" {
		t.Fatalf("stored input mutated (-want +got): %s", d)
	}
	if overlay["VALUE"] != "overlay" || overlay["EXTRA"] != "one\ntwo" {
		t.Fatal("overlay mutated")
	}
	if prepared.Contents["spec"].(map[string]any)["postBuild"].(map[string]any)["substitute"].(map[string]any)["VALUE"] != "overlay" {
		t.Fatal("effective values missing from prepared spec")
	}
	if values.VarsMap(prepared.PostBuildSubstitute)["EXTRA"] != "onetwo" {
		t.Fatal("overlay normalization changed")
	}
	ordinary, err := Prepare(original, provider)
	if err != nil || ordinary.PostBuildSubstitute["VALUE"] != "inline" {
		t.Fatalf("exported Prepare changed: %v", err)
	}
	for _, tc := range []struct {
		name      string
		postBuild *kustomizev1.PostBuild
		want      bool
	}{
		{name: "absent"}, {name: "empty", postBuild: &kustomizev1.PostBuild{}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ks := &manifest.Kustomization{Name: "plain", Namespace: "apps"}
			ks.PostBuild = tc.postBuild
			got, err := PrepareWithSubstitutions(ks, provider, overlay)
			if err != nil {
				t.Fatal(err)
			}
			if (len(got.PostBuildSubstitute) > 0) != tc.want {
				t.Fatal("overlay changed absent-postBuild behavior")
			}
		})
	}
	invalid, err := PrepareWithSubstitutions(original, provider, map[string]string{"z-invalid": "private", "a-invalid": "private"})
	if invalid != nil || !errors.Is(err, manifest.ErrInvalidSubstituteReference) || !strings.Contains(err.Error(), "[a-invalid z-invalid]") {
		t.Fatalf("invalid keys not classified/sorted: %v", err)
	}
}
