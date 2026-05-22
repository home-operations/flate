package kustomize

import (
	"testing"
)

func TestFilterKinds(t *testing.T) {
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "Secret"},
		{"kind": "Service"},
	}
	out := FilterKinds(docs, []string{"ConfigMap"})
	if len(out) != 1 || out[0]["kind"] != "ConfigMap" {
		t.Errorf("FilterKinds: %+v", out)
	}
	out = ExcludeKinds(docs, []string{"Secret", "Service"})
	if len(out) != 1 || out[0]["kind"] != "ConfigMap" {
		t.Errorf("ExcludeKinds: %+v", out)
	}
}

func TestSubstitute(t *testing.T) {
	in := []byte(`hello ${NAME}, version=${VERSION:=v1}, ${OPT:-x}`)
	out, err := Substitute(in, map[string]string{"NAME": "world"})
	if err != nil {
		t.Fatalf("Substitute: %v", err)
	}
	want := "hello world, version=v1, x"
	if string(out) != want {
		t.Errorf("Substitute: got %q want %q", out, want)
	}

	_, err = Substitute([]byte("${MISSING}"), nil)
	if err == nil {
		t.Errorf("expected error for missing var")
	}
}
