package loader

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_GeneratorData(t *testing.T) {
	t.Parallel()
	const encrypted = "ENC[AES256_GCM,data:fixture,iv:x,tag:y,type:str]"
	for _, tc := range []struct {
		name, kind, namespace, input, value, want string
		wipe, existing, symlink, external, escape bool
	}{
		{name: "outside_flux_graph", kind: "configMap", input: "literals: [VALUE=fixture]", want: "fixture", external: true},
		{name: "missing_file", kind: "configMap", input: "files: [VALUE=missing.txt]"},
		{name: "outside_root", kind: "configMap", input: "files: [VALUE=../../../outside.txt]"},
		{name: "resource_outside_root", kind: "configMap", input: "literals: [VALUE=outside]", escape: true},
		{name: "symlink_escape", kind: "configMap", input: "files: [VALUE=escape.txt]", symlink: true},
		{name: "implicit document", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "fixture", want: "fixture"},
		{name: "normalized environment", kind: "configMap", input: "env: ../input.txt", value: "VALUE=fixture", want: "fixture"},
		{name: "inside_root", kind: "configMap", input: "files: [VALUE=../input.txt]", value: "fixture", want: "fixture"},
		{name: "outer_namespace", kind: "configMap", namespace: "namespace: outer\n", input: "literals: [VALUE=fixture]", want: "fixture"},
		{name: "real_object_wins", kind: "configMap", input: "literals: [VALUE=generated]", existing: true, want: "real"},
		{name: "sops_configmap_wipe", kind: "configMap", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "sops_secret_wipe", kind: "secret", input: "envs: [../input.txt]", value: "VALUE=" + encrypted, wipe: true, want: "..PLACEHOLDER_VALUE.."},
		{name: "sops_secret_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: encrypted, want: encrypted},
		{name: "cleartext_preserved", kind: "secret", input: "files: [VALUE=../input.txt]", value: "fixture", wipe: true, want: "fixture"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			root := filepath.Join(parent, "repo")
			testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE")
			testutil.WriteFile(t, root, "apps/input.txt", tc.value)
			testutil.WriteFile(t, root, "apps/a/kustomization.yaml", tc.namespace+tc.kind+"Generator:\n- name: input\n  namespace: inner\n  "+tc.input+"\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n")
			if tc.escape {
				testutil.WriteFile(t, parent, "outside/kustomization.yaml", tc.kind+"Generator:\n- name: input\n  namespace: inner\n  "+tc.input+"\n")
				testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources: [../../../outside]\n"+tc.kind+"Generator:\n- name: sibling\n  namespace: inner\n  literals: [VALUE=sibling]\n")
			}
			if tc.symlink {
				if err := os.Symlink(filepath.Join(parent, "outside.txt"), filepath.Join(root, "apps/a/escape.txt")); err != nil {
					t.Fatal(err)
				}
			}
			st := store.New()
			ks := &manifest.Kustomization{Name: "owner", Namespace: "flux-system", Path: "./apps/a"}
			if tc.name == "implicit document" {
				ks.Path = "./apps"
			}
			if tc.external {
				ks.SourceRef.Kind, ks.SourceRef.Name, ks.SourceRef.Namespace = manifest.KindGitRepository, "external", "flux-system"
				st.AddObject(&manifest.GitRepository{Name: "external", Namespace: "flux-system"})
			}
			st.AddObject(ks)
			kind := manifest.KindConfigMap
			if tc.kind == "secret" {
				kind = manifest.KindSecret
			}
			ns := "inner"
			if tc.namespace != "" {
				ns = "outer"
			}
			id := manifest.NamedResource{Kind: kind, Namespace: ns, Name: "input"}
			if tc.existing {
				st.AddObject(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "real"}})
			}
			l := loaderFixture(st, nil)
			l.Options.WipeSecrets = tc.wipe
			l.documents = readerForTest(root)
			admitForTest(st, root)
			l.mountSources()
			l.documents.read(filepath.Join(root, "apps/a"))
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			obj := st.GetObject(id)
			if tc.want == "" {
				if obj != nil {
					t.Fatalf("failed generator materialized: %+v", obj)
				}
			} else {
				want := tc.want
				if kind == manifest.KindSecret {
					want = base64.StdEncoding.EncodeToString([]byte(want))
				}
				var got any
				switch o := obj.(type) {
				case *manifest.ConfigMap:
					got = o.Data["VALUE"]
				case *manifest.Secret:
					got = o.Data["VALUE"]
				}
				if got != want {
					t.Fatalf("VALUE=%v, want %q", got, want)
				}
			}
			var owners []manifest.NamedResource
			if !tc.external && !tc.escape {
				owners = []manifest.NamedResource{ks.Named()}
			}
			if got := testutil.ProductionOwners(l.Index.Records[id]); !slices.Equal(got, owners) {
				t.Fatalf("Production owners = %v, want %v", got, owners)
			}
			id.Name = "sibling"
			if st.GetObject(id) == nil {
				t.Fatal("failed generator stopped its sibling")
			}
		})
	}
}
