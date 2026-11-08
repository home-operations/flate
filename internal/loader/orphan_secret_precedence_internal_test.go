package loader

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPromoteOrphans_SecretPrecedence(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"standalone", "siblings", "siblings reversed", "mixed origins", "failed", "preexisting", "graph-owned", "unknown-file"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testPromoteSecretPrecedence(t, scenario)
		})
	}
}

func testPromoteSecretPrecedence(t *testing.T, scenario string) {
	t.Helper()
	dir := t.TempDir()
	st := store.New()
	admitForTest(st, dir)
	l := New(st)
	ids := []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "secure", Name: "a"}}
	if strings.HasPrefix(scenario, "siblings") || scenario == "mixed origins" {
		ids = append(ids, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "secure", Name: "b"})
	}
	if scenario == "siblings reversed" {
		slices.Reverse(ids)
	}
	saved := make([]*manifest.Secret, 0, len(ids))
	var body strings.Builder
	for _, id := range ids {
		body.WriteString(
			"---\napiVersion: v1\nkind: Secret\nmetadata: {name: " + id.Name + ", namespace: secure}\nstringData: {HOST: fixture-value}\n",
		)
		l.Index.Records[id] = manifest.DiscoveryRecord{
			Authored: id,
			Path:     filepath.Join(dir, "bundle.yaml"),
			File:     "bundle.yaml",
		}
		if scenario == "mixed origins" && id.Name == "b" {
			l.Index.Records[id] = manifest.DiscoveryRecord{
				Authored: id,
				Path:     filepath.Join(dir, "winner.yaml"),
				File:     "winner.yaml",
			}
		}
		if scenario == "unknown-file" {
			r := l.Index.Records[id]
			r.File = ""
			l.Index.Records[id] = r
		}
		value := "fixture-value"
		if scenario == "preexisting" {
			value = "preexisting-value"
		}
		obj := &manifest.Secret{
			Name:       id.Name,
			Namespace:  id.Namespace,
			StringData: map[string]any{"HOST": value},
		}
		saved = append(saved, obj)
		if scenario != "unknown-file" {
			doc := obj.Snapshot()
			doc["metadata"].(map[string]any)["uid"] = "saved-secret"
			st.AddObject(obj, doc)
		}
	}
	testutil.WriteFile(t, dir, "bundle.yaml", body.String())
	if scenario == "graph-owned" {
		prepareOrphanSecretOwner(t, l, dir, saved)
	}
	if scenario == "failed" {
		if err := os.Remove(filepath.Join(dir, "bundle.yaml")); err != nil {
			t.Fatal(err)
		}
	}
	assert.NoError(t, l.PromoteOrphans(t.Context()))
	for i := range ids {
		assertOrphanSecretPrecedence(t, st, scenario, saved[i])
	}
}

func prepareOrphanSecretOwner(t *testing.T, l *Loader, dir string, saved []*manifest.Secret) {
	t.Helper()
	st := l.Store
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
	ksDoc, err := testutil.SplitDocs(
		[]byte(
			testutil.KustomizationYAML(
				"owner",
				"flux-system",
				"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := manifest.ParseDoc(ksDoc[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	ks := parsed.Object
	st.AddObject(ks)
	source := &manifest.GitRepository{Name: "source", Namespace: ks.Named().Namespace}
	st.AddObject(source)
	st.SetArtifact(
		source.Named(),
		st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
	)
	l.SourceRoot = dir
	if _, err := l.Load(t.Context(), dir, manifest.NamedResource{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.FinalizeIndex(t.Context(), dir, true); err != nil {
		t.Fatal(err)
	}
	for _, obj := range saved {
		doc := obj.Snapshot()
		doc["metadata"].(map[string]any)["uid"] = "saved-secret"
		st.AddObject(obj, doc)
	}
}

func assertOrphanSecretPrecedence(t *testing.T, st *store.Store, scenario string, saved *manifest.Secret) {
	t.Helper()
	id := saved.Named()
	got, ok := st.GetObject(id).(*manifest.Secret)
	if scenario == "unknown-file" {
		if ok {
			t.Errorf("Secret without a source-file entry admitted: %+v", got)
		}
		return
	}
	if !ok {
		t.Fatalf("Secret %s missing", id)
	}
	if scenario == "standalone" || strings.HasPrefix(scenario, "siblings") ||
		scenario == "mixed origins" && id.Name == "a" {
		if got.StringData["HOST"] != "fixture-value" {
			t.Errorf("%s = %+v, want real file value", id, got)
		}
	} else if got != saved {
		t.Errorf("%s must preserve exact immutable object", id)
	}
	_, doc, emission := st.GetEmission(id)
	emission.Finish()
	if doc == nil {
		t.Fatal("promoted or restored secret lost its authoritative document")
	}
	if scenario == "failed" || scenario == "preexisting" || scenario == "graph-owned" ||
		scenario == "mixed origins" && id.Name == "b" {
		if doc["metadata"].(map[string]any)["uid"] != "saved-secret" {
			t.Fatalf("saved secret metadata lost: %v", doc)
		}
	}
	wantSaved := "fixture-value"
	if scenario == "preexisting" {
		wantSaved = "preexisting-value"
	}
	if saved.StringData["HOST"] != wantSaved {
		t.Error("saved immutable Secret mutated")
	}
}
