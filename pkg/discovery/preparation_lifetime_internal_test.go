package discovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_RejectedPreparationRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [owner.yaml, ready.yaml]\n")
	testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML(
		"owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n",
	))
	testutil.WriteFile(t, root, "entry/ready.yaml", testutil.KustomizationYAML("ready", "apps", "  path: ./ready\n"))
	testutil.WriteFile(t, root, "ready/kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	result, err := Run(t.Context(), Config{
		Input: source, Path: filepath.Join(root, "entry"), Store: st, Trees: testTrees(t),
	})
	assert.NoError(t, err)
	assert.Equal(t, result.Binding.Producer.ReportHidden(), true)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root + "-replacement", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), testutil.KustomizationID("apps", "owner")), false)
}

func TestPrepareDocuments_ReplacementRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "first"}
	first := owner.SourceID()
	st.SetArtifact(first, source)
	d := Session{cfg: Config{Store: st, Trees: testTrees(t)}, loader: loader.New(st)}
	_, err := d.prepareDocuments(t.Context(), owner, source, nil)
	assert.NoError(t, err)
	replacement := *owner
	replacement.SourceRef.Name = "second"
	second := replacement.SourceID()
	st.SetArtifact(second, source)
	_, err = d.prepareDocuments(t.Context(), &replacement, source, nil)
	assert.NoError(t, err)
	st.SetArtifact(first, &store.SourceArtifact{LocalPath: root + "-first", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), false)
	st.SetArtifact(second, &store.SourceArtifact{LocalPath: root + "-second", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), true)
}

func TestPrepareDocuments_ImplicitSourceIgnore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ignore  string
		exclude bool
	}{
		{name: "included", ignore: "# no exclusions\n"},
		{name: "excluded", ignore: "apps/cm.yaml\n", exclude: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, ".sourceignore", tc.ignore)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t, root, "apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: default}\ndata: {greeting: hi}\n",
			)
			st := store.New()
			source := &store.SourceArtifact{
				LocalPath: root, LocalRoot: root, FS: os.DirFS(root), Content: "snapshot", InputAlias: false,
			}
			st.SetArtifact(manifest.BootstrapSourceID, source)
			owner := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
			st.AddObject(owner)
			l := loader.New(st)
			defer l.Close()
			d := Session{cfg: Config{Store: st, Trees: testTrees(t)}, loader: l}
			defer d.Close()
			prepared, err := d.prepareDocuments(t.Context(), owner, source, nil)
			if tc.exclude {
				assert.FatalIf(t, err == nil, "excluded referenced input rendered successfully")
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, strings.Contains(err.Error(), "cm.yaml"), true)
				assert.Equal(t, strings.Contains(err.Error(), "file does not exist"), true)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, prepared.complete, true)
			assert.FatalIf(t, len(prepared.children) != 1, "prepared children = %d", len(prepared.children))
			output, err := yaml.Marshal(prepared.children[0].Doc)
			assert.NoError(t, err)
			assert.Equal(t, string(output), `apiVersion: v1
data:
  greeting: hi
kind: ConfigMap
metadata:
  labels:
    kustomize.toolkit.fluxcd.io/name: apps
    kustomize.toolkit.fluxcd.io/namespace: flux-system
  name: hello
  namespace: default
`)
		})
	}
}

func TestPreparePendingProducer_ReferencedSubstitutionSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(t, root, "child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./${TARGET}\n"))
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	st.SetArtifact(manifest.BootstrapSourceID, source)
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	owner.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
	}
	st.AddObject(owner)
	l := loader.New(st)
	defer l.Close()
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	d := Session{cfg: Config{Store: st, Trees: testTrees(t)}, loader: l}
	defer d.Close()
	for _, target := range []string{"one", "two"} {
		st.AddObject(&manifest.ConfigMap{
			Name: "settings", Namespace: "apps", Data: map[string]any{"TARGET": target},
		})
		if previous := d.prepared[owner.Named()]; previous != nil {
			assert.Equal(t, previous.support.Valid(), false)
		}
		prepared, prepareErr := d.preparePendingProducer(t.Context(), owner, nil)
		assert.NoError(t, prepareErr)
		assert.Equal(t, prepared, true)
		frame := d.prepared[owner.Named()]
		assert.FatalIf(t, frame == nil || len(frame.children) != 1, "prepared frame = %+v", frame)
		assert.Equal(t, frame.children[0].Obj.(*manifest.Kustomization).Path, "./"+target)
		assert.Equal(t, frame.support.Valid(), true)
		assert.Equal(t, owner.PostBuild.Substitute == nil, true)
	}
}

func TestPrepareProducer_RegistrationCancellation(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"producer", "pending producer", "qualification"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
			st := store.New()
			source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
			st.SetArtifact(manifest.BootstrapSourceID, source)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
			st.AddObject(owner)
			l := loader.New(st)
			defer l.Close()
			l.SourceRoot = root
			d := Session{cfg: Config{Store: st, Trees: testTrees(t)}, loader: l}
			defer d.Close()
			failure := errors.New("retained binding failure")
			d.binding.Producer, d.binding.Failure = owner.Named(), failure
			prepared, err := d.prepareDocuments(t.Context(), owner, source, nil)
			assert.NoError(t, err)
			assert.Equal(t, prepared.complete, true)
			assert.Equal(t, prepared.registered, false)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			switch route {
			case "producer":
				err = d.prepareProducer(ctx, owner, source, nil)
			case "pending producer":
				var registered bool
				registered, err = d.preparePendingProducer(ctx, owner, nil)
				assert.Equal(t, registered, false)
			case "qualification":
				var reads store.ReadSet
				reads.Init(st, owner.Named(), nil)
				defer reads.Retire()
				defer reads.ReleaseCapture()
				q := &entryQualification{loader: l, session: &d}
				walk := qualificationWalk{q: q, store: st, reads: &reads}
				err = walk.prepare(ctx, &applicationEvidence{object: owner}, owner, source)
				assert.Equal(t, q.changed, false)
			}
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			assert.Equal(t, prepared.registered, false)
			assert.Equal(t, d.prepared[owner.Named()], prepared)
			assert.Equal(t, d.binding.Failure, failure)
			d.Close()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root + "-replacement", FS: source.FS},
			)
			assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), false)
		})
	}
}
