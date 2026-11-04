package sourceref_test

import (
	stdcmp "cmp"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedTreeReplacesHeldConsumer(t *testing.T) {
	for _, tt := range []struct {
		name, held, want, heldSource               string
		survivorName, survivorPath, survivorSource string
	}{
		{
			name: "removed",
			held: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: restored, namespace: apps}\ndata: {value: tagged-consumer}\n",
			want: "value: tagged-consumer",
		},
		{
			name: "replaced",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  path: ./ok\n  sourceRef: {kind: GitRepository, name: flux-system}\n"),
			want: "value: ok-consumer",
		},
		{
			name: "different_name", heldSource: "unknown",
			survivorName: "b-survivor", survivorPath: "./apps/broken", survivorSource: "unknown",
			held: testutil.KustomizationYAML("b-survivor", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: unknown}\n"),
			want: "value: tagged",
		},
		{
			name: "different_path", heldSource: "unknown",
			survivorName: "a-held", survivorPath: "./ok", survivorSource: "unknown",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./ok\n  sourceRef: {kind: GitRepository, name: unknown}\n"),
			want: "value: tagged",
		},
		{
			name: "different_source_ref", heldSource: "unknown",
			survivorName: "a-held", survivorPath: "./apps/broken", survivorSource: "flux-system",
			held: testutil.KustomizationYAML("a-held", "flux-system", "  interval: 10m\n  suspend: true\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: flux-system}\n"),
			want: "value: tagged",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			{
				_, err := repo.CreateRemote(&config.RemoteConfig{
					Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
				})
				assert.FatalIf(t, err != nil, "%v", err)
			}
			for _, ks := range []struct{ name, path, source string }{
				{"a-entry", "apps", "flux-system"},
				{"z-pinned", "apps", "pinned"},
				{"z-stage", "stage", "flux-system"},
			} {
				testutil.WriteFile(
					t,
					root,
					"flux/"+ks.name+".yaml",
					testutil.KustomizationYAML(
						ks.name,
						"flux-system",
						"  interval: 10m\n  path: ./"+ks.path+"\n  sourceRef: {kind: GitRepository, name: "+ks.source+"}\n",
					),
				)
			}
			harness.MutateFile(
				t,
				filepath.Join(root, "flux/a-entry.yaml"),
				"interval: 10m",
				"interval: 10m\n  suspend: true",
			)
			testutil.WriteFile(t, root, "stage/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"stage/ks.yaml",
				testutil.KustomizationYAML(
					"z-sources",
					"flux-system",
					"  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
			testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, broken/held.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: tagged}\n",
			)
			testutil.WriteFile(t, root, "apps/broken/held.yaml", tt.held)
			testutil.WriteFile(t, root, "ok/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"ok/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ok, namespace: apps}\ndata: {value: ok-consumer}\n",
			)
			testutil.WriteFile(t, root, "apps/broken/kustomization.yaml", "resources: [held.yaml, extra.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/broken/extra.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: apps}\n",
			)
			testutil.GitCommitAll(
				t,
				repo,
				"init",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			head, err := repo.Head()
			assert.NoError(t, err)
			assert.NoError(t, repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash()),
			))
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: tagged", "value: newer")
			testutil.GitCommitAll(
				t,
				repo,
				"init",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			heldSource := stdcmp.Or(tt.heldSource, "pinned")
			testutil.WriteFile(
				t,
				root,
				"apps/broken/held.yaml",
				testutil.KustomizationYAML(
					"a-held",
					"flux-system",
					"  interval: 10m\n  path: ./apps/broken\n  sourceRef: {kind: GitRepository, name: "+heldSource+"}\n",
				),
			)
			testutil.WriteFile(t, root, "apps/broken/extra.yaml", "metadata: {name: extra\n")
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: newer", "value: dirty")
			l := loader.New(store.New())
			l.SourceRoot = root
			{
				_, err := l.Load(t.Context(), filepath.Join(root, "apps/broken"))
				assert.FatalIf(t, err != nil, "%v", err)
			}
			l.Admit(root)
			heldError := l.EntryFailure(t.Context(), root)
			assert.FatalIf(
				t,
				!errors.Is(heldError, manifest.ErrInput),
				"expected held consumer's decode error, got %v",
				heldError,
			)
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"2", "8"} {
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", concurrency, "--cache-dir", t.TempDir())
				if code != 0 || strings.Contains(stderr, heldError.Error()) ||
					!strings.Contains(out, "value: tagged") || !strings.Contains(out, tt.want) ||
					strings.Contains(
						out,
						"value: dirty",
					) || strings.Contains(out, "value: newer") || transport.calls.Load() != 0 {
					t.Fatalf("discarded held consumer: concurrency=%s exit=%d transport=%d\n%s\nstderr:\n%s",
						concurrency, code, transport.calls.Load(), out, stderr)
				}
			}
			if tt.survivorName != "" {
				st := store.New()
				_, err := discovery.Run(t.Context(), discovery.Config{
					Path: filepath.Join(root, "flux"), Store: st,
				})
				assert.FatalIf(t, err != nil, "incidental working-tree diagnostic failed discovery: %v", err)
				id := testutil.KustomizationID("flux-system", tt.survivorName)
				ks, ok := st.Get[*manifest.Kustomization](id)
				assert.FatalIf(t, !ok || ks.Path != tt.survivorPath || ks.SourceRef.Name != tt.survivorSource ||
					ks.SourceRef.Namespace != "", "expected survivor with independently changed identity field, got %+v", ks)
				if tt.survivorName != "a-held" {
					id.Name = "a-held"
					{
						_, ok := st.Get[*manifest.Kustomization](id)
						assert.FatalIf(t, ok, "%v", "held consumer survived replacement")
					}
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedFollowedSourceInheritsNamespace(t *testing.T) {
	for _, tt := range []struct{ name, namespace, sourceDir string }{
		{name: "explicit", namespace: ", namespace: flux-system", sourceDir: "clusters"},
		{name: "omitted", sourceDir: "clusters"},
		{name: "second_explicit", namespace: ", namespace: flux-system", sourceDir: "sub"},
		{name: "second_omitted", sourceDir: "sub"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			{
				_, err := repo.CreateRemote(&config.RemoteConfig{
					Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
				})
				assert.FatalIf(t, err != nil, "%v", err)
			}
			testutil.WriteFile(
				t,
				root,
				"flux/ks.yaml",
				testutil.KustomizationYAML(
					"meta",
					"flux-system",
					"  interval: 10m\n  path: ./clusters\n  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n",
				),
			)
			if tt.sourceDir == "sub" {
				testutil.WriteFile(
					t,
					root,
					"clusters/kustomization.yaml",
					"namespace: flux-system\nresources: [ks2.yaml]\n",
				)
				testutil.WriteFile(
					t,
					root,
					"clusters/ks2.yaml",
					testutil.KustomizationYAML(
						"sub",
						"",
						"  interval: 10m\n  path: ./sub\n  sourceRef: {kind: GitRepository, name: bootstrap, namespace: flux-system}\n",
					),
				)
			}
			testutil.WriteFile(
				t,
				root,
				tt.sourceDir+"/kustomization.yaml",
				"namespace: flux-system\nresources: [repo.yaml, apps.yaml]\n",
			)
			testutil.WriteFile(t, root, tt.sourceDir+"/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
`)
			testutil.WriteFile(
				t,
				root,
				tt.sourceDir+"/apps.yaml",
				testutil.KustomizationYAML(
					"apps",
					"",
					"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster"+tt.namespace+"}\n",
				),
			)
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			for _, version := range []string{"v1.0.0", "v2.0.0"} {
				testutil.WriteFile(
					t,
					root,
					"apps/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata:\n  value: "+version+"\n",
				)
				testutil.GitCommitAll(
					t,
					repo,
					"init",
					&gogit.CommitOptions{
						Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
					},
				)
				head, err := repo.Head()
				assert.NoError(t, err)
				assert.NoError(t, repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewTagReferenceName(version), head.Hash()),
				))
			}
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			transport := installSourceRefTransport(t, repo.Storer)
			for _, concurrency := range []string{"1", "2", "8"} {
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"),
					"--concurrency", concurrency, "--cache-dir", t.TempDir())
				if code != 0 || !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: dirty") ||
					transport.calls.Load() != 0 {
					t.Fatalf(
						"followed source pin: concurrency=%s exit=%d transport=%d\n%s\nstderr:\n%s",
						concurrency,
						code,
						transport.calls.Load(),
						out,
						stderr,
					)
				}
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedRootPreservesSource(t *testing.T) {
	root, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	for _, hash := range []plumbing.Hash{older, head} {
		assert.NoError(t, wt.Checkout(&gogit.CheckoutOptions{Hash: hash}))
		harness.MutateFile(t, filepath.Join(root, "flux/entry.yaml"), "path: ./apps", "path: ./")
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [apps, flux/entry.yaml]\n")
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		if hash == older {
			ref, err := repo.Head()
			assert.NoError(t, err)
			assert.NoError(t, repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash()),
			))
		}
	}
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	transport := installSourceRefTransport(t, repo.Storer)
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, scope),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			assert.FatalIf(t, !strings.Contains(out, "value: v1.0.0") || strings.Contains(out, "value: dirty") ||
				strings.Contains(out, "value: v2.0.0"), "pinned root rendered the working tree:\n%s", out)
			if transport.calls.Load() != 0 {
				t.Fatalf("local pin accessed transport %d times", transport.calls.Load())
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedPathIgnoresAddedDiscoveryObjects(t *testing.T) {
	original, _, _, _ := sourceRefFixture(t)
	current, _, _, _ := sourceRefFixture(t)
	for _, root := range []string{original, current} {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/entry.yaml, apps]\n")
	}
	testutil.WriteFile(
		t,
		current,
		"apps/added-ks.yaml",
		testutil.KustomizationYAML("added-ks", "flux-system", `  path: ./apps/phantom
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
`)+"---\n"+
			`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec: {url: 'git://fixture.invalid/cluster'}
`,
	)
	testutil.WriteFile(t, current, "apps/added-rs.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: added-rs, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: added-rs-output, namespace: apps}
    data: {value: phantom}
`)
	testutil.WriteFile(t, current, "apps/phantom/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		current,
		"apps/phantom/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added-ks-output, namespace: apps}\ndata: {value: phantom}\n",
	)
	harness.MutateFile(
		t,
		filepath.Join(current, "apps/kustomization.yaml"),
		"- cm.yaml\n",
		"- cm.yaml\n- added-ks.yaml\n- added-rs.yaml\n",
	)
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			e2ESourceRefPinnedPathIgnoresAddedDiscoveryObjectsScope(t, &current, &scope, &original)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedPathRendersDeletedKustomization(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	testutil.WriteFile(
		t,
		root,
		"apps/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"flux-system",
			"  path: ./leaf\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
		),
	)
	harness.MutateFile(t, filepath.Join(root, "apps/kustomization.yaml"), "- cm.yaml\n", "- cm.yaml\n- child.yaml\n")
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"leaf/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\ndata: {value: pinned-child}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash()),
	))
	assert.NoError(t, os.Remove(filepath.Join(root, "apps/child.yaml")))
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	assert.FatalIf(
		t,
		!strings.Contains(out, "name: child-output") || !strings.Contains(out, "value: pinned-child"),
		"deleted committed child did not render:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PinnedPathOwnsWorkingTreeReleases(t *testing.T) {
	root, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	for _, hash := range []plumbing.Hash{older, head} {
		assert.NoError(t, wt.Checkout(&gogit.CheckoutOptions{Hash: hash}))
		testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [entry.yaml]\n")
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources:\n- cm.yaml\n- release.yaml\n")
		testutil.WriteFile(t, root, "apps/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: existing, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
  values: {greeting: pinned}
`)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
  namespace: {{ .Release.Namespace }}
data:
  greeting: {{ .Values.greeting | quote }}
`)
		if hash == older {
			testutil.GitCommitAll(
				t,
				repo,
				"init",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			ref, headErr := repo.Head()
			assert.NoError(t, headErr)
			assert.NoError(t, repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash()),
			))
		}
	}
	loader := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(root, "apps/release.yaml"), "greeting: pinned", "greeting: dirty")
	release, err := os.ReadFile(filepath.Join(root, "apps/release.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "apps/added.yaml", strings.Replace(string(release), "name: existing", "name: added", 1))
	harness.MutateFile(
		t,
		filepath.Join(root, "apps/kustomization.yaml"),
		"- release.yaml\n",
		"- release.yaml\n- added.yaml\n",
	)
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "json")
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	var docs []struct {
		APIVersion string `json:"APIVersion"`
		Kind       string `json:"Kind"`
		Metadata   struct {
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
		} `json:"Metadata"`
		Data map[string]string `json:"Data"`
		Spec struct {
			Values map[string]string `json:"Values"`
		} `json:"Spec"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &docs))
	ids := make(map[manifest.NamedResource]int)
	for _, doc := range docs {
		group, _, hasGroup := strings.Cut(doc.APIVersion, "/")
		if !hasGroup {
			group = ""
		}
		ids[manifest.NamedResource{Group: group, Kind: doc.Kind, Namespace: doc.Metadata.Namespace, Name: doc.Metadata.Name}]++
		assert.FatalIf(t, doc.Kind == manifest.KindHelmRelease && doc.Spec.Values["greeting"] != "pinned" ||
			doc.Kind == manifest.KindConfigMap && doc.Metadata.Name == "existing-cm" &&
				doc.Data["greeting"] != "pinned" ||
			doc.Metadata.Name == "hello" && doc.Data["value"] != "v1.0.0", "expected pinned content:\n%s", out)
	}
	want := map[manifest.NamedResource]int{
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hello"}:                                                 1,
		testutil.KustomizationID("flux-system", "apps"):                                                                  1,
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}: 1,
		testutil.HelmReleaseID("apps", "existing"):                                                                       1,
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "existing-cm"}:                                           1,
	}
	{
		diff := cmp.Diff(want, ids)
		assert.FatalIf(t, diff != "", "pinned object ids (-want +got):\n%s\n%s", diff, out)
	}
}

func e2ESourceRefPinnedPathIgnoresAddedDiscoveryObjectsScope(
	t *testing.T,
	current *string,
	scope *string,
	original *string,
) {
	t.Helper()

	path := filepath.Join((*current), (*scope))
	out, _ := harness.RequireCLIOK(t, "build", "all", "--path", path,
		"--concurrency", "2", "--cache-dir", t.TempDir())
	docs, err := testutil.DecodeDocs(strings.NewReader(out))
	assert.NoError(t, err)
	ids := map[string]int{}
	pinned := 0
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		ids[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
		if manifest.DocKind(doc) == manifest.KindConfigMap && name == "hello" {
			metadata := doc["metadata"].(map[string]any)
			labels := metadata["labels"].(map[string]any)
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/name"], "apps")
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			assert.Equal(t, doc["data"].(map[string]any)["value"], "v1.0.0")
			pinned++
		}
	}
	assert.Equal(t, pinned, 1)
	if (*scope) == "flux" {
		assert.FatalIf(
			t,
			strings.Contains(out, "phantom") || strings.Contains(out, "added-"),
			"working-tree discovery objects escaped the pin:\n%s",
			out,
		)
	} else {
		assert.Diff(t, ids, map[string]int{
			"ConfigMap/apps/hello":               1,
			"ConfigMap/apps/added-ks-output":     1,
			"ConfigMap/apps/added-rs-output":     1,
			"GitRepository/flux-system/cluster":  1,
			"GitRepository/flux-system/working":  1,
			"Kustomization/flux-system/apps":     1,
			"Kustomization/flux-system/added-ks": 1,
			"ResourceSet/flux-system/added-rs":   1,
		})
	}
	for direction, paths := range [][2]string{{(*current), (*original)}, {(*original), (*current)}} {
		args := []string{
			"diff", "all", "--path", filepath.Join(paths[0], (*scope)),
			"--path-orig", filepath.Join(paths[1], (*scope)), "--concurrency", "2", "--cache-dir", t.TempDir(),
		}
		if (*scope) == "flux" {
			out, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
			assert.FatalIf(t, out != "", "working-tree discovery objects changed the pinned diff:\n%s", out)
			continue
		}
		product, _ := harness.RequireCLIOK(t, append(args, "-o", "json")...)
		var result struct {
			Changes []struct {
				Kind      string `json:"Kind"`
				Namespace string `json:"Namespace"`
				Name      string `json:"Name"`
				Status    string `json:"Status"`
				Parent    struct {
					Kind      string `json:"Kind"`
					Namespace string `json:"Namespace"`
					Name      string `json:"Name"`
					Path      string `json:"Path"`
				} `json:"Parent"`
			} `json:"Changes"`
			Suppressed []any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(product), &result))
		assert.Equal(t, len(result.Suppressed), 0)
		changed := map[string]int{}
		for changeIndex := range result.Changes {
			change := result.Changes[changeIndex]
			id := change.Kind + "/" + change.Namespace + "/" + change.Name
			changed[id]++
			assert.Equal(t, change.Status, []string{"added", "removed"}[direction])
			wantKind, wantNamespace, wantName, wantPath := "", "", "", "."
			if change.Kind == "ConfigMap" && change.Name == "added-ks-output" {
				wantKind, wantNamespace, wantName, wantPath = "Kustomization", "flux-system", "added-ks", ""
			}
			assert.Equal(t, change.Parent.Kind, wantKind)
			assert.Equal(t, change.Parent.Namespace, wantNamespace)
			assert.Equal(t, change.Parent.Name, wantName)
			if change.Kind != "ConfigMap" {
				assert.Equal(t, change.Parent.Path, wantPath)
			}
		}
		assert.Diff(t, changed, map[string]int{
			"ConfigMap/apps/added-ks-output":     1,
			"ConfigMap/apps/added-rs-output":     1,
			"GitRepository/flux-system/working":  1,
			"Kustomization/flux-system/added-ks": 1,
			"ResourceSet/flux-system/added-rs":   1,
		})
	}
}
