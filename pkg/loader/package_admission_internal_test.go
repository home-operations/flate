package loader

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestPackages_RootAdmission(t *testing.T) {
	t.Parallel()
	source := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: oci://fixture.invalid/cluster}\n"
	configMap := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values}\n"
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  map[string]bool
	}{
		{
			name:  "plain runnable roots",
			files: map[string]string{"one/source.yaml": source, "two/source.yaml": source, "data/cm.yaml": configMap},
			want:  map[string]bool{"one": true, "two": true, "data": false},
		},
		{
			name:  "native operands",
			files: map[string]string{"one/kustomization.yaml": "resources: [../operand]\n", "operand/kustomization.yaml": "resources: [source.yaml]\n", "operand/source.yaml": source},
			want:  map[string]bool{"one": true, "operand": false},
		},
		{
			name: "components and generators",
			files: map[string]string{
				"one/kustomization.yaml":       "components: [../component]\nconfigMapGenerator: [{name: values, files: [../generated/source.yaml]}]\n",
				"component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [source.yaml]\n",
				"component/source.yaml":        source,
				"generated/source.yaml":        source,
			},
			want: map[string]bool{"one": true, "generated": false},
		},
		{
			name: "explicit composition",
			files: map[string]string{
				"kustomization.yaml":     "resources: [one, two]\n",
				"one/kustomization.yaml": "resources: [source.yaml]\n", "one/source.yaml": source,
				"two/kustomization.yaml": "resources: [source.yaml]\n", "two/source.yaml": source,
			},
			want: map[string]bool{".": true, "one": false, "two": false},
		},
		{
			name: "application ownership",
			files: map[string]string{
				"one/owner.yaml":  testutil.KustomizationYAML("one", "apps", "  path: ./two\n"),
				"two/source.yaml": source,
			},
			want: map[string]bool{"one": true, "two": false},
		},
		{
			name: "mutual application ownership",
			files: map[string]string{
				"one/owner.yaml": testutil.KustomizationYAML("one", "apps", "  path: ./two\n"),
				"two/owner.yaml": testutil.KustomizationYAML("two", "apps", "  path: ./one\n"),
			},
			want: map[string]bool{"one": true, "two": false},
		},
		{
			name: "self application anchors",
			files: map[string]string{
				"one/owner.yaml": testutil.KustomizationYAML("same", "apps", "  path: ./one\n  sourceRef: {kind: GitRepository, name: local}\n"),
				"two/owner.yaml": testutil.KustomizationYAML("same", "apps", "  path: ./two\n  sourceRef: {kind: GitRepository, name: local}\n"),
			},
			want: map[string]bool{"one": true, "two": true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, compact := range []bool{false, true} {
				t.Run(strconv.FormatBool(compact), func(t *testing.T) {
					t.Parallel()
					assertPackageAdmissionForTest(t, tc.files, tc.want, compact)
				})
			}
		})
	}
}

func assertPackageAdmissionForTest(t *testing.T, contents map[string]string, want map[string]bool, compact bool) {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{}
	for name, body := range contents {
		files[name] = &fstest.MapFile{Data: []byte(body)}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: compact})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	for range 2 {
		packages, err := l.Packages(t.Context())
		assert.NoError(t, err)
		got := map[string]bool{}
		for _, p := range packages {
			got[p.Path] = p.Independent
			for i := range p.Occurrences {
				occurrence := &p.Occurrences[i]
				coordinate := rootCoordinate{
					declaration: physicalOccurrence(
						occurrence,
					).Declaration,
					identity: occurrence.Identity,
					context:  p.Path,
				}
				assert.Equal(
					t,
					l.roots.occurrences[coordinate].occurrence.Declaration,
					physicalOccurrence(occurrence).Declaration,
				)
			}
		}
		assert.Diff(t, got, want)
	}
}

func TestPackages_UnboundSourceScope(t *testing.T) {
	t.Parallel()
	for _, pinnedDirectory := range []string{"one", "two"} {
		t.Run(pinnedDirectory, func(t *testing.T) {
			t.Parallel()
			for _, compact := range []bool{false, true} {
				t.Run(strconv.FormatBool(compact), func(t *testing.T) {
					t.Parallel()
					files := map[string]string{
						"one/owner.yaml": testutil.KustomizationYAML(
							"owner", "apps", "  path: ./target\n  sourceRef: {kind: GitRepository, name: cluster}\n",
						),
						"one/source.yaml":    "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/one'}\n",
						"two/source.yaml":    "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/two'}\n",
						"target/source.yaml": "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: target, namespace: apps}\nspec: {url: oci://fixture.invalid/target}\n",
					}
					name := pinnedDirectory + "/source.yaml"
					files[name] = strings.Replace(
						files[name],
						"spec: {url:",
						"spec: {ref: {commit: '"+strings.Repeat("c", 40)+"'}, url:",
						1,
					)
					assertPackageAdmissionForTest(
						t,
						files,
						map[string]bool{"one": true, "two": true, "target": true},
						compact,
					)
				})
			}
		})
	}
}

func TestPackages_AdmissionRetiresWorkingTreeEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		native, compact bool
	}{
		{"full native", true, false},
		{"compact native", true, true},
		{"full plain", false, false},
		{"compact plain", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			source := &manifest.GitRepository{Name: "cluster", Namespace: "control"}
			st.AddObject(source)
			files := fstest.MapFS{
				"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("root", "control",
					"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster}\n"))},
				"apps/phantom.yaml": {Data: []byte(testutil.KustomizationYAML("phantom", "control",
					"  path: ./phantom\n"))},
			}
			if tc.native {
				files["apps/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [phantom.yaml]\n")}
			}
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: tc.compact})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			packages, err := l.Packages(t.Context())
			assert.NoError(t, err)
			for _, pkg := range packages {
				if pkg.Path == "apps" {
					assert.Equal(t, pkg.Independent, true)
				}
			}
			pin := filepath.Join(root, ".pin")
			st.SetArtifact(source.Named(), &store.SourceArtifact{
				LocalPath: pin, LocalRoot: root, Content: "committed", FS: fstest.MapFS{
					"apps/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
					"apps/cm.yaml": {
						Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: pinned}\n"),
					},
				},
			})
			_, err = l.Load(t.Context(), filepath.Join(pin, "apps"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			l.Admit(root)
			packages, err = l.Packages(t.Context())
			assert.NoError(t, err)
			for _, pkg := range packages {
				if pkg.Path == "apps" {
					assert.Equal(t, pkg.Independent, false)
				}
			}
		})
	}
}

func TestOccurrenceObject_SourceDeclarations(t *testing.T) {
	t.Parallel()
	for _, compact := range []bool{false, true} {
		t.Run(strconv.FormatBool(compact), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := sourceDeclarationsForTest(compact)
			input := &store.SourceArtifact{LocalPath: root, FS: files}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: compact})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assertSourceDeclarationsForTest(t, l, input, compact)
		})
	}
}

func sourceDeclarationsForTest(compact bool) fstest.MapFS {
	files := fstest.MapFS{}
	for _, dir := range []string{"one", "two"} {
		first := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/" + dir + "', ref: {commit: '" + strings.Repeat(
			"a",
			40,
		) + "'}}\n"
		second := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/" + dir + "-second', ref: {commit: '" + strings.Repeat(
			"b",
			40,
		) + "'}}\n"
		body := "{}\n---\n" + first + "---\n" + second
		if compact {
			body = "{}\n---\napiVersion: v1\nkind: List\nitems:\n- " + strings.ReplaceAll(
				strings.TrimSuffix(first, "\n"),
				"\n",
				"\n  ",
			) + "\n- " + strings.ReplaceAll(
				strings.TrimSuffix(second, "\n"),
				"\n",
				"\n  ",
			) + "\n"
		}
		files[dir+"/source.yaml"] = &fstest.MapFile{Data: []byte(body)}
	}
	return files
}

func assertSourceDeclarationsForTest(t *testing.T, l *Loader, input *store.SourceArtifact, compact bool) {
	t.Helper()
	for _, dir := range []string{"one", "two"} {
		occurrences, err := l.Occurrences(t.Context(), filepath.Join(input.LocalPath, dir), "")
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			len(occurrences) != 2,
			"source occurrences: %+v; document: %+v",
			occurrences,
			l.documents.cache[filepath.Join(input.LocalPath, dir, "source.yaml")],
		)
		for _, index := range []int{1, 0} {
			occurrence := &occurrences[index]
			coordinate := selection.OccurrenceID{
				Source:   input.LocalPath,
				File:     dir + "/source.yaml",
				Document: index + 1,
			}
			if compact {
				coordinate.Document, coordinate.Item = 1, index
			}
			assert.Equal(t, occurrence.Declaration, coordinate)
			child, err := l.OccurrenceObject(t.Context(), occurrence, input)
			assert.NoError(t, err)
			assert.FatalIf(t, child.Obj == nil, "missing source occurrence: %+v", occurrence)
			git := child.Obj.(*manifest.GitRepository)
			suffix := ""
			if index == 1 {
				suffix = "-second"
			}
			assert.Equal(t, git.URL, "https://fixture.invalid/"+dir+suffix)
			commit := strings.Repeat("a", 40)
			if index == 1 {
				commit = strings.Repeat("b", 40)
			}
			assert.Equal(t, git.Reference.Commit, commit)
			if compact {
				assert.Equal(t, occurrence.Object, manifest.BaseManifest(nil))
			}
		}
	}
}

func TestOccurrenceObject_SourceReplacement(t *testing.T) {
	t.Parallel()
	for _, compact := range []bool{false, true} {
		t.Run(strconv.FormatBool(compact), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := func(side string) *store.SourceArtifact {
				return &store.SourceArtifact{LocalPath: root, Content: side, FS: fstest.MapFS{
					"source.yaml": {
						Data: []byte(
							"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/" + side + "'}\n",
						),
					},
				}}
			}
			first, second := input("first"), input("second")
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, first)
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: compact})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			packages, err := l.Packages(t.Context())
			assert.NoError(t, err)
			assert.FatalIf(t, len(packages) != 1 || len(packages[0].Occurrences) != 1, "packages=%+v", packages)
			occurrence := packages[0].Occurrences[0]
			child, err := l.OccurrenceObject(t.Context(), &occurrence, first)
			assert.NoError(t, err)
			child.Doc["metadata"].(map[string]any)["name"] = "caller-owned"
			child, err = l.OccurrenceObject(t.Context(), &occurrence, first)
			assert.NoError(t, err)
			assert.Equal(t, child.Doc["metadata"].(map[string]any)["name"], any("cluster"))
			prepared := l.ParsedInputs()
			other := New(st)
			other.SourceRoot, other.Options.ParsedInputs = root, prepared
			configureSelection(t, other, selection.Request{Changed: compact})
			_, err = other.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			child, err = other.OccurrenceObject(t.Context(), &occurrence, first)
			assert.NoError(t, err)
			assert.Equal(t, child.Obj.(*manifest.GitRepository).URL, "https://fixture.invalid/first")
			st.SetArtifact(manifest.BootstrapSourceID, second)
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.Equal(t, len(l.roots.identities), 0)
			for _, evidence := range l.roots.occurrences {
				assert.Equal(t, selection.Root(evidence.facts).Independent, false)
			}
			child, err = l.OccurrenceObject(t.Context(), &occurrence, second)
			assert.NoError(t, err)
			assert.Equal(t, child.Obj.(*manifest.GitRepository).URL, "https://fixture.invalid/second")
			child, err = other.OccurrenceObject(t.Context(), &occurrence, second)
			assert.NoError(t, err)
			assert.Equal(t, child.Obj.(*manifest.GitRepository).URL, "https://fixture.invalid/second")
		})
	}
}

func TestEntryRoots_DeclarationCoordinates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	admitForTest(st, root)
	files := fstest.MapFS{}
	for _, path := range []string{"one", "two"} {
		files[path+"/producer.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: same, namespace: apps}\nspec: {resources: []}\n",
			),
		}
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.Packages(t.Context())
	assert.NoError(t, err)
	l.Admit(root)
	roots := l.EntryRoots(nil, nil)
	assert.FatalIf(t, len(roots) != 2, "entry roots=%+v", roots)
	assert.Equal(t, roots[0].Identity, roots[1].Identity)
	assert.Equal(t, roots[0].Declaration, selection.OccurrenceID{Source: root, File: "one/producer.yaml"})
	assert.Equal(t, roots[1].Declaration, selection.OccurrenceID{Source: root, File: "two/producer.yaml"})
}
