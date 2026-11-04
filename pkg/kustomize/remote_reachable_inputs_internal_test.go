package kustomize

import (
	"context"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPreflightRemoteResources_ReachableInputs(t *testing.T) {
	t.Parallel()
	const base = "resources: [cm.yaml]\n"
	const patch = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {marker: patched}\n"
	const target = "target: {kind: ConfigMap, name: demo}\n"
	const replacement = "source: {kind: ConfigMap, name: demo, fieldPath: metadata.name}\ntargets:\n- select: {kind: ConfigMap, name: demo}\n  fieldPaths: [data.marker]\n"
	const plugin = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [local.txt, $URL/file.txt]\n"
	cases := []struct {
		name, control, subpath, want, wantErr string
		files, replies, gitFiles              map[string]string
	}{
		{
			name:    "resource",
			control: "resources: [$URL/resource.yaml]\n",
			replies: map[string]string{"/resource.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "base",
			control: "bases: [$URL/resource.yaml]\n",
			replies: map[string]string{"/resource.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "patch",
			control: base + "patches: [{path: $URL/patch.yaml}]\n",
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "json-patch",
			control: base + "patchesJson6902:\n- " + target + "  path: $URL/patch.json\n",
			replies: map[string]string{"/patch.json": `[{"op":"replace","path":"/data/marker","value":"patched"}]`},
			want:    "marker: patched",
		},
		{
			name:    "strategic-patch",
			control: base + "patchesStrategicMerge: [$URL/patch.yaml]\n",
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "replacement",
			control: base + "replacements: [{path: $URL/replacement.yaml}]\n",
			replies: map[string]string{"/replacement.yaml": replacement},
			want:    "marker: demo",
		},
		{
			name:    "configuration",
			control: base + "configurations: [$URL/config.yaml]\n",
			replies: map[string]string{"/config.yaml": "nameReference: []\n"},
			want:    "name: demo",
		},
		{
			name:    "crd",
			control: base + "crds: [$URL/crd.json]\n",
			replies: map[string]string{"/crd.json": "{}"},
			want:    "name: demo",
		},
		{
			name:    "openapi",
			control: base + "openapi: {path: $URL/schema.json}\n",
			replies: map[string]string{"/schema.json": `{"swagger":"2.0","definitions":{}}`},
			want:    "name: demo",
		},
		{
			name:    "inferred-key",
			control: base + "configMapGenerator: [{name: generated, files: [$URL/file.txt]}]\n",
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
		{
			name:    "explicit-key",
			control: base + "configMapGenerator: [{name: generated, files: [chosen=$URL/file.txt]}]\n",
			replies: map[string]string{"/file.txt": "contents"},
			want:    "chosen: contents",
		},
		{
			name:    "invalid-key-grammar",
			control: base + "configMapGenerator: [{name: generated, files: ['key=$URL/file.txt=extra']}]\n",
			wantErr: "key name or file path contains '='",
		},
		{
			name:    "invalid-empty-key",
			control: base + "configMapGenerator: [{name: generated, files: ['=$URL/file.txt']}]\n",
			wantErr: "missing key name",
		},
		{
			name:    "configmap-envs",
			control: base + "configMapGenerator: [{name: generated, envs: [$URL/env]}]\n",
			replies: map[string]string{"/env": "marker=env-value\n"},
			want:    "marker: env-value",
		},
		{
			name:    "configmap-env",
			control: base + "configMapGenerator: [{name: generated, env: $URL/env}]\n",
			replies: map[string]string{"/env": "marker=env-value\n"},
			want:    "marker: env-value",
		},
		{
			name:    "secret-files",
			control: base + "secretGenerator: [{name: generated, files: [$URL/file.txt]}]\n",
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: Y29udGVudHM=",
		},
		{
			name:    "secret-envs",
			control: base + "secretGenerator: [{name: generated, envs: [$URL/env]}]\n",
			replies: map[string]string{"/env": "marker=secret\n"},
			want:    "marker: c2VjcmV0",
		},
		{
			name:    "secret-env",
			control: base + "secretGenerator: [{name: generated, env: $URL/env}]\n",
			replies: map[string]string{"/env": "marker=secret\n"},
			want:    "marker: c2VjcmV0",
		},
		{
			name:    "case-shadowed-resource",
			control: "resources: [child]\n",
			files: map[string]string{
				"child/kustomization.yaml": "Resources: [../cm.yaml]\nresources: [$URL/resource.yaml]\n",
			},
			replies: map[string]string{"/resource.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "case-shadowed-data",
			control: "resources: [child]\n",
			files:   map[string]string{"child/kustomization.yaml": "resources: [../cm.yaml]\nResources: [$URL/data]\n"},
			want:    "name: demo",
		},
		{
			name:    "case-insensitive-control",
			control: "resources: [child]\n",
			files:   map[string]string{"child/kustomization.yaml": "Resources: [$URL/resource.yaml]\n"},
			replies: map[string]string{"/resource.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "local-parent-alternate-control",
			control: "resources: [child]\n",
			files:   map[string]string{"child/Kustomization": "resources: [$URL/resource.yaml]\n"},
			replies: map[string]string{"/resource.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "outside-component",
			subpath: "selected",
			control: "resources: [cm.yaml]\ncomponents: [../component]\n",
			files: map[string]string{
				"component/kustomization.yml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches: [{path: $URL/patch.yaml}]\n",
			},
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "generated-component",
			subpath: "selected",
			control: base,
			files: map[string]string{
				"component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches: [{path: $URL/patch.yaml}]\n",
			},
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "coalesced-resource-patch",
			control: "resources: [$URL/shared.yaml]\npatches: [{path: $URL/shared.yaml}]\n",
			replies: map[string]string{"/shared.yaml": cmYAML("remote")},
			want:    "name: remote",
		},
		{
			name:    "fetched-git-input",
			control: "resources: ['file:///example.invalid/o/r.git//selected?ref=main']\n",
			gitFiles: map[string]string{
				"selected/kustomization.yaml":  base + "components: [../component]\n",
				"selected/cm.yaml":             cmYAML("demo"),
				"component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nconfigMapGenerator: [{name: generated, files: [$URL/file.txt]}]\n",
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
		{
			name:    "plugin-git-directory",
			control: base + "generators: ['file:///example.invalid/o/r.git//plugins?ref=main']\n",
			gitFiles: map[string]string{
				"plugins/kustomization.yaml": "resources: [g.yaml]\n",
				"plugins/g.yaml":             plugin,
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "local.txt: owner",
		},
		{
			name:    "plugin-file",
			control: base + "generators: [conf/g.yaml]\n",
			files:   map[string]string{"conf/g.yaml": plugin},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
		{
			name: "plugin-inline-multidoc",
			control: base + "generators:\n- |\n  " + strings.ReplaceAll(
				plugin+"---\napiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: other}\nenvs: [$URL/env]\n",
				"\n",
				"\n  ",
			),
			replies: map[string]string{"/file.txt": "contents", "/env": "marker=secret\n"},
			want:    "marker: c2VjcmV0",
		},
		{
			name:    "git-looking-file",
			control: base + "patches: [{path: $URL/repo.git//patch.yaml}]\n",
			replies: map[string]string{"/repo.git//patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "plugin-inline",
			control: base + "generators:\n- |\n" + "  " + strings.ReplaceAll(plugin, "\n", "\n  "),
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
		{
			name:    "plugin-directory-owner",
			control: base + "generators: [plugins]\n",
			files: map[string]string{
				"plugins/kustomization.yaml": "resources: [conf/g.yaml]\n",
				"plugins/conf/g.yaml":        plugin,
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "local.txt: owner",
		},
		{
			name:    "plugin-multidoc",
			control: base + "generators: [$URL/g.yaml]\n",
			replies: map[string]string{
				"/g.yaml":   plugin + "---\napiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: other}\nenvs: [$URL/env]\n",
				"/file.txt": "contents",
				"/env":      "marker=secret\n",
			},
			want: "marker: c2VjcmV0",
		},
		{
			name:    "plugin-secret-files",
			control: base + "generators: [g.yaml]\n",
			files: map[string]string{
				"g.yaml": "apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: generated}\nfiles: [$URL/file.txt]\n",
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: Y29udGVudHM=",
		},
		{
			name:    "plugin-case-insensitive-files",
			control: base + "generators: [g.yaml]\n",
			files:   map[string]string{"g.yaml": strings.ReplaceAll(plugin, "files:", "Files:")},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
		{
			name:    "plugin-local-directory",
			control: base + "generators: [plugins]\n",
			files: map[string]string{
				"plugins/kustomization.yaml": "resources: [g.yaml]\n",
				"plugins/g.yaml":             strings.ReplaceAll(plugin, ", $URL/file.txt", ""),
			},
			want: "local.txt: owner",
		},
		{
			name:    "plugin-directory-two-owners",
			control: "resources: [a, b]\n",
			files: map[string]string{
				"a/kustomization.yaml":              "namePrefix: a-\ngenerators: [../plugins]\n",
				"b/kustomization.yaml":              "namePrefix: b-\ngenerators: [../plugins]\n",
				"plugins/kustomization.yaml":        "resources: [nested]\n",
				"plugins/nested/kustomization.yaml": "resources: [../g.yaml]\n",
				"plugins/g.yaml":                    plugin,
				"a/local.txt":                       "owner-a",
				"b/local.txt":                       "owner-b",
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "local.txt: owner-b",
		},
		{
			name:    "plugin-patch",
			control: base + "transformers: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: PatchTransformer\nmetadata: {name: patch}\npath: $URL/patch.yaml\n",
			},
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "plugin-json",
			control: base + "transformers: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: PatchJson6902Transformer\nmetadata: {name: patch}\n" + target + "path: $URL/patch.json\n",
			},
			replies: map[string]string{"/patch.json": `[{"op":"replace","path":"/data/marker","value":"patched"}]`},
			want:    "marker: patched",
		},
		{
			name:    "plugin-strategic",
			control: base + "transformers: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: PatchStrategicMergeTransformer\nmetadata: {name: patch}\npaths: [$URL/patch.yaml]\n",
			},
			replies: map[string]string{"/patch.yaml": patch},
			want:    "marker: patched",
		},
		{
			name:    "plugin-replacement-validator",
			control: base + "validators: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: ReplacementTransformer\nmetadata: {name: replace}\nreplacements: [{path: $URL/replacement.yaml}]\n",
			},
			replies: map[string]string{
				"/replacement.yaml": strings.ReplaceAll(replacement, "metadata.name", "data.marker"),
			},
			want: "marker: initial",
		},
		{
			name:    "plugin-valueadd",
			control: base + "transformers: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: ValueAddTransformer\nmetadata: {name: add}\nvalue: injected\ntargetFilePath: $URL/targets.yaml\n",
			},
			replies: map[string]string{"/targets.yaml": "targets: [{fieldPath: metadata/namespace}]\n"},
			want:    "namespace: injected",
		},
		{
			name:    "directory-resource-and-plugin-role",
			control: "resources: [plugins]\ngenerators: [plugins]\n",
			files:   map[string]string{"plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": plugin},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "files: $URL/file.txt",
		},
		{
			name:    "resource-and-plugin-role",
			control: "resources: [conf/g.yaml]\ngenerators: [conf/g.yaml]\n",
			files:   map[string]string{"conf/g.yaml": plugin},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "files: $URL/file.txt",
		},
		{
			name:    "two-plugin-owners",
			control: "resources: [a, b]\n",
			files: map[string]string{
				"a/kustomization.yaml": "namePrefix: a-\ngenerators: [../g.yaml]\n",
				"b/kustomization.yaml": "namePrefix: b-\ngenerators: [../g.yaml]\n",
				"g.yaml":               plugin,
				"a/local.txt":          "owner-a",
				"b/local.txt":          "owner-b",
			},
			replies: map[string]string{"/file.txt": "contents"},
			want:    "local.txt: owner-b",
		},
		{
			name:    "inline-plugin-strategic-data",
			control: base + "transformers: [p.yaml]\n",
			files: map[string]string{
				"p.yaml": "apiVersion: builtin\nkind: PatchStrategicMergeTransformer\nmetadata: {name: patch}\npaths:\n- |\n  " + strings.ReplaceAll(
					strings.ReplaceAll(patch, "patched", "$URL/data"),
					"\n",
					"\n  ",
				),
			},
			want: "marker: $URL/data",
		},
		{
			name:    "replacement-field-data",
			control: base + "replacements: [{source: {kind: ConfigMap, name: demo, fieldPath: metadata.name}, targets: [{select: {kind: ConfigMap}, fieldPaths: ['data.[$URL/data]'], options: {create: true}}]}]\n",
			want:    "$URL/data: demo",
		},
		{
			name:    "unreachable-sibling",
			control: base,
			files:   map[string]string{"unused/kustomization.yaml": "resources: [$URL/unreachable]\n"},
			want:    "name: demo",
		},
		{
			name:    "literal-and-inline-data",
			control: base + "configMapGenerator: [{name: literal, literals: [website=$URL/data]}]\npatchesStrategicMerge:\n- |\n  apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: demo}\n  data: {marker: $URL/data}\n",
			want:    "website: $URL/data",
		},
		{
			name:    "unknown-plugin-data",
			control: base + "generators: [g.yaml]\n",
			files: map[string]string{
				"g.yaml": "apiVersion: example.org/v1\nkind: ConfigMapGenerator\nmetadata: {name: external}\nfiles: [$URL/data]\n",
			},
			wantErr: "external plugins disabled",
		},
		{
			name:    "malformed-plugin",
			control: base + "generators: [g.yaml]\n",
			files:   map[string]string{"g.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: [\n"},
			wantErr: "kustomize build ",
		},
		{
			name:    "malformed-remote-plugin",
			control: base + "generators: [$URL/g.yaml]\n",
			replies: map[string]string{"/g.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: [\n"},
			wantErr: "preflight:",
		},
		{
			name:    "plugin-directory-cycle",
			control: base + "generators: [plugins]\n",
			files:   map[string]string{"plugins/kustomization.yaml": "resources: [..]\n"},
			wantErr: "cycle",
		},
		{
			name:    "malformed-root-control",
			subpath: "broken",
			control: "resources: [\n",
			wantErr: "flux kustomize generator: error converting YAML to JSON:",
		},
		{
			name:    "malformed-reachable-control",
			control: "resources: [child]\n",
			files:   map[string]string{"child/kustomization.yaml": "resources: [\n"},
			wantErr: "kustomize build ",
		},
		{
			name:    "cycle",
			control: "resources: [child]\n",
			files:   map[string]string{"child/kustomization.yaml": "resources: [..]\n"},
			wantErr: "cycle",
		},
		{
			name:    "fetch-failure",
			control: base + "patches: [{path: $URL/missing}]\n",
			replies: map[string]string{"/missing": "404"},
			wantErr: ".patches[0].path",
		},
		{
			name:    "aliased-operand",
			control: base + "configMapGenerator:\n- name: first\n  files: &files [$URL/file.txt]\n- name: second\n  files: *files\n",
			replies: map[string]string{"/file.txt": "contents"},
			want:    "file.txt: contents",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPreflightRemoteResourcesReachableInputsCase(
				t,
				&tc,
			)
		},
		)
	}
}

func testPreflightRemoteResourcesReachableInputsCase(
	t *testing.T,
	testCaseInput *struct {
		name     string
		control  string
		subpath  string
		want     string
		wantErr  string
		files    map[string]string
		replies  map[string]string
		gitFiles map[string]string
	},
) {
	t.Helper()
	var marked atomic.Int32
	var url string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		marked.Add(1)
		body, ok := testCaseInput.replies[r.URL.Path]
		if !ok || body == "404" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, strings.ReplaceAll(body, "$URL", url))
	}))
	t.Cleanup(srv.Close)
	url = srv.URL
	files := map[string]string{
		filepath.Join(testCaseInput.subpath, "kustomization.yaml"): testCaseInput.control,
		filepath.Join(testCaseInput.subpath, "cm.yaml"):            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {marker: initial}\n",
		filepath.Join(testCaseInput.subpath, "local.txt"):          "owner",
	}
	maps.Copy(files, testCaseInput.files)
	for name, body := range files {
		files[name] = strings.ReplaceAll(body, "$URL", url)
	}
	root := testutil.WriteTree(t, files)
	cache := newTestTreeCache(t, "")
	testPreflightRemoteResourcesReachableInputsCaseLoop(t, struct {
		testCaseInput **struct {
			name     string
			control  string
			subpath  string
			want     string
			wantErr  string
			files    map[string]string
			replies  map[string]string
			gitFiles map[string]string
		}
		url   *string
		cache **TreeCache
	}{testCaseInput: &testCaseInput, url: &url, cache: &cache})

	spec := demoSpec
	if testCaseInput.name == "generated-component" {
		spec = demoSpec.Clone()
		spec.Components = []string{"../component"}
	}
	before := manifest.Fingerprint(spec)
	out, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			cache,
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: testCaseInput.subpath, Spec: spec},
			sourcecache.RetryConfig{},
		),
	)
	if testCaseInput.wantErr != "" {
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), testCaseInput.wantErr),
			"error=%v, want %q",
			err,
			testCaseInput.wantErr,
		)
		assert.FatalIf(
			t,
			testCaseInput.name == "plugin-directory-cycle" && !strings.Contains(err.Error(), remoteResourcePrefix),
			"cycle diagnostic lost private view: %v",
			err,
		)
	} else if err != nil || !hasRenderField(out, strings.SplitN(strings.ReplaceAll(testCaseInput.want, "$URL", url), ": ", 2)[0], strings.SplitN(strings.ReplaceAll(testCaseInput.want, "$URL", url), ": ", 2)[1]) {
		t.Fatalf("render: %s, %v; want %q", out, err, testCaseInput.want)
	}
	assert.FatalIf(
		t,
		strings.Contains(testCaseInput.name, "two-owners") && !hasRenderField(out, "local.txt", "owner-a"),
		"first owner's relative operand was lost: %s",
		out,
	)
	assert.FatalIf(
		t,
		marked.Load() != int32(len(testCaseInput.replies)),
		"requests=%d want=%d",
		marked.Load(),
		len(testCaseInput.replies),
	)
	for path := range testCaseInput.replies {
		_, _ = cache.remoteInputs.Acquire(t.Context(), &source.HTTPRequest{URL: url + path})
	}
	assert.FatalIf(
		t,
		marked.Load() != int32(len(testCaseInput.replies)),
		"preflight bypassed shared acquisition: requests=%d want=%d",
		marked.Load(),
		len(testCaseInput.replies),
	)
	if testCaseInput.name == "git-looking-file" {
		rec := newReadSet(root)
		view := newTreeFS(localSource(t, root).FS, root, rec)
		{
			err := preflightRemoteResources(t.Context(), cache, view, root)
			assert.NoError(t, err)
		}
		snapFrame, err := decodeFrame(testRenderFrame(t, new(rec.snapshot()), testRenderEncoding(t, out)))
		snap := snapFrame.header.Reads
		assert.FatalIf(
			t,
			err != nil || len(snap.Remotes) != 1 || snap.Remotes[0].Input.Operation != remoteHTTP,
			"admitted file operation: %+v, %v",
			snap,
			err,
		)
		replay := newTestTreeCache(t, "")
		replay.SetGitBaseFetcher(
			func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				t.Error("file replay reclassified the admitted HTTP operation")
				return nil, manifest.ErrInput
			},
		)
		{
			valid, err := snap.stillValid(t.Context(), replay, view, false)
			assert.FatalIf(t, err != nil || !valid, "file frame replay: %t, %v", valid, err)
		}
		assert.FatalIf(t, marked.Load() != 2, "HTTP initial/replay: requests=%d", marked.Load())
	}
	assert.FatalIf(t, manifest.Fingerprint(spec) != before, "%s", "stored Flux specification mutated")
	for name, body := range files {
		got, err := os.ReadFile(filepath.Join(root, name))
		assert.FatalIf(t, err != nil || string(got) != body, "source mutation %s: %q, %v", name, got, err)
	}
	{
		matches, _ := filepath.Glob(filepath.Join(root, remoteResourcePrefix+"*"))
		assert.FatalIf(t, len(matches) != 0, "overlay escaped to disk: %v", matches)
	}
}

func testPreflightRemoteResourcesReachableInputsCaseLoop(t *testing.T, loopInputs struct {
	testCaseInput **struct {
		name     string
		control  string
		subpath  string
		want     string
		wantErr  string
		files    map[string]string
		replies  map[string]string
		gitFiles map[string]string
	}
	url   *string
	cache **TreeCache
},
) {
	t.Helper()

	if (*loopInputs.testCaseInput).gitFiles != nil {
		gitFiles := make(map[string]string, len((*loopInputs.testCaseInput).gitFiles))
		for name, body := range (*loopInputs.testCaseInput).gitFiles {
			gitFiles[name] = strings.ReplaceAll(body, "$URL", (*loopInputs.url))
		}
		withFakeGitBase(t, (*loopInputs.cache), fakeWorktree(t, gitFiles), nil)
	}
}
