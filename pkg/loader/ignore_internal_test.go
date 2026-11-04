package loader

import (
	"bufio"
	"errors"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoadIgnore_Patterns(t *testing.T) {
	t.Parallel()
	type match struct {
		rel         string
		isDir, want bool
	}
	for _, tc := range []struct {
		name   string
		text   string
		checks []match
	}{
		{name: "recursive", text: "apps/junk/**\n", checks: []match{
			{"apps/junk/file.yaml", false, true},
			{"apps/junk/sub/file.yaml", false, true},
			{"apps/junk/a/b/c/deep.yaml", false, true},
			{"apps/other/file.yaml", false, false},
			{"other/junk/file.yaml", false, false},
		}},
		{name: "single glob", text: "apps/*.yaml\n", checks: []match{
			{"apps/foo.yaml", false, true},
			{"apps/sub/foo.yaml", false, false},
			{"other/foo.yaml", false, false},
		}},
		{name: "absent file", checks: []match{
			{"anything.yaml", false, false},
		}},
		{name: "comments and blank lines", text: "\n# this is a comment\napps/junk/**\n\n# another comment\n", checks: []match{
			{"apps/junk/x.yaml", false, true},
		}},
		{name: "directories", text: "tmp/\n", checks: []match{
			{"tmp", true, true},
			{"tmp", false, false},
			{"tmp/file.yaml", false, true},
			{"tmp.yaml", false, false},
			{"other", true, false},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			if tc.text != "" {
				testutil.WriteFile(t, root, ".krmignore", tc.text)
			}
			ig, err := new(readerForTest(root)).loadIgnore(root)
			assert.FatalIf(t, err != nil, "loadIgnore: %v", err)
			for _, check := range tc.checks {
				t.Run(check.rel+"/"+strconv.FormatBool(check.isDir), func(t *testing.T) {
					t.Parallel()
					abs := filepath.Join(root, filepath.FromSlash(check.rel))
					{
						got := ig.matches(abs, root, check.isDir)
						assert.ErrorIf(
							t,
							got != check.want,
							"matches(%q, isDir=%t) = %t, want %t",
							check.rel,
							check.isDir,
							got,
							check.want,
						)
					}
				})
			}
		})
	}
}

func TestWalkerIgnoreMatches_MemoMatchesUncached(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		patterns string
		wantSize int
	}{
		{name: "patterns", patterns: "apps/junk/**\ntmp/\n", wantSize: 6},
		{name: "nil matcher"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testWalkerIgnoreMatchesMemoMatchesUncachedCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestLoader_KrmignoreEnvironmentAllowlist(t *testing.T) {
	t.Parallel()
	appID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}
	commonID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "common-settings"}

	writeTree := func(t *testing.T, ignore string) *Loader {
		t.Helper()
		dir := t.TempDir()
		testutil.WriteFile(t, dir, ".krmignore", ignore)
		testutil.WriteFile(t, dir, "common/settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: common-settings, namespace: default}
`)
		for _, env := range []string{"production", "staging"} {
			testutil.WriteFile(t, dir, "environments/"+env+"/app.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: app-config, namespace: default}
data: {env: `+env+`}
`)
		}
		s := store.New()
		admitForTest(s, dir)
		l := New(s)
		{
			_, err := loadForTest(t.Context(), t, l, dir)
			assert.FatalIf(t, err != nil, "Load: %v", err)
		}
		return l
	}

	envOf := func(t *testing.T, l *Loader) string {
		t.Helper()
		{
			promoted, err := Promote(
				t.Context(),
				l.Store,
				appID,
				PromoteOptions{
					Record: productionForTest(new(l.Index.Records[appID])),
					Admit:  func(manifest.BaseManifest) (bool, error) { return true, nil },
				},
			)
			assert.FatalIf(t, err != nil ||
				!promoted, "%s", "app-config was not indexed for promotion")
		}
		obj := l.Store.GetObject(appID)
		assert.FatalIf(t, obj == nil, "%s", "app-config was not loaded at all")
		cm, ok := obj.(*manifest.ConfigMap)
		assert.FatalIf(t, !ok, "app-config is %T, want *manifest.ConfigMap", obj)
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("no filtering: staging shadows production", func(t *testing.T) {
		t.Parallel()
		testLoaderKrmignoreEnvironmentAllowlistNoFilteringStagingShadowsProduction(
			t,
			&testLoaderKrmignoreEnvironmentAllowlistNoFilteringStagingShadowsProductionInputs{
				writeTree: &writeTree,
				envOf:     &envOf,
			},
		)
	},
	)

	t.Run("issue #816 allowlist patterns select production", func(t *testing.T) {
		t.Parallel()
		testLoaderKrmignoreEnvironmentAllowlistIssue816AllowlistPatternsSelectProduction(
			t,
			&testLoaderKrmignoreEnvironmentAllowlistIssue816AllowlistPatternsSelectProductionInputs{
				commonID:  &commonID,
				writeTree: &writeTree,
				envOf:     &envOf,
			},
		)
	},
	)

	t.Run("directory exclusion selects production", func(t *testing.T) {
		t.Parallel()
		testLoaderKrmignoreEnvironmentAllowlistDirectoryExclusionSelectsProduction(
			t,
			&testLoaderKrmignoreEnvironmentAllowlistDirectoryExclusionSelectsProductionInputs{
				commonID:  &commonID,
				writeTree: &writeTree,
				envOf:     &envOf,
			},
		)
	},
	)
}

// TestLoader_IgnoreFileOverride covers Loader.IgnoreFile: an explicitly
// named file replaces <root>/.krmignore, its patterns anchor at the scan
// root regardless of where the file lives, and naming a file that does
// not exist fails loud instead of silently scanning unfiltered.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestLoader_IgnoreFileOverride(t *testing.T) {
	appID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}

	dir := t.TempDir()
	testutil.WriteFile(t, dir, ".krmignore", "/environments/staging/\n")
	testutil.WriteFile(t, dir, "ignores/production.krmignore", "/environments/production/\n")
	for _, env := range []string{"production", "staging"} {
		testutil.WriteFile(t, dir, "environments/"+env+"/app.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: app-config, namespace: default}
data: {env: `+env+`}
`)
	}

	envOf := func(t *testing.T, ignoreFile string) string {
		t.Helper()
		s := store.New()
		admitForTest(s, dir)
		l := loaderFixture(s, nil)
		l.IgnoreFile = ignoreFile
		{
			_, err := loadForTest(t.Context(), t, l, dir)
			assert.FatalIf(t, err != nil, "Load: %v", err)
		}
		{
			promoted, err := Promote(
				t.Context(),
				s,
				appID,
				PromoteOptions{
					Record: productionForTest(new(l.Index.Records[appID])),
					Admit:  func(manifest.BaseManifest) (bool, error) { return true, nil },
				},
			)
			assert.FatalIf(t, err != nil ||
				!promoted, "%s", "app-config was not indexed for promotion")
		}
		cm, ok := s.GetObject(appID).(*manifest.ConfigMap)
		assert.FatalIf(t, !ok, "%s", "app-config was not loaded")
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("empty falls back to root .krmignore", func(t *testing.T) {
		{
			got := envOf(t, "")
			assert.ErrorIf(t, got != "production", "env = %q, want production", got)
		}
	})

	t.Run("override anchors at the scan root", func(t *testing.T) {
		{
			got := envOf(t, filepath.Join(dir, "ignores", "production.krmignore"))
			assert.ErrorIf(t, got != "staging", "env = %q, want staging", got)
		}
	})

	t.Run("relative override anchors at the scan root", func(t *testing.T) {
		t.Chdir(dir)
		{
			got := envOf(t, "ignores/production.krmignore")
			assert.ErrorIf(t, got != "staging", "env = %q, want staging", got)
		}
	})

	t.Run("missing override file errors", func(t *testing.T) {
		l := New(store.New())
		l.IgnoreFile = filepath.Join(dir, "nope.krmignore")
		{
			_, err := loadForTest(t.Context(), t, l, dir)
			assert.FatalIf(t, err == nil, "%s", "Load: expected error for missing IgnoreFile")
		}
	})
}

func TestLoader_KrmignoreNormalization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, rules string
		wantErr     error
	}{
		{name: "trimmed exclusion", rules: "  secret.yaml\n"},
		{name: "trimmed inclusion", rules: "*\n  !keep.yaml\n"},
		{name: "oversized line", rules: strings.Repeat("x", 65_536) + "\n", wantErr: bufio.ErrTooLong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testLoaderKrmignoreNormalizationCase(t, &tc)
		},
		)
	}
	t.Run("reader error", func(t *testing.T) { t.Parallel(); testLoaderKrmignoreNormalizationReaderError(t) })
}

func testWalkerIgnoreMatchesMemoMatchesUncachedCase(
	t *testing.T,
	testCaseInput *struct {
		name     string
		patterns string
		wantSize int
	},
) {
	t.Helper()

	root := t.TempDir()
	if testCaseInput.patterns != "" {
		testutil.WriteFile(t, root, ".krmignore", testCaseInput.patterns)
	}
	ig, err := new(readerForTest(root)).loadIgnore(root)
	assert.FatalIf(t, err != nil, "loadIgnore: %v", err)
	w := traversal{ignore: ig, directories: map[string]struct{}{}, ignoreCache: map[ignoreKey]bool{}}
	for _, check := range []struct {
		rel   string
		isDir bool
	}{
		{"apps/junk/file.yaml", false},
		{"apps/junk/sub/file.yaml", false},
		{"apps/other/file.yaml", false},
		{"tmp", true},
		{"tmp", false},
		{"unrelated/path.yaml", false},
	} {
		t.Run(check.rel+"/"+strconv.FormatBool(check.isDir), func(t *testing.T) {
			abs := filepath.Join(root, filepath.FromSlash(check.rel))
			want := ig.matches(abs, root, check.isDir)
			for range 2 {
				{
					got := w.ignoreMatches(root, abs, check.isDir)
					assert.ErrorIf(
						t,
						got != want,
						"ignoreMatches(%q, isDir=%t) = %t, want %t",
						check.rel,
						check.isDir,
						got,
						want,
					)
				}
				assert.ErrorIf(
					t,
					testCaseInput.patterns == "" && want,
					"%s",
					"nil matcher must not match any path",
				)
			}
		})
	}
	if got := len(w.ignoreCache); got != testCaseInput.wantSize {
		t.Errorf("ignoreCache size = %d, want %d", got, testCaseInput.wantSize)
	}
}

type testLoaderKrmignoreEnvironmentAllowlistNoFilteringStagingShadowsProductionInputs struct {
	writeTree *func(t *testing.T, ignore string) *Loader
	envOf     *func(t *testing.T, l *Loader) string
}

func testLoaderKrmignoreEnvironmentAllowlistNoFilteringStagingShadowsProduction(
	t *testing.T,
	testCaseInput *testLoaderKrmignoreEnvironmentAllowlistNoFilteringStagingShadowsProductionInputs,
) {
	t.Helper()

	s := (*testCaseInput.writeTree)(t, "")
	{
		got := (*testCaseInput.envOf)(t, s)
		assert.ErrorIf(
			t,
			got != "staging",
			"unfiltered walk: app-config env = %q, want %q (alphabetically-last env wins)",
			got,
			"staging",
		)
	}
}

type testLoaderKrmignoreEnvironmentAllowlistIssue816AllowlistPatternsSelectProductionInputs struct {
	commonID  *manifest.NamedResource
	writeTree *func(t *testing.T, ignore string) *Loader
	envOf     *func(t *testing.T, l *Loader) string
}

func testLoaderKrmignoreEnvironmentAllowlistIssue816AllowlistPatternsSelectProduction(
	t *testing.T,
	testCaseInput *testLoaderKrmignoreEnvironmentAllowlistIssue816AllowlistPatternsSelectProductionInputs,
) {
	t.Helper()

	s := (*testCaseInput.writeTree)(t, `/*
!/common/**/*.yaml
!/environments/production/**/*.yaml
`)
	{
		got := (*testCaseInput.envOf)(t, s)
		assert.ErrorIf(t, got != "production", "allowlist walk: app-config env = %q, want %q", got, "production")
	}
	{
		_, indexed := s.Index.Lookup((*testCaseInput.commonID))
		assert.ErrorIf(t, !indexed, "%s", "common-settings must be re-included by !/common/**/*.yaml")
	}
}

type testLoaderKrmignoreEnvironmentAllowlistDirectoryExclusionSelectsProductionInputs struct {
	commonID  *manifest.NamedResource
	writeTree *func(t *testing.T, ignore string) *Loader
	envOf     *func(t *testing.T, l *Loader) string
}

func testLoaderKrmignoreEnvironmentAllowlistDirectoryExclusionSelectsProduction(
	t *testing.T,
	testCaseInput *testLoaderKrmignoreEnvironmentAllowlistDirectoryExclusionSelectsProductionInputs,
) {
	t.Helper()

	s := (*testCaseInput.writeTree)(t, "/environments/staging/\n")
	{
		got := (*testCaseInput.envOf)(t, s)
		assert.ErrorIf(t, got != "production", "exclusion walk: app-config env = %q, want %q", got, "production")
	}
	{
		_, indexed := s.Index.Lookup((*testCaseInput.commonID))
		assert.ErrorIf(t, !indexed, "%s", "common-settings must still load")
	}
}

func testLoaderKrmignoreNormalizationCase(t *testing.T, testCaseInput *struct {
	name    string
	rules   string
	wantErr error
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".krmignore", testCaseInput.rules)
	for _, name := range []string{"secret", "keep"} {
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+"}\n",
		)
	}
	st := store.New()
	l := New(st)
	_, err := loadForTest(t.Context(), t, l, root)
	if !errors.Is(err, testCaseInput.wantErr) {
		t.Fatalf("Load error = %v, want %v", err, testCaseInput.wantErr)
	}
	if err != nil {
		return
	}
	for _, name := range []string{"secret", "keep"} {
		_, indexed := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: name})
		want := name == "keep"
		assert.FatalIf(t, indexed != want, "%s indexed = %t, want %t", name, indexed, want)
	}
	assert.FatalIf(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil ||
		st.GetObject(manifest.BootstrapSourceID) != nil, "%s", "standalone load published bootstrap identity")
}

func testLoaderKrmignoreNormalizationReaderError(t *testing.T) {
	t.Helper()

	{
		_, err := parseIgnore(iotest.ErrReader(fs.ErrPermission), t.TempDir())
		assert.FatalIf(t, !errors.Is(err, fs.ErrPermission), "parseIgnore error = %v, want permission error", err)
	}
}
