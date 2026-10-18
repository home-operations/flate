package loader

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoadIgnore_Patterns(t *testing.T) {
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
			root := t.TempDir()
			if tc.text != "" {
				testutil.WriteFile(t, root, ".krmignore", tc.text)
			}
			ig, err := readerForTest(root).loadIgnore(root)
			if err != nil {
				t.Fatalf("loadIgnore: %v", err)
			}
			for _, check := range tc.checks {
				t.Run(check.rel+"/"+fmt.Sprint(check.isDir), func(t *testing.T) {
					abs := filepath.Join(root, filepath.FromSlash(check.rel))
					if got := ig.matches(abs, root, check.isDir); got != check.want {
						t.Errorf("matches(%q, isDir=%t) = %t, want %t", check.rel, check.isDir, got, check.want)
					}
				})
			}
		})
	}
}

func TestWalkerIgnoreMatches_MemoMatchesUncached(t *testing.T) {
	for _, tc := range []struct {
		name     string
		patterns string
		wantSize int
	}{
		{name: "patterns", patterns: "apps/junk/**\ntmp/\n", wantSize: 6},
		{name: "nil matcher"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if tc.patterns != "" {
				testutil.WriteFile(t, root, ".krmignore", tc.patterns)
			}
			ig, err := readerForTest(root).loadIgnore(root)
			if err != nil {
				t.Fatalf("loadIgnore: %v", err)
			}
			w := walker{ignore: ig, scanRoot: root, visited: map[string]struct{}{}, ignoreCache: map[ignoreKey]bool{}}
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
				t.Run(check.rel+"/"+fmt.Sprint(check.isDir), func(t *testing.T) {
					abs := filepath.Join(root, filepath.FromSlash(check.rel))
					want := ig.matches(abs, root, check.isDir)
					for range 2 {
						if got := w.ignoreMatches(abs, check.isDir); got != want {
							t.Errorf("ignoreMatches(%q, isDir=%t) = %t, want %t", check.rel, check.isDir, got, want)
						}
						if tc.patterns == "" && want {
							t.Error("nil matcher must not match any path")
						}
					}
				})
			}
			if got := len(w.ignoreCache); got != tc.wantSize {
				t.Errorf("ignoreCache size = %d, want %d", got, tc.wantSize)
			}
		})
	}
}

func TestLoader_KrmignoreEnvironmentAllowlist(t *testing.T) {
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
		l := New(s)
		if _, err := l.Load(t.Context(), dir); err != nil {
			t.Fatalf("Load: %v", err)
		}
		return l
	}

	envOf := func(t *testing.T, l *Loader) string {
		t.Helper()
		if !Promote(l.Store, appID, l.Index.Records[appID], PromoteOptions{Admit: func(manifest.BaseManifest) bool { return true }}) {
			t.Fatal("app-config was not indexed for promotion")
		}
		obj := l.Store.GetObject(appID)
		if obj == nil {
			t.Fatal("app-config was not loaded at all")
		}
		cm, ok := obj.(*manifest.ConfigMap)
		if !ok {
			t.Fatalf("app-config is %T, want *manifest.ConfigMap", obj)
		}
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("no filtering: staging shadows production", func(t *testing.T) {
		s := writeTree(t, "")
		if got := envOf(t, s); got != "staging" {
			t.Errorf("unfiltered walk: app-config env = %q, want %q (alphabetically-last env wins)", got, "staging")
		}
	})

	t.Run("issue #816 allowlist patterns select production", func(t *testing.T) {
		s := writeTree(t, `/*
!/common/**/*.yaml
!/environments/production/**/*.yaml
`)
		if got := envOf(t, s); got != "production" {
			t.Errorf("allowlist walk: app-config env = %q, want %q", got, "production")
		}
		if _, indexed := s.Index.Lookup(commonID); !indexed {
			t.Error("common-settings must be re-included by !/common/**/*.yaml")
		}
	})

	t.Run("directory exclusion selects production", func(t *testing.T) {
		s := writeTree(t, "/environments/staging/\n")
		if got := envOf(t, s); got != "production" {
			t.Errorf("exclusion walk: app-config env = %q, want %q", got, "production")
		}
		if _, indexed := s.Index.Lookup(commonID); !indexed {
			t.Error("common-settings must still load")
		}
	})
}

// TestLoader_IgnoreFileOverride covers Loader.IgnoreFile: an explicitly
// named file replaces <root>/.krmignore, its patterns anchor at the scan
// root regardless of where the file lives, and naming a file that does
// not exist fails loud instead of silently scanning unfiltered.
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
		l := loaderFixture(s, nil)
		l.IgnoreFile = ignoreFile
		if _, err := l.Load(t.Context(), dir); err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !Promote(s, appID, l.Index.Records[appID], PromoteOptions{Admit: func(manifest.BaseManifest) bool { return true }}) {
			t.Fatal("app-config was not indexed for promotion")
		}
		cm, ok := s.GetObject(appID).(*manifest.ConfigMap)
		if !ok {
			t.Fatal("app-config was not loaded")
		}
		env, _ := cm.Data["env"].(string)
		return env
	}

	t.Run("empty falls back to root .krmignore", func(t *testing.T) {
		if got := envOf(t, ""); got != "production" {
			t.Errorf("env = %q, want production", got)
		}
	})

	t.Run("override anchors at the scan root", func(t *testing.T) {
		if got := envOf(t, filepath.Join(dir, "ignores", "production.krmignore")); got != "staging" {
			t.Errorf("env = %q, want staging", got)
		}
	})

	t.Run("relative override anchors at the scan root", func(t *testing.T) {
		t.Chdir(dir)
		if got := envOf(t, "ignores/production.krmignore"); got != "staging" {
			t.Errorf("env = %q, want staging", got)
		}
	})

	t.Run("missing override file errors", func(t *testing.T) {
		l := New(store.New())
		l.IgnoreFile = filepath.Join(dir, "nope.krmignore")
		if _, err := l.Load(t.Context(), dir); err == nil {
			t.Fatal("Load: expected error for missing IgnoreFile")
		}
	})
}
