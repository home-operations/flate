package sourceignore

import "testing"

func TestNew_PatternDefaults(t *testing.T) {
	extra := "*.tmp\n"
	cases := []struct {
		name         string
		files        map[string]string
		extra        *string
		withDefaults bool
		wantDefaults bool
		wantVCS      bool
		wantTemp     bool
	}{
		{name: "no sourceignore", withDefaults: true, wantDefaults: true, wantVCS: true},
		{name: "empty sourceignore", files: map[string]string{".sourceignore": ""}, withDefaults: true, wantDefaults: true, wantVCS: true},
		{name: "comments only", files: map[string]string{".sourceignore": "# comment\n\n"}, withDefaults: true, wantDefaults: true, wantVCS: true},
		{name: "root patterns", files: map[string]string{".sourceignore": "*.tmp\n"}, withDefaults: true, wantVCS: true, wantTemp: true},
		{name: "nested patterns", files: map[string]string{"app/.sourceignore": "*.tmp\n"}, withDefaults: true, wantVCS: true, wantTemp: true},
		{name: "extra patterns", extra: &extra, withDefaults: true, wantVCS: true, wantTemp: true},
		{name: "defaults disabled"},
		{name: "patterns without defaults", files: map[string]string{".sourceignore": "*.tmp\n"}, wantTemp: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for rel, body := range tc.files {
				write(t, root, rel, body)
			}
			matcher, err := New(root, tc.extra, tc.withDefaults)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			for _, rel := range []string{"app/icons/logo.png", ".github/workflows/ci.yml", "app/.sops.yaml"} {
				if got := matcher.Match(rel, false); got != tc.wantDefaults {
					t.Errorf("Match(%q) = %v, want %v", rel, got, tc.wantDefaults)
				}
			}
			if got := matcher.Match(".git/HEAD", false); got != tc.wantVCS {
				t.Errorf("Match(.git/HEAD) = %v, want %v", got, tc.wantVCS)
			}
			if got := matcher.Match("app/cache.tmp", false); got != tc.wantTemp {
				t.Errorf("Match(app/cache.tmp) = %v, want %v", got, tc.wantTemp)
			}
			if matcher.Match("app/keep.yaml", false) {
				t.Error("unmatched manifest must remain visible")
			}
		})
	}
}
