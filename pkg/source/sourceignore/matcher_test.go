package sourceignore

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestNew_PatternDefaults(t *testing.T) {
	defaultMatches := []string{".git/HEAD", ".github/workflows/ci.yml", "app/icons/logo.png", "app/.sops.yaml"}
	defaultKeeps := []string{"app/keep.yaml", "app/cache.tmp", "other/cache.tmp"}
	patternKeeps := []string{"app/keep.yaml", ".github/workflows/ci.yml", "app/icons/logo.png", "app/.sops.yaml"}
	cases := []struct {
		name         string
		files        map[string]string
		extra        *string
		withDefaults bool
		match        []string
		keep         []string
	}{
		{name: "no sourceignore", withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "empty field", extra: new(""), withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "empty sourceignore", files: map[string]string{".sourceignore": ""}, withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "empty sourceignore and field", files: map[string]string{".sourceignore": ""}, extra: new(""), withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "whitespace sourceignore", files: map[string]string{".sourceignore": " \t\n\n"}, withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "whitespace sourceignore and empty field", files: map[string]string{".sourceignore": " \t\n\n"}, extra: new(""), withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "comments only", files: map[string]string{".sourceignore": "# comment\n\n"}, withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{name: "comments only and empty field", files: map[string]string{".sourceignore": "# comment\n\n"}, extra: new(""), withDefaults: true, match: defaultMatches, keep: defaultKeeps},
		{
			name: "root patterns", files: map[string]string{".sourceignore": "*.tmp\n"}, withDefaults: true,
			match: []string{".git/HEAD", "app/cache.tmp", "other/cache.tmp"}, keep: patternKeeps,
		},
		{
			name: "nested patterns", files: map[string]string{"app/.sourceignore": "*.tmp\n"}, withDefaults: true,
			match: []string{".git/HEAD", "app/cache.tmp"}, keep: append([]string{"other/cache.tmp"}, patternKeeps...),
		},
		{
			name: "field patterns", extra: new("*.tmp\n"), withDefaults: true,
			match: []string{".git/HEAD", "app/cache.tmp", "other/cache.tmp"}, keep: patternKeeps,
		},
		{
			name: "field negates file pattern", files: map[string]string{".sourceignore": "*.tmp\n"},
			extra: new("!app/keep.tmp\n"), withDefaults: true,
			match: []string{".git/HEAD", "app/cache.tmp", "other/cache.tmp"}, keep: append([]string{"app/keep.tmp"}, patternKeeps...),
		},
		{
			name: "defaults disabled",
			keep: append([]string{".git/HEAD", "app/cache.tmp", "other/cache.tmp"}, patternKeeps...),
		},
		{
			name: "empty field without defaults", extra: new(""),
			keep: append([]string{".git/HEAD", "app/cache.tmp", "other/cache.tmp"}, patternKeeps...),
		},
		{
			name: "file patterns without defaults", files: map[string]string{".sourceignore": "*.tmp\n"},
			match: []string{"app/cache.tmp", "other/cache.tmp"}, keep: append([]string{".git/HEAD"}, patternKeeps...),
		},
		{
			name: "field patterns without defaults", extra: new("*.tmp\n"),
			match: []string{"app/cache.tmp", "other/cache.tmp"}, keep: append([]string{".git/HEAD"}, patternKeeps...),
		},
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
			for _, rel := range tc.match {
				t.Run("match/"+rel, func(t *testing.T) {
					assert.Equal(t, matcher.Match(rel, false), true)
				})
			}
			for _, rel := range tc.keep {
				t.Run("keep/"+rel, func(t *testing.T) {
					assert.Equal(t, matcher.Match(rel, false), false)
				})
			}
		})
	}
}
