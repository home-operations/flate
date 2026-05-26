package loader

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// ignoreSet is the matched-rule set from a .krmignore (or .gitignore-style)
// file at the scan root.
type ignoreSet struct {
	patterns []string
}

// loadIgnore reads <root>/.krmignore (or returns an empty set if not
// present). The grammar is a subset of gitignore: hash comments, blank
// lines, and one pattern per line; patterns are interpreted relative to
// root via filepath.Match-style globbing.
func loadIgnore(root string) (*ignoreSet, error) {
	out := &ignoreSet{}
	path := filepath.Join(root, ".krmignore")
	f, err := os.Open(path) //nolint:gosec // root is the cluster scan root
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return out, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out.patterns = append(out.patterns, line)
	}
	return out, sc.Err()
}

// matches reports whether path (an absolute file path under root)
// should be ignored.
func (i *ignoreSet) matches(path, root string) bool {
	if i == nil || len(i.patterns) == 0 {
		return false
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	for _, pat := range i.patterns {
		if matched, _ := filepath.Match(pat, rel); matched {
			return true
		}
		// Support directory-prefix matches (pat/...).
		if strings.HasPrefix(rel, strings.TrimSuffix(pat, "/")+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

