package diff

import "github.com/home-operations/flate/internal/output"

// Format selects the diff output flavor. The github/human/brief/gitlab/
// gitea styles map to dyff's own output styles; diff is a plain unified
// diff.
type Format string

// Recognized Format values.
const (
	// FormatGitHub is dyff's `--output github` mode: path-based diff
	// syntax (`@@`, `+`, `-`, `!`) that GitHub's diff lexer renders
	// natively as a colored diff block when wrapped in a ```diff
	// fence. K8s-aware: list entries are matched by identifier
	// (container name, env-var name, etc.), so reordering a list
	// produces no diff churn.
	FormatGitHub Format = Format(output.FormatGitHub)
	// FormatDiff is a standard unified diff (`diff -u` / `git diff`
	// style) of each resource's YAML. Not Kubernetes-aware — it diffs
	// lines, so a reordered list shows churn — but familiar and
	// consumable by any unified-diff tooling.
	FormatDiff Format = Format(output.FormatDiff)
	// FormatHuman is dyff's colored, human-readable report — the default
	// style, and the zero value.
	FormatHuman Format = Format(output.FormatHuman)
	// FormatBrief is dyff's one-line-per-change summary.
	FormatBrief Format = Format(output.FormatBrief)
	// FormatGitLab is dyff's GitLab diff syntax (`=` path/root prefixes).
	FormatGitLab Format = Format(output.FormatGitLab)
	// FormatGitea is dyff's Gitea/Forgejo diff syntax.
	FormatGitea Format = Format(output.FormatGitea)
	// FormatHTML renders a self-contained HTML document: a per-resource,
	// GitHub-style diff with YAML syntax highlighting (via chroma) and a
	// side-by-side ⇄ unified toggle. Built on the same line diff as
	// FormatDiff (not Kubernetes-aware). Meant for browser review or a CI
	// artifact, not the terminal.
	FormatHTML Format = Format(output.FormatHTML)
	// FormatJSON encodes changes and suppressions in one JSON object.
	FormatJSON Format = Format(output.FormatJSON)
)
