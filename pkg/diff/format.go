package diff

// Format selects the diff output flavor. The github/human/brief/gitlab/
// gitea styles map to dyff's own output styles; diff is a plain unified
// diff; yaml/json/markdown are flate aggregations of the per-resource
// bodies.
type Format string

// Recognized Format values.
const (
	// FormatGitHub is dyff's `--output github` mode: path-based diff
	// syntax (`@@`, `+`, `-`, `!`) that GitHub's diff lexer renders
	// natively as a colored diff block when wrapped in a ```diff
	// fence. K8s-aware: list entries are matched by identifier
	// (container name, env-var name, etc.), so reordering a list
	// produces no diff churn. The default style, and the body style
	// embedded in yaml/json/markdown output.
	FormatGitHub Format = "github"
	// FormatDiff is a standard unified diff (`diff -u` / `git diff`
	// style) of each resource's YAML. Not Kubernetes-aware — it diffs
	// lines, so a reordered list shows churn — but familiar and
	// consumable by any unified-diff tooling.
	FormatDiff Format = "diff"
	// FormatHuman is dyff's default colored, human-readable report.
	FormatHuman Format = "human"
	// FormatBrief is dyff's one-line-per-change summary.
	FormatBrief Format = "brief"
	// FormatGitLab is dyff's GitLab diff syntax (`=` path/root prefixes).
	FormatGitLab Format = "gitlab"
	// FormatGitea is dyff's Gitea/Forgejo diff syntax.
	FormatGitea Format = "gitea"
	// FormatYAML is a structured list of {parent, resource, body}.
	FormatYAML Format = "yaml"
	// FormatJSON is FormatYAML marshaled as JSON.
	FormatJSON Format = "json"
	// FormatMarkdown is a PR-comment view: a summary table plus one
	// ```diff fence per resource.
	FormatMarkdown Format = "markdown"
)

// bodyStyle maps an output format to the per-resource diff body style.
// yaml/json embed the body verbatim and markdown wraps it in a ```diff
// fence, so all three (and the zero value) use the canonical github
// diff-syntax; every other format renders its own body.
func bodyStyle(f Format) Format {
	switch f {
	case FormatYAML, FormatJSON, FormatMarkdown, "":
		return FormatGitHub
	default:
		return f
	}
}
