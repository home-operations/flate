package diff

import "github.com/homeport/dyff/pkg/dyff"

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
	FormatGitHub Format = "github"
	// FormatDiff is a standard unified diff (`diff -u` / `git diff`
	// style) of each resource's YAML. Not Kubernetes-aware — it diffs
	// lines, so a reordered list shows churn — but familiar and
	// consumable by any unified-diff tooling.
	FormatDiff Format = "diff"
	// FormatHuman is dyff's colored, human-readable report — the default
	// style, and the zero value.
	FormatHuman Format = "human"
	// FormatBrief is dyff's one-line-per-change summary.
	FormatBrief Format = "brief"
	// FormatGitLab is dyff's GitLab diff syntax (`=` path/root prefixes).
	FormatGitLab Format = "gitlab"
	// FormatGitea is dyff's Gitea/Forgejo diff syntax.
	FormatGitea Format = "gitea"
	// FormatHTML renders a self-contained HTML document: a per-resource,
	// GitHub-style diff with YAML syntax highlighting (via chroma) and a
	// side-by-side ⇄ unified toggle. Built on the same line diff as
	// FormatDiff (not Kubernetes-aware). Meant for browser review or a CI
	// artifact, not the terminal.
	FormatHTML Format = "html"
	// FormatJSON encodes changes and suppressions in one JSON object.
	FormatJSON Format = "json"
)

const (
	dyffContextLines         = 4
	dyffMinorChangeThreshold = 0.1
)

// Prefixes mirror dyff's CLI and keep suppression notes in the same syntax.
var diffStyles = map[Format]struct {
	note   string
	writer func(dyff.Report) dyff.ReportWriter
}{
	FormatGitHub: {note: "! ", writer: diffSyntax("@@", "#", "!")},
	FormatGitLab: {note: "# ", writer: diffSyntax("=", "=", "#")},
	FormatGitea:  {note: "! ", writer: diffSyntax("@@", "=", "!")},
	FormatDiff:   {note: "# "},
	FormatHuman: {note: "⚠ ", writer: func(report dyff.Report) dyff.ReportWriter {
		return &dyff.HumanReport{
			Report: report, Indent: 2, UseIndentLines: true, OmitHeader: true,
			MultilineContextLines: dyffContextLines, MinorChangeThreshold: dyffMinorChangeThreshold,
		}
	}},
	FormatBrief: {note: "⚠ ", writer: func(report dyff.Report) dyff.ReportWriter {
		return &dyff.BriefReport{Report: report}
	}},
}

func diffSyntax(path, root, change string) func(dyff.Report) dyff.ReportWriter {
	return func(report dyff.Report) dyff.ReportWriter {
		return &dyff.DiffSyntaxReport{
			PathPrefix: path, RootDescriptionPrefix: root, ChangeTypePrefix: change,
			Report: report, Indent: 0, UseIndentLines: true, NoTableStyle: true, OmitHeader: true,
			PrefixMultiline: true, MultilineContextLines: dyffContextLines, MinorChangeThreshold: dyffMinorChangeThreshold,
		}
	}
}
