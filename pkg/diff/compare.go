package diff

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/homeport/dyff/pkg/dyff"

	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Snapshot contains selected document batches and their render availability.
// Manifests and status entries remain immutable during comparison.
type Snapshot struct {
	Subjects    map[manifest.NamedResource]manifest.DiagnosticIdentity
	Manifests   []manifest.DocumentBatch
	Unavailable map[manifest.NamedResource]store.StatusInfo
	// ProducerKeys maps actual producers to their paired comparison identities.
	ProducerKeys map[manifest.NamedResource]manifest.NamedResource
}

// Group retains selected errors so partial output stays available.
type Group struct {
	Cluster        string
	Orig, Current  Snapshot
	SelectedErr    error
	Added, Removed bool
}

// GroupResult keeps raw warnings separate from structured suppression disclosures.
type GroupResult struct {
	Warnings   []manifest.Warning
	Suppressed []Suppression
	Subjects   []manifest.NamedResource
}

// Comparison retains prepared output so repeated writes never normalize inputs again.
type Comparison struct {
	Groups []GroupResult
	format Format
	output []preparedGroup
}

type preparedGroup struct {
	cluster        string
	added, removed bool
	lines          []lineChange
	native         dyff.Report
	changes        []Change
	subjects       [2]map[manifest.NamedResource]manifest.DiagnosticIdentity
}

// Compare preserves group order and returns usable output alongside preparation and selected errors.
//
//nolint:gocritic // Comparison takes a value snapshot of caller options.
func Compare(groups []Group, opts Options) (*Comparison, error) {
	c := &Comparison{
		Groups: make([]GroupResult, len(groups)),
		output: make([]preparedGroup, len(groups)),
		format: cmp.Or(opts.Format, FormatHuman),
	}
	var resultErr error
	for i := range groups {
		group := &groups[i]
		p, notes := prepareSnapshots(group.Orig, group.Current, &opts)
		c.Groups[i].Warnings = p.Warnings
		c.Groups[i].Suppressed = append(slices.Clone(opts.Suppressed), notes...)
		var err error
		c.output[i], c.Groups[i].Subjects, err = prepareOutput(p, c.format)
		c.output[i].cluster, c.output[i].added, c.output[i].removed = group.Cluster, group.Added, group.Removed
		resultErr = errors.Join(resultErr, err, group.SelectedErr)
	}
	c.nativeInventory()
	return c, resultErr
}

func prepareOutput(p prepared, format Format) (preparedGroup, []manifest.NamedResource, error) {
	out := preparedGroup{subjects: p.Subjects}
	var subjects []manifest.NamedResource
	var err error
	switch format {
	case FormatDiff, FormatHTML:
		out.lines, err = prepareLines(p)
		subjects = lineSubjects(out.lines, format)
	case FormatJSON:
		out.changes = publicChanges(p)
	case FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea:
		out.native, err = p.native()
		if err == nil {
			subjects = nativeDiffSubjects(&p, &out.native)
		}
	default:
		err = fmt.Errorf("unsupported diff format %q", format)
	}
	return out, subjects, err
}

func (c *Comparison) nativeInventory() {
	if c.format == FormatDiff || c.format == FormatHTML || c.format == FormatJSON {
		return
	}
	count := 0
	for i := range c.output {
		count += len(c.output[i].native.From.Documents)
	}
	if c.format == FormatBrief || count <= 1 {
		for i := range c.Groups {
			c.Groups[i].Subjects = nil
		}
	}
}

func (c *Comparison) Write(w io.Writer, names []Names) error {
	if names == nil {
		names = c.names()
	}
	e := encoder{Writer: w, Format: c.format}
	switch c.format {
	case FormatJSON:
		groups := make([]diffGroup, len(c.Groups))
		for i := range groups {
			out := &c.output[i]
			body, err := renderJSON(slices.Clone(out.changes), c.Groups[i].Suppressed, out.subjects)
			if err != nil {
				return err
			}
			groups[i] = diffGroup{Cluster: out.cluster, Added: out.added, Removed: out.removed, Body: body}
		}
		return e.diffGroups(groups)
	case FormatDiff, FormatHTML:
		var lines []lineChange
		for i := range c.output {
			group := slices.Clone(c.output[i].lines)
			nameLines(group, names[i])
			lines = append(lines, group...)
		}
		return e.lines(lines)
	case FormatHuman, FormatGitHub, FormatBrief, FormatGitLab, FormatGitea:
		groups := make([]dyff.Report, len(c.output))
		for i := range groups {
			groups[i] = c.output[i].native
		}
		report := aggregateNative(groups)
		from, to := 0, 0
		for i := range groups {
			for j, name := range groups[i].From.Names {
				report.From.Names[from+j] = names[i].Prefix + name
			}
			for j, name := range groups[i].To.Names {
				report.To.Names[to+j] = names[i].Prefix + name
			}
			from += len(groups[i].From.Names)
			to += len(groups[i].To.Names)
		}
		return e.native(report)
	default:
		return nil
	}
}

func (c *Comparison) names() []Names {
	clusters := make([]string, len(c.Groups))
	inventory := make([][]manifest.NamedResource, len(c.Groups))
	subjects := make([]map[manifest.NamedResource]manifest.DiagnosticIdentity, len(c.Groups))
	for i := range c.Groups {
		clusters[i], inventory[i] = c.output[i].cluster, c.Groups[i].Subjects
		subjects[i] = c.output[i].subjects[0]
		if subjects[i] == nil {
			subjects[i] = c.output[i].subjects[1]
		} else if c.output[i].subjects[1] != nil {
			subjects[i] = maps.Clone(subjects[i])
			maps.Copy(subjects[i], c.output[i].subjects[1])
		}
	}
	return PrepareNames(clusters, inventory, subjects).Groups
}

func renderPrepared(p prepared, opts *Options) ([]byte, error) {
	format := cmp.Or(opts.Format, FormatHuman)
	out, subjects, err := prepareOutput(p, format)
	if err != nil {
		return nil, err
	}
	c := &Comparison{
		format: format,
		output: []preparedGroup{out},
		Groups: []GroupResult{{Warnings: p.Warnings, Suppressed: slices.Clone(opts.Suppressed), Subjects: subjects}},
	}
	c.nativeInventory()
	var buf bytes.Buffer
	if err := c.Write(&buf, nil); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MergeWarnings preserves duplicate disclosures and never mutates caller-owned slices.
func MergeWarnings(warnings []manifest.Warning, notes []Suppression) []manifest.Warning {
	out := slices.Clone(warnings)
	for i := range notes {
		note := &notes[i]
		out = append(out, manifest.Warning{
			Resource: note.Parent.NamedResource, Subject: note.Subject,
			Category: manifest.WarnDiffSuppressed, Message: note.String(),
		})
	}
	slices.SortFunc(out, manifest.CompareWarning)
	return out
}

// CompareImages returns the sorted images added between selected orig/current
// snapshots, plus removals when requested. Unavailable producer batches are
// withheld before extraction and disclosed as structured suppressions with document counts.
func CompareImages(orig, current Snapshot, selectedErr error, includeRemoved bool) ([]string, []Suppression, error) {
	projection := suppress(orig, current)
	left, right, suppressed := projection.Orig, projection.Current, projection.Suppressed
	origImages := imageutil.Collect(left)
	currentImages := imageutil.Collect(right)
	out := make([]string, 0, len(currentImages))
	for img := range currentImages {
		if _, ok := origImages[img]; !ok {
			out = append(out, img)
		}
	}
	if includeRemoved {
		for img := range origImages {
			if _, ok := currentImages[img]; !ok {
				out = append(out, img)
			}
		}
	}
	slices.Sort(out)
	return out, suppressed, selectedErr
}

func aggregateNative(groups []dyff.Report) *dyff.Report {
	report := &dyff.Report{}
	report.From.Location, report.To.Location = "from", "to"
	for groupIndex := range groups {
		group := groups[groupIndex]
		from, to := len(report.From.Documents), len(report.To.Documents)
		report.From.Documents = append(report.From.Documents, group.From.Documents...)
		report.From.Names = append(report.From.Names, group.From.Names...)
		report.To.Documents = append(report.To.Documents, group.To.Documents...)
		report.To.Names = append(report.To.Names, group.To.Names...)
		for _, delta := range group.Diffs {
			path := *delta.Path
			if path.Root.Location == "to" {
				path.Root, path.DocumentIdx = &report.To, path.DocumentIdx+to
			} else {
				path.Root, path.DocumentIdx = &report.From, path.DocumentIdx+from
			}
			delta.Path = &path
			report.Diffs = append(report.Diffs, delta)
		}
	}
	return report
}

func nativeDiffSubjects(prepared *prepared, report *dyff.Report) []manifest.NamedResource {
	subjects := slices.Grow([]manifest.NamedResource(nil), len(report.Diffs))
	ids := [2][]manifest.NamedResource{
		make([]manifest.NamedResource, len(report.From.Documents)),
		make([]manifest.NamedResource, len(report.To.Documents)),
	}
	for pairIndex := range prepared.Pairs {
		pair := prepared.Pairs[pairIndex]
		if pair.Old != nil {
			ids[0][pair.OldPos] = pair.Resource
		}
		if pair.New != nil {
			ids[1][pair.NewPos] = pair.Resource
		}
	}
	for _, delta := range report.Diffs {
		side := 0
		if delta.Path.Root.Location == "to" {
			side = 1
		}
		subjects = append(subjects, ids[side][delta.Path.DocumentIdx])
	}
	return subjects
}
