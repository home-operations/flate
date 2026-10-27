package cli

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/diffprep"
	"github.com/homeport/dyff/pkg/dyff"
	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

func newDiffCmd() *cobra.Command {
	cmd := diffCmd("diff", nil, "Diff rendered output against a previous revision (defaults to all)", "")
	cmd.AddCommand(
		diffCmd("all", nil,
			"Diff all Flux producer output against another path", ""),
		diffCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Diff Kustomizations against another path", manifest.KindKustomization),
		diffCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Diff HelmReleases against another path", manifest.KindHelmRelease),
		newDiffImagesCmd(),
	)
	return cmd
}

type diffFlags struct {
	stripAttrs  []string
	stripFields []string
}

func bindDiffFlags(cmd *cobra.Command, d *diffFlags) {
	cmd.Flags().StringArrayVar(&d.stripAttrs, "strip-attr", diff.DefaultStripAttrs, "Ignore a metadata key in diffs (repeatable; replaces defaults)")
	cmd.Flags().StringArrayVar(&d.stripFields, "strip-field", diff.DefaultStripFields, "Ignore a dotted field in diffs (repeatable; replaces defaults)")
}

func diffCmd(use string, aliases []string, short, kind string) *cobra.Command {
	c := &commonFlags{}
	d := &diffFlags{}
	positional := cobra.MaximumNArgs(1)
	if kind == "" {
		positional = cobra.NoArgs
	}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Long:    short + ". With -o json, emit a 2-space-indented JSON object {\"changes\": [...], \"suppressed\": [...]} with normalized manifests and unavailable producer disclosures.",
		Args:    positional,
		RunE: func(cmd *cobra.Command, args []string) error {
			return d.run(cmd, c, c.roots(firstArg(args), kind)[0])
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.DiffFormats...)
	bindDiffFlags(cmd, d)
	return cmd
}

func newDiffImagesCmd() *cobra.Command {
	c := &commonFlags{}
	var includeRemoved bool
	cmd := &cobra.Command{
		Use:   "images",
		Short: "Diff container images between current and baseline",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDiffImages(cmd, c, includeRemoved)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatName, output.FormatYAML, output.FormatJSON)
	cmd.Flags().BoolVar(&includeRemoved, "include-removed", false, "Include images found only in the baseline")
	return cmd
}

func runDiffImages(cmd *cobra.Command, c *commonFlags, includeRemoved bool) error {
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots("", renderKinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
	}, func(inv *selectedInvocation) selectedPayload {
		groups := make([]output.ImageGroup, len(inv.groups))
		result := selectedPayload{warnings: make([][]manifest.Warning, len(groups))}
		for i, group := range inv.groups {
			orig, current := group.sides[0], group.sides[1]
			imgs, suppressed, err := diff.CompareImages(
				diff.Snapshot{Manifests: orig.Manifests, Unavailable: orig.Unavailable},
				diff.Snapshot{Manifests: current.Manifests, Unavailable: current.Unavailable},
				nil, includeRemoved,
			)
			groups[i] = output.ImageGroup{Cluster: group.cluster, Images: imgs, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead]}
			result.warnings[i], result.err = appendSuppressionWarnings(nil, suppressed), errors.Join(result.err, err)
		}
		result.prepare = func(names []output.Names) {
			for i := range groups {
				for _, image := range groups[i].Images {
					groups[i].Names = append(groups[i].Names, names[i].Prefix+image)
				}
			}
		}
		result.write = func() error {
			return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ImageGroups(groups)
		}
		return result
	})
}

func (d *diffFlags) run(cmd *cobra.Command, c *commonFlags, root manifest.NamedResource) error {
	kinds := runnableKinds
	if root.Kind != "" {
		kinds = []string{root.Kind}
	}
	return runSelected(cmd.Context(), selectedRequest{
		flags: *c, roots: c.roots(root.Name, kinds...), pair: true,
		stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
	}, func(inv *selectedInvocation) selectedPayload {
		format := output.Format(c.output)
		encoder := output.Encoder{Writer: cmd.OutOrStdout(), Format: format}
		groups := make([]output.DiffGroup, len(inv.groups))
		lines := make([][]output.LineChange, len(inv.groups))
		native := make([]dyff.Report, len(inv.groups))
		nativeSubjects := make([][]manifest.NamedResource, len(inv.groups))
		result := selectedPayload{warnings: make([][]manifest.Warning, len(groups)), subjects: make([][]manifest.NamedResource, len(groups))}
		for i, group := range inv.groups {
			orig, current := group.sides[0], group.sides[1]
			if format == output.FormatJSON {
				body, warnings, suppressed, err := diff.Compare(
					diff.Snapshot{Manifests: orig.Manifests, Unavailable: orig.Unavailable},
					diff.Snapshot{Manifests: current.Manifests, Unavailable: current.Unavailable}, nil,
					diff.Options{Format: diff.FormatJSON, StripAttrs: d.stripAttrs, StripFields: d.stripFields})
				groups[i] = output.DiffGroup{Cluster: group.cluster, Added: group.absent[manifest.TreeBase], Removed: group.absent[manifest.TreeHead], Body: body}
				result.warnings[i] = appendSuppressionWarnings(warnings, suppressed)
				result.err = errors.Join(result.err, err)
				continue
			}
			left, right, notes := diffprep.Suppress(diffprep.Snapshot{Manifests: orig.Manifests, Unavailable: orig.Unavailable},
				diffprep.Snapshot{Manifests: current.Manifests, Unavailable: current.Unavailable})
			p := diffprep.Prepare(diffprep.Documents(left), diffprep.Documents(right), diffprep.Options{StripAttrs: d.stripAttrs, StripFields: d.stripFields})
			result.warnings[i] = slices.Grow(p.Warnings, len(notes))
			for _, note := range notes {
				result.warnings[i] = append(result.warnings[i], manifest.Warning{
					Resource: note.Parent.NamedResource, Category: manifest.WarnDiffSuppressed, Message: note.String(),
				})
			}
			slices.SortFunc(result.warnings[i], manifest.CompareWarning)
			var err error
			switch format {
			case output.FormatDiff, output.FormatHTML:
				lines[i], err = output.PrepareLines(p)
				result.subjects[i] = output.LineSubjects(lines[i], format)
			default:
				native[i], err = p.Native()
				if err == nil {
					ids := [2][]manifest.NamedResource{make([]manifest.NamedResource, len(native[i].From.Documents)), make([]manifest.NamedResource, len(native[i].To.Documents))}
					for _, pair := range p.Pairs {
						if pair.Old != nil {
							ids[0][pair.OldPos] = pair.Resource
						}
						if pair.New != nil {
							ids[1][pair.NewPos] = pair.Resource
						}
					}
					for _, delta := range native[i].Diffs {
						side := 0
						if delta.Path.Root.Location == "to" {
							side = 1
						}
						nativeSubjects[i] = append(nativeSubjects[i], ids[side][delta.Path.DocumentIdx])
					}
				}
			}
			result.err = errors.Join(result.err, err)
		}
		aggregate := aggregateNative(native)
		if format != output.FormatBrief && len(aggregate.From.Documents) > 1 {
			copy(result.subjects, nativeSubjects)
		}
		result.prepare = func(names []output.Names) {
			from, to := 0, 0
			for i := range inv.groups {
				output.NameLines(lines[i], names[i])
				for j, name := range native[i].From.Names {
					aggregate.From.Names[from+j] = names[i].Prefix + name
				}
				for j, name := range native[i].To.Names {
					aggregate.To.Names[to+j] = names[i].Prefix + name
				}
				from += len(native[i].From.Names)
				to += len(native[i].To.Names)
			}
		}
		result.write = func() error {
			switch format {
			case output.FormatJSON:
				return encoder.DiffGroups(groups)
			case output.FormatDiff, output.FormatHTML:
				return encoder.Lines(slices.Concat(lines...))
			default:
				return encoder.Native(*aggregate)
			}
		}
		return result
	})
}

func appendSuppressionWarnings(warnings []manifest.Warning, suppressed []diff.Suppression) []manifest.Warning {
	if len(suppressed) == 0 {
		return warnings
	}
	warnings = slices.Grow(warnings, len(suppressed))
	for _, note := range suppressed {
		warnings = append(warnings, manifest.Warning{
			Resource: note.Parent.NamedResource, Category: manifest.WarnDiffSuppressed,
			Message: note.String(),
		})
	}
	slices.SortFunc(warnings, manifest.CompareWarning)
	return warnings
}

func aggregateNative(groups []dyff.Report) *dyff.Report {
	report := &dyff.Report{}
	report.From.Location, report.To.Location = "from", "to"
	for _, group := range groups {
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
