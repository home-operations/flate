package cli

import (
	"cmp"
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

func newGetCmd() *cobra.Command {
	cmd := getAllCmd("get", "List Kustomizations, HelmReleases, or container images (defaults to all)")
	cmd.AddCommand(newGetKSCmd(), newGetHRCmd(), newGetImagesCmd(),
		getAllCmd("all", "List every Kustomization and HelmRelease"))
	return cmd
}

func newGetKSCmd() *cobra.Command {
	return resourceListCmd(ksView())
}

func ksView() listView[*manifest.Kustomization] {
	return listView[*manifest.Kustomization]{
		use: "ks", aliases: []string{"kustomization", "kustomizations"},
		short: "List Kustomizations", kind: manifest.KindKustomization, cols: ksColumns,
		mapper: func(_ *selection.Selection, o *manifest.Kustomization) (row map[string]string, doc map[string]any) {
			source := o.SourceID()
			doc = map[string]any{
				"kind": manifest.KindKustomization, "namespace": o.Namespace,
				"name": o.Name, "path": o.Path,
				"sourceRef": map[string]string{
					"kind":      source.Kind,
					"name":      source.Name,
					"namespace": source.Namespace,
				},
				"prune": o.Prune,
				"wait":  o.Wait,
			}
			if o.Suspend {
				doc["suspend"] = true
			}
			if o.TargetNamespace != "" {
				doc["targetNamespace"] = o.TargetNamespace
			}
			return map[string]string{
				"namespace": o.Namespace, "name": o.Name, "path": cmp.Or(o.Path, "./"),
			}, doc
		},
	}
}

func newGetHRCmd() *cobra.Command {
	return resourceListCmd(hrView())
}

func hrView() listView[*manifest.HelmRelease] {
	return listView[*manifest.HelmRelease]{
		use: "hr", aliases: []string{"helmrelease", "helmreleases"},
		short: "List HelmReleases", kind: manifest.KindHelmRelease, cols: hrColumns,
		mapper: func(res *selection.Selection, o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			source := o.ChartSource()
			chart := o.Chart
			if source.Name != "" {
				chart.Source = source
			}
			chartName := chart.ChartName()
			version := o.Chart.Version
			if version == "" {
				version = res.ChartVersions[o.Named()]
			}
			doc = map[string]any{
				"kind": manifest.KindHelmRelease, "namespace": o.Namespace,
				"name": o.Name, "chart": chartName,
				"version": version, "source": source.Name,
				"sourceRef": map[string]string{
					"kind":      source.Kind,
					"name":      source.Name,
					"namespace": source.Namespace,
				},
				"releaseName": o.ReleaseName(),
			}
			if o.Suspend {
				doc["suspend"] = true
			}
			if o.TargetNamespace != "" {
				doc["targetNamespace"] = o.TargetNamespace
			}
			return map[string]string{
				"namespace": o.Namespace, "name": o.Name,
				"chart": chartName, "version": version,
				"source": source.Name,
			}, doc
		},
	}
}

func getAllCmd(use, short string) *cobra.Command {
	c := &commonFlags{}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSelected(cmd.Context(), selectedRequest{
				flags: *c, roots: c.roots("", renderKinds...),
				stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
			}, func(inv *selectedInvocation) selectedPayload {
				return resourcePayload(inv, []listSection{
					collectSelected(inv, ksView()),
					collectSelected(inv, hrView()),
				}, output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}, true)
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName)
	return cmd
}

func newGetImagesCmd() *cobra.Command {
	c := &commonFlags{}
	cmd := &cobra.Command{
		Use:   "images",
		Short: "List container images across the cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSelected(cmd.Context(), selectedRequest{
				flags: *c, roots: c.roots("", renderKinds...),
				stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
			}, func(inv *selectedInvocation) selectedPayload {
				groups := make([]output.ImageGroup, len(inv.groups))
				for i, group := range inv.groups {
					groups[i] = output.ImageGroup{Cluster: group.cluster, Images: slices.Sorted(maps.Keys(imageutil.Collect(group.sides[0].Manifests)))}
				}
				table := output.Table{Columns: []output.Column{{Header: "IMAGE", Key: "image"}}}
				for _, group := range groups {
					for _, image := range group.Images {
						table.Rows = append(table.Rows, map[string]string{"cluster": group.Cluster, "image": image})
					}
				}
				tables, _ := prepareGetTables(inv, []output.Table{table})
				return selectedPayload{prepare: func(names []output.Names) {
					for i := range groups {
						for _, image := range groups[i].Images {
							groups[i].Names = append(groups[i].Names, names[i].Prefix+image)
						}
					}
				}, write: func() error {
					encoder := output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}
					if encoder.Format == output.FormatTable {
						return encoder.Tables(tables)
					}
					return encoder.ImageGroups(groups)
				}}
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName)
	return cmd
}

type listView[T manifest.BaseManifest] struct {
	use, short, kind string
	aliases          []string
	cols             []output.Column
	mapper           func(*selection.Selection, T) (map[string]string, map[string]any)
}

func resourceListCmd[T manifest.BaseManifest](view listView[T]) *cobra.Command {
	c := &commonFlags{}
	l := &listFlags{}
	cmd := &cobra.Command{
		Use: view.use + " [name]", Aliases: view.aliases, Short: view.short,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSelected(cmd.Context(), selectedRequest{
				flags: *c, roots: c.roots(firstArg(args), view.kind), labels: l.labels,
				stdout: cmd.OutOrStdout(), stderr: cmd.ErrOrStderr(),
			}, func(inv *selectedInvocation) selectedPayload {
				return resourcePayload(inv, []listSection{collectSelected(inv, view)},
					output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}, false)
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName)
	bindSelector(cmd.Flags(), l)
	return cmd
}

type listSection struct {
	kind      string
	table     output.Table
	documents []output.DocumentGroup
}

func collectSelected[T manifest.BaseManifest](inv *selectedInvocation, view listView[T]) listSection {
	section := listSection{kind: view.kind, table: output.Table{Columns: view.cols},
		documents: make([]output.DocumentGroup, len(inv.groups))}
	for i, group := range inv.groups {
		res := group.sides[0]
		section.documents[i] = output.DocumentGroup{Cluster: group.cluster, Documents: []map[string]any{}}
		for _, obj := range res.Objects {
			if obj, ok := obj.(T); ok {
				row, doc := view.mapper(res, obj)
				row["cluster"] = group.cluster
				section.table.Rows = append(section.table.Rows, row)
				section.documents[i].Documents = append(section.documents[i].Documents, doc)
			}
		}
	}
	return section
}

func resourcePayload(inv *selectedInvocation, sections []listSection, encoder output.Encoder, combined bool) selectedPayload {
	var tables []output.Table
	documents := make([]output.DocumentGroup, len(inv.groups))
	for i, group := range inv.groups {
		documents[i] = output.DocumentGroup{Cluster: group.cluster, Documents: []map[string]any{}}
	}
	for _, section := range sections {
		for i, group := range section.documents {
			documents[i].Documents = append(documents[i].Documents, group.Documents...)
		}
		if combined {
			if len(section.table.Rows) == 0 {
				continue
			}
			for _, row := range section.table.Rows {
				row["name"] = section.kind + "/" + row["name"]
			}
		}
		tables = append(tables, section.table)
	}
	tables, multi := prepareGetTables(inv, tables)
	return selectedPayload{write: func() error {
		switch encoder.Format {
		case output.FormatJSON, output.FormatYAML:
			return encoder.DocumentGroups(documents)
		case output.FormatName:
			return encoder.Names(func(yield func(string) bool) {
				for _, table := range tables {
					for _, row := range table.Rows {
						name := row["name"]
						if multi {
							name = row["cluster"] + ":" + name
						}
						if !yield(name) {
							return
						}
					}
				}
			})
		default:
			return encoder.Tables(tables)
		}
	}}
}

func prepareGetTables(inv *selectedInvocation, tables []output.Table) ([]output.Table, bool) {
	multi := len(inv.groups) > 1
	prepared := slices.Clone(tables)
	for i := range prepared {
		if multi {
			prepared[i].Columns = slices.Concat([]output.Column{{Header: "CLUSTER", Key: "cluster"}}, prepared[i].Columns)
		}
	}
	return prepared, multi
}

var (
	ksColumns = []output.Column{
		{Header: "NAMESPACE", Key: "namespace"},
		{Header: "NAME", Key: "name"},
		{Header: "PATH", Key: "path"},
	}
	hrColumns = []output.Column{
		{Header: "NAMESPACE", Key: "namespace"},
		{Header: "NAME", Key: "name"},
		{Header: "CHART", Key: "chart"},
		{Header: "VERSION", Key: "version"},
		{Header: "SOURCE", Key: "source"},
	}
)
