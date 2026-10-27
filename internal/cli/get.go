package cli

import (
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/imageutil"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

func newGetCmd() *cobra.Command {
	cmd := getAllCmd("get", "List Kustomizations, HelmReleases, container images, or a cluster summary (defaults to all)")
	cmd.AddCommand(newGetKSCmd(), newGetHRCmd(), newGetImagesCmd(),
		getAllCmd("all", "Summarize every Kustomization and HelmRelease"))
	return cmd
}

func newGetKSCmd() *cobra.Command {
	return resourceListCmd(listView[*manifest.Kustomization]{
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
				"namespace": o.Namespace, "name": o.Name, "path": o.Path,
			}, doc
		},
	})
}

func newGetHRCmd() *cobra.Command {
	return resourceListCmd(listView[*manifest.HelmRelease]{
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
	})
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
				groups := make([]output.SummaryGroup, len(inv.groups))
				for i, group := range inv.groups {
					res := group.sides[0]
					groups[i] = output.SummaryGroup{Cluster: group.cluster, Summary: map[string]any{
						"kustomizations": res.Counts[manifest.KindKustomization],
						"helmReleases":   res.Counts[manifest.KindHelmRelease],
					}}
				}
				return selectedPayload{write: func() error {
					return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ClusterGroups(groups)
				}}
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatYAML, output.FormatJSON)
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
				return selectedPayload{prepare: func(names []output.Names) {
					for i := range groups {
						for _, image := range groups[i].Images {
							groups[i].Names = append(groups[i].Names, names[i].Prefix+image)
						}
					}
				}, write: func() error {
					return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ImageGroups(groups)
				}}
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatName, output.FormatYAML, output.FormatJSON)
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
				groups := make([]output.ResourceGroup, len(inv.groups))
				for i, group := range inv.groups {
					res := group.sides[0]
					groups[i] = output.ResourceGroup{Cluster: group.cluster, Rows: []map[string]string{}, Documents: []map[string]any{}}
					for _, obj := range res.Objects {
						if obj, ok := obj.(T); ok {
							row, doc := view.mapper(res, obj)
							groups[i].Rows = append(groups[i].Rows, row)
							groups[i].Documents = append(groups[i].Documents, doc)
						}
					}
				}
				return selectedPayload{prepare: func(names []output.Names) {
					for i, group := range groups {
						for _, row := range group.Rows {
							row["displayName"] = names[i].Prefix + row["name"]
						}
					}
				}, write: func() error {
					return (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).ResourceGroups(view.cols, groups)
				}}
			})
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName)
	bindSelector(cmd.Flags(), l)
	return cmd
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
