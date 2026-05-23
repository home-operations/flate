package cli

import (
	"io"
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/selector"
)

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "List Kustomizations, HelmReleases, container images, or a cluster summary",
	}
	cmd.AddCommand(newGetKSCmd(), newGetHRCmd(), newGetImagesCmd(), newGetAllCmd())
	return cmd
}

func newGetKSCmd() *cobra.Command {
	return resourceListCmd("ks", []string{"kustomization", "kustomizations"},
		"List Kustomizations", manifest.KindKustomization, ksColumns,
		func(o *manifest.Kustomization) (row map[string]string, doc map[string]any) {
			doc = map[string]any{
				"kind": manifest.KindKustomization, "namespace": o.Namespace,
				"name": o.Name, "path": o.Path,
				"sourceRef": map[string]string{
					"kind":      o.SourceKind,
					"name":      o.SourceName,
					"namespace": o.SourceNamespace,
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
	)
}

func newGetHRCmd() *cobra.Command {
	return resourceListCmd("hr", []string{"helmrelease", "helmreleases"},
		"List HelmReleases", manifest.KindHelmRelease, hrColumns,
		func(o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			doc = map[string]any{
				"kind": manifest.KindHelmRelease, "namespace": o.Namespace,
				"name": o.Name, "chart": o.Chart.ChartName(),
				"version": o.Chart.Version, "source": o.Chart.RepoName,
				"sourceRef": map[string]string{
					"kind":      o.Chart.RepoKind,
					"name":      o.Chart.RepoName,
					"namespace": o.Chart.RepoNamespace,
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
				"chart": o.Chart.ChartName(), "version": o.Chart.Version,
				"source": o.Chart.RepoName,
			}, doc
		})
}

func newGetAllCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Summarize every Kustomization and HelmRelease",
		RunE: func(cmd *cobra.Command, _ []string) error {
			o, err := runOrchestrator(cmdContext(cmd), *c, *h)
			if err != nil {
				return err
			}
			return printCluster(cmd.OutOrStdout(), o, c.output)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

// newGetImagesCmd emits a deduplicated list of container images
// extracted from every rendered Kustomization and HelmRelease — the
// symmetric counterpart of `flate diff images`, which emits the same
// shape filtered to images that actually changed between paths.
func newGetImagesCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:   "images",
		Short: "List container images across the cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			o, err := runOrchestrator(cmdContext(cmd), *c, *h)
			if err != nil {
				return err
			}
			imgs := slices.Sorted(maps.Keys(collectImages(o, c)))
			return emitImageList(cmd.OutOrStdout(), imgs, c.output)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

// resourceListCmd builds a `get <kind>` subcommand. mapper converts each
// stored object to (table row, structured doc); cols is the table
// schema.
func resourceListCmd[T manifest.BaseManifest](
	use string, aliases []string, short, kind string,
	cols []format.Column, mapper func(T) (map[string]string, map[string]any),
) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:     use + " [name]",
		Aliases: aliases,
		Short:   short,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o, err := runOrchestrator(cmdContext(cmd), *c, *h)
			if err != nil {
				return err
			}
			sel := selector.Metadata{
				Name:   firstArg(args),
				Labels: c.labels,
			}
			return printResources(cmd.OutOrStdout(), o, sel, c, c.output, kind, cols, mapper)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

func printResources[T manifest.BaseManifest](
	w io.Writer, o *orchestrator.Orchestrator, sel selector.Metadata, c *commonFlags,
	out, kind string,
	cols []format.Column, mapper func(T) (map[string]string, map[string]any),
) error {
	objs := o.Store().ListObjects(kind)
	rows := make([]map[string]string, 0, len(objs))
	docs := make([]map[string]any, 0, len(objs))
	for _, obj := range objs {
		if !sel.Matches(obj) {
			continue
		}
		id := obj.Named()
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		t, ok := obj.(T)
		if !ok {
			continue
		}
		row, doc := mapper(t)
		rows = append(rows, row)
		docs = append(docs, doc)
	}
	sortRows(rows)

	switch format.Output(out) {
	case format.OutputYAML:
		return format.YAMLMulti(w, docs)
	case format.OutputJSON:
		return format.JSON(w, docs)
	case format.OutputName:
		return format.Name(w, rows, "name")
	}
	return format.Table(w, cols, rows)
}

var (
	ksColumns = []format.Column{
		{Header: "NAMESPACE", Key: "namespace"},
		{Header: "NAME", Key: "name"},
		{Header: "PATH", Key: "path"},
	}
	hrColumns = []format.Column{
		{Header: "NAMESPACE", Key: "namespace"},
		{Header: "NAME", Key: "name"},
		{Header: "CHART", Key: "chart"},
		{Header: "VERSION", Key: "version"},
		{Header: "SOURCE", Key: "source"},
	}
)

func printCluster(w io.Writer, o *orchestrator.Orchestrator, out string) error {
	summary := map[string]any{
		"kustomizations": len(o.Store().ListObjects(manifest.KindKustomization)),
		"helmReleases":   len(o.Store().ListObjects(manifest.KindHelmRelease)),
	}
	if format.Output(out) == format.OutputJSON {
		return format.JSON(w, summary)
	}
	return format.YAML(w, summary)
}

