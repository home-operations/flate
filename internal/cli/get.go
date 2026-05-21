package cli

import (
	"cmp"
	"io"
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/image"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/selector"
	"github.com/home-operations/flate/pkg/store"
)

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "get", Short: "List Flux objects"}
	cmd.AddCommand(newGetKSCmd(), newGetHRCmd(), newGetAllCmd())
	return cmd
}

func newGetKSCmd() *cobra.Command {
	return resourceListCmd("ks", []string{"kustomization", "kustomizations"},
		"List Kustomizations", manifest.KindKustomization, ksColumns,
		func(o *manifest.Kustomization) (row map[string]string, doc map[string]any) {
			return map[string]string{
					"namespace": o.Namespace, "name": o.Name, "path": o.Path,
				},
				map[string]any{
					"kind": manifest.KindKustomization, "namespace": o.Namespace,
					"name": o.Name, "path": o.Path,
				}
		},
	)
}

func newGetHRCmd() *cobra.Command {
	return resourceListCmd("hr", []string{"helmrelease", "helmreleases"},
		"List HelmReleases", manifest.KindHelmRelease, hrColumns,
		func(o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			return map[string]string{
					"namespace": o.Namespace, "name": o.Name,
					"chart": o.Chart.ChartName(), "version": o.Chart.Version,
					"source": o.Chart.RepoName,
				},
				map[string]any{
					"kind": manifest.KindHelmRelease, "namespace": o.Namespace,
					"name": o.Name, "chart": o.Chart.ChartName(),
					"version": o.Chart.Version, "source": o.Chart.RepoName,
				}
		})
}

func newGetAllCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	var enableImages, onlyImages bool
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Summarize every Kustomization and HelmRelease",
		RunE: func(cmd *cobra.Command, _ []string) error {
			o, err := runOrchestrator(cmdContext(cmd), *c, *h)
			if err != nil && o == nil {
				return err
			}
			w, closeFn, err := c.resolveWriter(cmd.OutOrStdout())
			if err != nil {
				return err
			}
			defer func() { _ = closeFn() }()
			if enableImages || onlyImages {
				return printClusterImages(w, o, c, onlyImages)
			}
			return printCluster(w, o, c.output)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	cmd.Flags().BoolVar(&enableImages, "enable-images", false, "include container images in the output")
	cmd.Flags().BoolVar(&onlyImages, "only-images", false, "emit only the deduplicated list of images")
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
			if err != nil && o == nil {
				return err
			}
			sel := selector.Metadata{
				Name:          firstArg(args),
				AllNamespaces: true, // namespace scope handled via c.includeNamespace
				Labels:        c.labels,
			}
			w, closeFn, err := c.resolveWriter(cmd.OutOrStdout())
			if err != nil {
				return err
			}
			defer func() { _ = closeFn() }()
			return printResources(w, o, sel, c, c.output, kind, cols, mapper)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

// printResources collects, sorts, and emits a typed resource list in
// the user-selected format.
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

type imageEntry struct {
	HelmRelease string   `json:"helmRelease" yaml:"helmRelease"`
	Images      []string `json:"images"      yaml:"images"`
}

func printClusterImages(w io.Writer, o *orchestrator.Orchestrator, c *commonFlags, onlyImages bool) error {
	if onlyImages {
		return emitImageList(w, slices.Sorted(maps.Keys(collectImages(o, c))), c.output)
	}

	var releases []imageEntry
	for _, obj := range o.Store().ListObjects(manifest.KindHelmRelease) {
		hr := obj.(*manifest.HelmRelease)
		if !c.includeNamespace(o.Filter(), hr.Namespace) {
			continue
		}
		art, ok := o.Store().GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
		if !ok {
			continue
		}
		set := map[string]struct{}{}
		for _, doc := range art.Manifests {
			imgs, _ := image.Extract(doc)
			for _, img := range imgs {
				set[img] = struct{}{}
			}
		}
		if len(set) == 0 {
			continue
		}
		releases = append(releases, imageEntry{
			HelmRelease: hr.NamespacedName(),
			Images:      slices.Sorted(maps.Keys(set)),
		})
	}
	slices.SortFunc(releases, func(a, b imageEntry) int { return cmp.Compare(a.HelmRelease, b.HelmRelease) })

	if format.Output(c.output) == format.OutputJSON {
		return format.JSON(w, releases)
	}
	return format.YAML(w, releases)
}
