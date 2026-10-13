package cli

import (
	"errors"
	"io"
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
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
		mapper: func(_ *orchestrator.Result, o *manifest.Kustomization) (row map[string]string, doc map[string]any) {
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
	})
}

func newGetHRCmd() *cobra.Command {
	return resourceListCmd(listView[*manifest.HelmRelease]{
		use: "hr", aliases: []string{"helmrelease", "helmreleases"},
		short: "List HelmReleases", kind: manifest.KindHelmRelease, cols: hrColumns,
		mapper: func(res *orchestrator.Result, o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			version := o.Chart.Version
			if version == "" {
				version = res.ChartVersions[o.Named()]
			}
			doc = map[string]any{
				"kind": manifest.KindHelmRelease, "namespace": o.Namespace,
				"name": o.Name, "chart": o.Chart.ChartName(),
				"version": version, "source": o.Chart.RepoName,
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
				"chart": o.Chart.ChartName(), "version": version,
				"source": o.Chart.RepoName,
			}, doc
		},
	})
}

func getAllCmd(use, short string) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots("", manifest.KindKustomization, manifest.KindHelmRelease), nil)
			if len(selected) == 0 {
				return runErr
			}
			return errors.Join(printCluster(cmd.OutOrStdout(), selected[0], c.output), runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON)
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
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots("", manifest.KindKustomization, manifest.KindHelmRelease), nil)
			if len(selected) == 0 {
				return runErr
			}
			imgs := slices.Sorted(maps.Keys(collectImages(selected[0])))
			return errors.Join(emitImageList(cmd.OutOrStdout(), imgs, c.output), runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputName, format.OutputYAML, format.OutputJSON)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

type listView[T manifest.BaseManifest] struct {
	use, short, kind string
	aliases          []string
	cols             []format.Column
	mapper           func(*orchestrator.Result, T) (map[string]string, map[string]any)
}

func resourceListCmd[T manifest.BaseManifest](view listView[T]) *cobra.Command {
	c := &commonFlags{}
	l := &listFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use: view.use + " [name]", Aliases: view.aliases, Short: view.short,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			name := firstArg(args)
			selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots(name, view.kind), l.labels)
			if len(selected) == 0 {
				return runErr
			}
			if name != "" && selected[0].Counts[view.kind] == 0 {
				return errors.Join(noNamedError(view.kind, name), runErr)
			}
			return errors.Join(printResources(cmd.OutOrStdout(), selected[0], c.output, view), runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputTable, format.OutputYAML, format.OutputJSON, format.OutputName)
	bindSelector(cmd.Flags(), l)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

func printResources[T manifest.BaseManifest](w io.Writer, res *orchestrator.Result, out string, view listView[T]) error {
	rows := make([]map[string]string, 0, len(res.Objects))
	docs := make([]map[string]any, 0, len(res.Objects))
	for _, obj := range res.Objects {
		t, ok := obj.(T)
		if !ok {
			continue
		}
		row, doc := view.mapper(res, t)
		rows = append(rows, row)
		docs = append(docs, doc)
	}
	switch format.Output(out) {
	case format.OutputYAML:
		return format.YAMLMulti(w, docs)
	case format.OutputJSON:
		return format.JSON(w, docs)
	case format.OutputName:
		return format.Name(w, rows, "name")
	}
	return format.Table(w, view.cols, rows)
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

func printCluster(w io.Writer, res *orchestrator.Result, out string) error {
	summary := map[string]any{
		"kustomizations": res.Counts[manifest.KindKustomization],
		"helmReleases":   res.Counts[manifest.KindHelmRelease],
	}
	if format.Output(out) == format.OutputJSON {
		return format.JSON(w, summary)
	}
	return format.YAML(w, summary)
}
