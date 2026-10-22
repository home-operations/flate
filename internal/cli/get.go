package cli

import (
	"errors"
	"io"
	"maps"
	"slices"

	"github.com/spf13/cobra"

	"github.com/home-operations/flate/internal/output"
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
		mapper: func(res *orchestrator.Result, o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			source := o.ChartSource()
			chart := o.Chart
			if source.Name != "" {
				chart.RepoName, chart.RepoNamespace = source.Name, source.Namespace
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
			o, res, runErr := runOrchestrator(cmd.Context(), *c)
			selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots("", manifest.KindKustomization, manifest.KindHelmRelease), nil)
			if len(selected) == 0 {
				return runErr
			}
			return errors.Join(printCluster(cmd.OutOrStdout(), selected[0], c.output), runErr)
		},
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatYAML, output.FormatJSON)
	return cmd
}

// newGetImagesCmd emits a deduplicated list of container images
// extracted from every rendered Kustomization and HelmRelease — the
// symmetric counterpart of `flate diff images`, which emits the same
// shape filtered to images that actually changed between paths.
func newGetImagesCmd() *cobra.Command {
	c := &commonFlags{}
	cmd := &cobra.Command{
		Use:   "images",
		Short: "List container images across the cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			o, res, runErr := runOrchestrator(cmd.Context(), *c)
			selected, runErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr,
				c.roots("", manifest.KindKustomization, manifest.KindHelmRelease), nil)
			if len(selected) == 0 {
				return runErr
			}
			imgs := slices.Sorted(maps.Keys(collectImages(selected[0])))
			return errors.Join((output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).Images(imgs), runErr)
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
	mapper           func(*orchestrator.Result, T) (map[string]string, map[string]any)
}

func resourceListCmd[T manifest.BaseManifest](view listView[T]) *cobra.Command {
	c := &commonFlags{}
	l := &listFlags{}
	cmd := &cobra.Command{
		Use: view.use + " [name]", Aliases: view.aliases, Short: view.short,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o, res, runErr := runOrchestrator(cmd.Context(), *c)
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
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatTable, output.FormatYAML, output.FormatJSON, output.FormatName)
	bindSelector(cmd.Flags(), l)
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
	return (output.Encoder{Writer: w, Format: output.Format(out)}).Resources(view.cols, rows, docs)
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

func printCluster(w io.Writer, res *orchestrator.Result, out string) error {
	summary := map[string]any{
		"kustomizations": res.Counts[manifest.KindKustomization],
		"helmReleases":   res.Counts[manifest.KindHelmRelease],
	}
	return (output.Encoder{Writer: w, Format: output.Format(out)}).Cluster(summary)
}
