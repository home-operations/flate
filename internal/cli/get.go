package cli

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"

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
		func(_ *orchestrator.Orchestrator, o *manifest.Kustomization) (row map[string]string, doc map[string]any) {
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
		func(orch *orchestrator.Orchestrator, o *manifest.HelmRelease) (row map[string]string, doc map[string]any) {
			version := o.Chart.Version
			// chartRef HRs leave hr.Chart.Version empty — the version is
			// pinned on the referenced OCIRepository (ref.digest,
			// ref.semver, or ref.tag) or
			// HelmChart CRD (spec.version). Surface that for display
			// instead of an empty column.
			if version == "" {
				version = resolveChartRefVersion(orch, o)
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
		})
}

// resolveChartRefVersion looks up the version pinned on the source CR
// that hr.Chart references. For OCIRepository the source's
// spec.ref digest, semver, or tag is the version; for the HelmChart
// CRD the version field is part of the CRD spec.
func resolveChartRefVersion(orch *orchestrator.Orchestrator, hr *manifest.HelmRelease) string {
	srcID := manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
	obj := orch.Store().GetObject(srcID)
	switch s := obj.(type) {
	case *manifest.OCIRepository:
		if s.Reference != nil {
			return cmp.Or(s.Reference.Digest, s.Reference.SemVer, s.Reference.Tag)
		}
	case *manifest.HelmChartSource:
		return s.Version
	}
	return ""
}

func newGetAllCmd() *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Summarize every Kustomization and HelmRelease",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// printCluster emits a key/value summary; yaml/json/markdown
			// shape it. Reject `-o name` (which printCluster used to
			// silently coerce to yaml) for parity with build/diff.
			if err := c.requireOutput(format.OutputYAML, format.OutputJSON, format.OutputMarkdown); err != nil {
				return err
			}
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			if err := printCluster(cmd.OutOrStdout(), o, c, string(c.outputOrDefault(format.OutputYAML))); err != nil {
				return errors.Join(err, scopedRunError(o, res, c, runErr))
			}
			return scopedRunError(o, res, c, runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON, format.OutputMarkdown)
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
			// Mirrors `diff images`: name is the natural default
			// (one image per line); yaml/json/markdown are the
			// structured alternatives. Reject anything else loudly.
			if err := c.requireOutput(format.OutputYAML, format.OutputJSON, format.OutputName, format.OutputMarkdown); err != nil {
				return err
			}
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			imgs := slices.Sorted(maps.Keys(collectImages(o, res, c)))
			if err := emitImageList(cmd.OutOrStdout(), imgs, string(c.outputOrDefault(format.OutputName))); err != nil {
				return errors.Join(err, scopedRunError(o, res, c, runErr))
			}
			return scopedRunError(o, res, c, runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON, format.OutputName, format.OutputMarkdown)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

// resourceListCmd builds a `get <kind>` subcommand. mapper converts each
// stored object to (table row, structured doc); cols is the table
// schema. The orchestrator is threaded into mapper so display logic
// can resolve cross-references (e.g. HR chartRef → OCIRepository tag).
func resourceListCmd[T manifest.BaseManifest](
	use string, aliases []string, short, kind string,
	cols []format.Column, mapper func(*orchestrator.Orchestrator, T) (map[string]string, map[string]any),
) *cobra.Command {
	c := &commonFlags{}
	l := &listFlags{}
	h := &helmFlags{}
	cmd := &cobra.Command{
		Use:     use + " [name]",
		Aliases: aliases,
		Short:   short,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			sel := selector.Metadata{
				Name:   firstArg(args),
				Labels: l.labels,
			}
			if err := printResources(cmd.OutOrStdout(), o, sel, c, c.output, kind, cols, mapper); err != nil {
				return errors.Join(err, scopedRunError(o, res, c, runErr))
			}
			return scopedRunError(o, res, c, runErr)
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON, format.OutputName, format.OutputMarkdown)
	bindSelector(cmd.Flags(), l)
	bindHelmFlags(cmd.Flags(), h)
	return cmd
}

func printResources[T manifest.BaseManifest](
	w io.Writer, o *orchestrator.Orchestrator, sel selector.Metadata, c *commonFlags,
	out, kind string,
	cols []format.Column, mapper func(*orchestrator.Orchestrator, T) (map[string]string, map[string]any),
) error {
	objs := o.Store().ListObjects(kind)
	type pair struct {
		row map[string]string
		doc map[string]any
	}
	pairs := make([]pair, 0, len(objs))
	nameExists := sel.Name == ""
	for _, obj := range objs {
		if sel.Name != "" && obj.Named().Name != sel.Name {
			continue
		}
		id := obj.Named()
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		nameExists = true
		if !(selector.Metadata{Labels: sel.Labels}).Matches(obj) {
			continue
		}
		t, ok := obj.(T)
		if !ok {
			continue
		}
		row, doc := mapper(o, t)
		pairs = append(pairs, pair{row, doc})
	}
	if !nameExists {
		return fmt.Errorf("no %s named %q in --path", kind, sel.Name)
	}
	// Store.ListObjects iterates a Go map (random order); sort the
	// (row, doc) tuple so every output flavor — including yaml/json —
	// is deterministic across runs.
	slices.SortFunc(pairs, func(a, b pair) int {
		return cmp.Or(
			cmp.Compare(a.row["namespace"], b.row["namespace"]),
			cmp.Compare(a.row["name"], b.row["name"]),
		)
	})
	rows := make([]map[string]string, len(pairs))
	docs := make([]map[string]any, len(pairs))
	for i, p := range pairs {
		rows[i] = p.row
		docs[i] = p.doc
	}

	switch format.Output(out) {
	case format.OutputYAML:
		return format.YAMLMulti(w, docs)
	case format.OutputJSON:
		return format.JSON(w, docs)
	case format.OutputName:
		return format.Name(w, rows, "name")
	case format.OutputMarkdown:
		return format.MarkdownTable(w, cols, rows)
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

func printCluster(w io.Writer, o *orchestrator.Orchestrator, c *commonFlags, out string) error {
	ksCount := countObjects(o, c, manifest.KindKustomization)
	hrCount := countObjects(o, c, manifest.KindHelmRelease)
	summary := map[string]any{
		"kustomizations": ksCount,
		"helmReleases":   hrCount,
	}
	switch format.Output(out) {
	case format.OutputJSON:
		return format.JSON(w, summary)
	case format.OutputMarkdown:
		// Two-row pipe table fronted by an H2 — slots into PR
		// comments and step summaries without further massaging.
		if _, err := io.WriteString(w, "## Cluster summary\n\n"); err != nil {
			return err
		}
		return format.MarkdownTable(w,
			[]format.Column{
				{Header: "Resource", Key: "resource"},
				{Header: "Count", Key: "count"},
			},
			[]map[string]string{
				{"resource": "Kustomizations", "count": strconv.Itoa(ksCount)},
				{"resource": "HelmReleases", "count": strconv.Itoa(hrCount)},
			},
		)
	}
	return format.YAML(w, summary)
}

func countObjects(o *orchestrator.Orchestrator, c *commonFlags, kind string) int {
	count := 0
	for _, obj := range o.Store().ListObjects(kind) {
		if c == nil || c.includeNamespace(o.Filter(), obj.Named().Namespace) {
			count++
		}
	}
	return count
}
