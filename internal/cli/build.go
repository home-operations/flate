package cli

import (
	"cmp"
	"fmt"
	"io"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// buildFlags holds flags shared across `build ks`, `build hr`, and
// `build all` that aren't part of commonFlags.
type buildFlags struct {
	onlyCRDs bool
}

func bindBuildFlags(fs *pflag.FlagSet, b *buildFlags) {
	fs.BoolVar(&b.onlyCRDs, "only-crds", false,
		"emit only CustomResourceDefinition resources (implies --skip-crds=false)")
}

func newBuildCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Render Flux objects to YAML",
	}
	cmd.AddCommand(
		buildCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Render Kustomizations", cobra.MaximumNArgs(1),
			manifest.KindKustomization),
		buildCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Render HelmReleases", cobra.MaximumNArgs(1),
			manifest.KindHelmRelease),
		buildCmd("all", nil,
			"Render all Kustomization and HelmRelease objects", cobra.NoArgs,
			manifest.KindKustomization, manifest.KindHelmRelease),
	)
	return cmd
}

func buildCmd(use string, aliases []string, short string, args cobra.PositionalArgs, kinds ...string) *cobra.Command {
	c := &commonFlags{}
	h := &helmFlags{}
	b := &buildFlags{}
	cmd := &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Args:    args,
		RunE: func(cmd *cobra.Command, argv []string) error {
			applyBuildFlags(c, b)
			o, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			w := cmd.OutOrStdout()
			name := firstArg(argv)
			for _, kind := range kinds {
				if err := writeRendered(w, o, kind, name, c, b); err != nil {
					return err
				}
			}
			// Per-resource Run failures: emit whatever we rendered, then
			// flip the exit code so CI pipelines piping `flate build` into
			// kubectl apply don't silently apply a half-rendered tree.
			return runErr
		},
	}
	bindCommon(cmd.Flags(), c)
	bindHelmFlags(cmd.Flags(), h)
	bindBuildFlags(cmd.Flags(), b)
	return cmd
}

func applyBuildFlags(c *commonFlags, b *buildFlags) {
	if b.onlyCRDs {
		c.skipCRDs = false
	}
}

func writeRendered(w io.Writer, o *orchestrator.Orchestrator, kind, name string, c *commonFlags, b *buildFlags) error {
	// Sort by (namespace, name) so `build` output is deterministic
	// across runs — `Store.ListObjects` iterates the byName map and
	// would otherwise produce a different ordering each invocation,
	// breaking shell-piped diffs and CI consumers.
	objs := o.Store().ListObjects(kind)
	slices.SortFunc(objs, func(a, b manifest.BaseManifest) int {
		ai, bi := a.Named(), b.Named()
		return cmp.Or(cmp.Compare(ai.Namespace, bi.Namespace), cmp.Compare(ai.Name, bi.Name))
	})
	matched := 0
	for _, obj := range objs {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		matched++
		a, ok := o.Store().GetArtifact(id).(store.RenderedArtifact)
		if !ok {
			continue
		}
		// Stream per-artifact rather than accumulating every doc into a
		// single slice — keeps the working set small on big repos and
		// lets onlyCRDs filter run incrementally instead of building a
		// throw-away intermediate.
		//
		// Clone-and-sort per-artifact so output is byte-stable across
		// runs even when a Helm chart uses `range $name, $svc := .Values`
		// (Go map iteration is randomized — the chart still emits the
		// same set but in arbitrary order). Sort by (kind, ns, name).
		// SSA-applied output doesn't care about order; CI / diff
		// consumers do.
		docs := slices.Clone(a.RenderedManifests())
		slices.SortStableFunc(docs, compareDocs)
		if b.onlyCRDs {
			docs = filterCRDsOnly(docs)
			if len(docs) == 0 {
				continue
			}
		}
		if err := format.YAMLMulti(w, docs); err != nil {
			return err
		}
	}
	// An explicit name positional that matches nothing in the store
	// should error rather than silently emit an empty render — a typo
	// shouldn't look like a successful build of a nonexistent resource.
	if name != "" && matched == 0 {
		return fmt.Errorf("no %s named %q in --path", kind, name)
	}
	return nil
}

// compareDocs orders rendered docs by (kind, namespace, name).
func compareDocs(a, b map[string]any) int {
	an, ans := manifest.DocMetadata(a)
	bn, bns := manifest.DocMetadata(b)
	return cmp.Or(
		cmp.Compare(manifest.DocKind(a), manifest.DocKind(b)),
		cmp.Compare(ans, bns),
		cmp.Compare(an, bn),
	)
}

// filterCRDsOnly returns the subset of docs whose `kind` is
// CustomResourceDefinition. Inlined here (rather than via
// kustomize.FilterKinds) to skip the slice-copy when nothing matches:
// most rendered artifacts contain zero CRDs, so the common case is to
// return a length-0 slice without allocation.
func filterCRDsOnly(docs []map[string]any) []map[string]any {
	var out []map[string]any
	for _, doc := range docs {
		if manifest.DocKind(doc) == manifest.KindCustomResourceDefinition {
			out = append(out, doc)
		}
	}
	return out
}
