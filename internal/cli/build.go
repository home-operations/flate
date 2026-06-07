package cli

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
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
			stopProfile, err := startProfile(c.profileMode, c.profileOut)
			if err != nil {
				return err
			}
			defer stopProfile()
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			if o == nil {
				return runErr
			}
			name := firstArg(argv)
			docs := []map[string]any{}
			var emitErr error
			for _, kind := range kinds {
				rendered, err := collectRendered(o, res, kind, name, c, b)
				if err != nil {
					emitErr = err
					break
				}
				docs = append(docs, rendered...)
			}
			if emitErr == nil {
				emitErr = emitDocs(cmd.OutOrStdout(), docs, format.Output(c.output))
			}
			// Per-resource Run failures: emit whatever we rendered, then
			// flip the exit code so CI pipelines piping `flate build` into
			// kubectl apply don't silently apply a half-rendered tree.
			// errors.Join surfaces both an emit-time IO failure AND the
			// partial-failure list — previously the emit error masked
			// the run failures, so CI fixed the wrong thing.
			return errors.Join(emitErr, scopedRunError(o, res, c, runErr))
		},
	}
	bindCommon(cmd.Flags(), c, format.OutputYAML, format.OutputJSON)
	bindHelmFlags(cmd.Flags(), h)
	bindBuildFlags(cmd.Flags(), b)
	return cmd
}

func applyBuildFlags(c *commonFlags, b *buildFlags) {
	if b.onlyCRDs {
		c.skipCRDs = false
	}
}

func collectRendered(o *orchestrator.Orchestrator, res *orchestrator.Result, kind, name string, c *commonFlags, b *buildFlags) ([]map[string]any, error) {
	// Walk every loaded object of this kind so an explicit name positional
	// that didn't render (failed reconcile, suspended, no docs) still
	// counts as a match — without this the typo-detection error below
	// would also fire for failed-but-existing resources.
	// Store.ListObjects(kind) already returns results sorted by
	// (namespace, name) (store.go), so single-kind output is deterministic
	// across runs without a re-sort here.
	objs := o.Store().ListObjects(kind)
	skipKinds := c.skipResourceKinds()
	matched := 0
	var out []map[string]any
	for _, obj := range objs {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		matched++
		// A missing entry means the resource didn't render
		// (failed, suspended, or produced zero docs).
		mans, ok := res.Manifests[id]
		if !ok || len(mans) == 0 {
			continue
		}
		// Clone-and-sort per-artifact so output is byte-stable across
		// runs even when a Helm chart uses `range $name, $svc := .Values`
		// (Go map iteration is randomized — the chart still emits the
		// same set but in arbitrary order). Sort by (kind, ns, name).
		// SSA-applied output doesn't care about order; CI / diff
		// consumers do.
		docs := slices.Clone(mans)
		slices.SortStableFunc(docs, compareDocs)
		if b.onlyCRDs {
			docs = filterCRDsOnly(docs)
		} else {
			// Defensive re-drop. Orchestrator.Render already filters
			// Result.Manifests at the embed boundary using the same
			// kind set, so this is a no-op for the normal CLI path.
			docs = manifest.DropKinds(docs, skipKinds)
		}
		if len(docs) == 0 {
			continue
		}
		out = append(out, docs...)
	}
	// An explicit name positional that matches nothing in the store
	// should error rather than silently emit an empty render — a typo
	// shouldn't look like a successful build of a nonexistent resource.
	if name != "" && matched == 0 {
		return nil, fmt.Errorf("no %s named %q in --path", kind, name)
	}
	return out, nil
}

// emitDocs writes a sequence of rendered docs as either multi-doc YAML
// (the default for `flate build`) or a single JSON array. Other -o values
// are rejected at parse time by the -o flag.
func emitDocs(w io.Writer, docs []map[string]any, out format.Output) error {
	switch out {
	case format.OutputJSON:
		return format.JSON(w, docs)
	default:
		return format.YAMLMulti(w, docs)
	}
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
// manifest.DropKinds) to skip the slice-copy when nothing matches:
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
