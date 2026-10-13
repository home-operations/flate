package cli

import (
	"cmp"
	"errors"
	"io"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/internal/report"
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
	kinds := []string{manifest.KindKustomization, manifest.KindHelmRelease}
	cmd := buildCmd("build", nil, "Render Flux objects to YAML (defaults to all)", cobra.NoArgs, kinds...)
	cmd.AddCommand(
		buildCmd("ks [name]", []string{"kustomization", "kustomizations"},
			"Render Kustomizations", cobra.MaximumNArgs(1),
			manifest.KindKustomization),
		buildCmd("hr [name]", []string{"helmrelease", "helmreleases"},
			"Render HelmReleases", cobra.MaximumNArgs(1),
			manifest.KindHelmRelease),
		buildCmd("all", nil,
			"Render all Kustomization and HelmRelease objects", cobra.NoArgs, kinds...),
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
			name := firstArg(argv)
			o, res, runErr := runOrchestrator(cmdContext(cmd), *c, *h)
			selected, runErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr, c.roots(name, kinds...), nil)
			if len(selected) == 0 {
				return runErr
			}
			res = selected[0]
			var emitErr error
			if name != "" && res.Counts[kinds[0]] == 0 {
				emitErr = noNamedError(kinds[0], name)
			} else {
				emitErr = emitDocs(cmd.OutOrStdout(), collectRendered(res, b), format.Output(c.output))
			}
			return reportFailures(cmd.ErrOrStderr(), report.Build(res), name != "", reportErrors{
				selected: errors.Join(emitErr, runErr), remainder: errors.Join(emitErr, remainder),
			})
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

func collectRendered(res *orchestrator.Result, b *buildFlags) []map[string]any {
	out := []map[string]any{}
	for _, obj := range res.Objects {
		out = append(out, emissionDocs(res.Manifests[obj.Named()], b)...)
	}
	return out
}

// emissionDocs sorts a cloned batch without changing the selected snapshot.
func emissionDocs(mans []map[string]any, b *buildFlags) []map[string]any {
	docs := slices.Clone(mans)
	slices.SortStableFunc(docs, compareDocs)
	if b.onlyCRDs {
		return slices.DeleteFunc(docs, func(doc map[string]any) bool {
			return manifest.DocKind(doc) != manifest.KindCustomResourceDefinition
		})
	}
	return docs
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
