package cli

import (
	"errors"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

// buildFlags holds flags shared across `build ks`, `build hr`, and
// `build all` that aren't part of commonFlags.
type buildFlags struct {
	onlyKinds []string
}

func bindBuildFlags(fs *pflag.FlagSet, b *buildFlags) {
	fs.StringSliceVar(&b.onlyKinds, "only-kinds", nil, "emit only these kinds (empty includes all kinds)")
}

func newBuildCmd() *cobra.Command {
	kinds := []string{manifest.KindKustomization, manifest.KindHelmRelease}
	cmd := buildCmd(&cobra.Command{
		Use:   "build",
		Short: "Render Flux objects to YAML (defaults to all)",
		Args:  cobra.NoArgs,
	}, kinds...)
	cmd.AddCommand(
		buildCmd(&cobra.Command{
			Use:     "ks [name]",
			Aliases: []string{"kustomization", "kustomizations"},
			Short:   "Render Kustomizations",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindKustomization),
		buildCmd(&cobra.Command{
			Use:     "hr [name]",
			Aliases: []string{"helmrelease", "helmreleases"},
			Short:   "Render HelmReleases",
			Args:    cobra.MaximumNArgs(1),
		}, manifest.KindHelmRelease),
		buildCmd(&cobra.Command{
			Use:   "all",
			Short: "Render all Kustomization and HelmRelease objects",
			Args:  cobra.NoArgs,
		}, kinds...),
	)
	return cmd
}

func buildCmd(cmd *cobra.Command, kinds ...string) *cobra.Command {
	c := &commonFlags{}
	b := &buildFlags{}
	cmd.RunE = func(cmd *cobra.Command, argv []string) error {
		if !cmd.Flags().Changed("skip-kinds") {
			c.skipKinds = slices.DeleteFunc(c.skipKinds, func(kind string) bool {
				return slices.Contains(b.onlyKinds, kind)
			})
		}
		stopProfile, err := startProfile()
		if err != nil {
			return err
		}
		defer stopProfile()
		name := firstArg(argv)
		o, res, runErr := runOrchestrator(cmd.Context(), *c)
		selected, runErr, remainder := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: res, Err: runErr}}, runErr, c.roots(name, kinds...), nil)
		if len(selected) == 0 {
			return runErr
		}
		res = selected[0]
		var emitErr error
		if name != "" && res.Counts[kinds[0]] == 0 {
			emitErr = noNamedError(kinds[0], name)
		} else {
			emitErr = (output.Encoder{Writer: cmd.OutOrStdout(), Format: output.Format(c.output)}).Documents(collectRendered(res, b))
		}
		return reportFailures(cmd.ErrOrStderr(), failureModel(res), name != "", reportErrors{
			selected: errors.Join(emitErr, runErr), remainder: errors.Join(emitErr, remainder),
		})
	}
	bindCommon(cmd.Flags(), c)
	bindOutput(cmd.Flags(), &c.output, output.FormatYAML, output.FormatJSON)
	bindBuildFlags(cmd.Flags(), b)
	return cmd
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
	if len(b.onlyKinds) > 0 {
		return slices.DeleteFunc(docs, func(doc map[string]any) bool {
			return !slices.Contains(b.onlyKinds, manifest.DocKind(doc))
		})
	}
	return docs
}

// compareDocs orders rendered docs by canonical resource identity.
func compareDocs(a, b map[string]any) int {
	an, ans := manifest.DocMetadata(a)
	bn, bns := manifest.DocMetadata(b)
	aid := (&manifest.RawObject{APIVersion: manifest.DocAPIVersion(a), Kind: manifest.DocKind(a), Namespace: ans, Name: an}).Named()
	bid := (&manifest.RawObject{APIVersion: manifest.DocAPIVersion(b), Kind: manifest.DocKind(b), Namespace: bns, Name: bn}).Named()
	return aid.Compare(bid)
}
