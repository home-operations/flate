package cli

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/kustomize"
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
			o, err := runOrchestrator(cmdContext(cmd), *c, *h)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			name := firstArg(argv)
			for _, kind := range kinds {
				if err := writeRendered(w, o, kind, name, c, b); err != nil {
					return err
				}
			}
			return nil
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
	var all []map[string]any
	for _, obj := range o.Store().ListObjects(kind) {
		id := obj.Named()
		if name != "" && id.Name != name {
			continue
		}
		if !c.includeNamespace(o.Filter(), id.Namespace) {
			continue
		}
		if a, ok := o.Store().GetArtifact(id).(store.RenderedArtifact); ok {
			all = append(all, a.RenderedManifests()...)
		}
	}
	if b.onlyCRDs {
		all = kustomize.FilterKinds(all, []string{manifest.KindCustomResourceDefinition})
	}
	return format.YAMLMulti(w, all)
}
