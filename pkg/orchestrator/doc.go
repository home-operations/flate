// Package orchestrator wires the controllers together and runs the
// reconcile loop. It owns:
//
//   - A central Store.
//   - A task.Service tracking active reconciliations.
//   - The Source, Kustomization, and HelmRelease controllers.
//   - The loader that primes the Store with on-disk objects.
//
// The lifecycle is:
//
//	o, err := orchestrator.New(ctx, orchestrator.Config{...})
//	if err := o.Bootstrap(ctx); err != nil { ... }
//	if err := o.Run(ctx); err != nil { ... }       // blocks until done
//	o.Stop()
//
// Bootstrap loads YAML manifests from the configured path AND
// publishes a synthetic GitRepository pointing at the local working
// tree, so Kustomizations whose sourceRef resolves to the bootstrap
// repo can reconcile immediately.
package orchestrator
