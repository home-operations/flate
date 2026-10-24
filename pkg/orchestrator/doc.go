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
//	if err := o.Stop(); err != nil { ... }
//
// Run and Render join remote workers before releasing internally owned backing.
// Successful Bootstrap without Run requires Stop. Failed Bootstrap is terminal;
// repeat calls retain its combined error and recovery requires a fresh New.
// Stop MUST follow completion of active renderer calls; cancel their context to
// abort them. Stores remain readable after Stop. Injected Cache remains owned by
// the caller, who closes it only after every borrowing run and reader finishes.
//
// Direct renderer callers finish and join Render calls, close each borrowed
// TreeCache, close its source.HTTPInputs, close remaining readers, then release their cache.New share and
// handle cache.Close's error. TreeCache.Close does not close borrowed backing.
//
// Bootstrap loads YAML manifests from the configured path AND
// publishes a synthetic GitRepository pointing at the local working
// tree, so Kustomizations whose sourceRef resolves to the bootstrap
// repo can reconcile immediately.
package orchestrator
