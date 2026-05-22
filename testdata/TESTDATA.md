# Testdata

This directory holds fixtures used by `pkg/` unit tests and the `test/e2e/`
integration suite.

## Layout

- `simple/` — a hand-crafted minimal cluster used by E2E tests. Exercises
  the Kustomization + HelmRelease pipeline end-to-end without any network
  access.
  - `cluster/` — Flux GitRepository, Kustomization, and HelmRelease objects.
  - `apps/` — kustomize-rendered ConfigMap + Namespace.
  - `charts/mychart/` — a tiny local Helm chart referenced via
    `sourceRef.kind: GitRepository`.

- `components/` — two consumer apps (`app-a`, `app-b`) referencing a shared
  kustomize component (`components/shared`). Exists to test change-detection
  propagation: editing the shared component must surface in both consumers'
  diff output (`TestE2E_ComponentChangePropagatesToAllConsumers`) and an
  app-local edit must NOT propagate to the sibling
  (`TestE2E_NonSharedChangeDoesNotPropagate`).

- `parent-patches/` — the stripped-down bjw-s home-ops cluster pattern.
  A top-level `cluster-apps` Flux Kustomization injects
  `postBuild.substituteFrom` via `spec.patches` into every child KS rendered
  from `./apps`. The fixture also ships a SOPS-encrypted Secret alongside the
  patched leaf to exercise the wipe-to-PLACEHOLDER behavior end-to-end. Used
  by `TestE2E_ParentPatchesPropagateToChildren`,
  `TestE2E_SOPSValueWipedToPlaceholder`, and
  `TestE2E_SubstituteDisabledAnnotation`.

- `orphans/` — a parent kustomize tree that lists only one child
  Kustomization (`wired`) while a second one (`orphan`) exists on disk but
  is intentionally NOT referenced. Real Flux would never reconcile the
  orphan; flate downgrades its failure to a warning and reports
  `resource orphaned`. Used by `TestE2E_OrphanedKustomizationIsWarning`.

## Upstream attribution

flate's controller architecture and behavior follow the design of
[`flux-local`](https://github.com/allenporter/flux-local). The flate
`testdata/simple` corpus is intentionally minimal and was authored from
scratch for the Go port; the larger `flux-local/tests/testdata/*` clusters
remain available in upstream for reference.
