# Testdata

This directory holds fixtures used by `pkg/` unit tests and the `test/e2e/`
integration suite. Fixtures stay here while tests run in scenario packages beneath
`test/e2e/`, using one shared `test/e2e/internal/harness` package. Run all scenarios
with `go test -race ./test/e2e/...`.

| Package | Scenarios |
| --- | --- |
| `.` | ArtifactGenerator strategies, bootstrap inputs, partial failure and emitted changed-only diff |
| `sourceref` | Source references, pinned trees and discovery |
| `scope` | Selection, ownership, orphans, diff and change scope |
| `helm` | Helm listing, builds, diagnostics and stale values |
| `crds` | ResourceSet expansion and CRD availability |
| `substitution` | Substitution producers, data reading and inherited patches |

## Layout

- `simple/` - a hand-crafted minimal cluster used across the scenario packages.
  Exercises the Kustomization + HelmRelease pipeline end-to-end without any network
  access.
    - `cluster/` — Flux GitRepository, Kustomization, and HelmRelease objects.
    - `apps/` — kustomize-rendered ConfigMap + Namespace.
    - `charts/mychart/` — a tiny local Helm chart referenced via
      `sourceRef.kind: GitRepository`.

- `components/` — two consumer apps (`app-a`, `app-b`) referencing a shared
  kustomize component (`components/shared`). Exists to test change-detection
  propagation in `test/e2e/scope`: editing the shared component must surface in
  both consumers' diff output (`TestE2E_ComponentChangePropagatesToAllConsumers`) and an
  app-local edit must NOT propagate to the sibling
  (`TestE2E_NonSharedChangeDoesNotPropagate`).

- `parent-patches/` — the stripped-down bjw-s home-ops cluster pattern.
  A top-level `cluster-apps` Flux Kustomization injects
  `postBuild.substituteFrom` via `spec.patches` into every child KS rendered
  from `./apps`. The fixture also ships a SOPS-encrypted Secret alongside the
  patched leaf to exercise the wipe-to-PLACEHOLDER behavior end-to-end. Used
  by `test/e2e/substitution` tests `TestE2E_ParentPatchesPropagateToChildren`,
  `TestE2E_SOPSValueWipedToPlaceholder`, and
  `TestE2E_SubstituteDisabledAnnotation`.

- `orphans/` — a parent kustomize tree that lists only one child
  Kustomization (`wired`) while a second one (`orphan`) exists on disk but
  is intentionally NOT referenced. Real Flux would never reconcile the
  orphan; flate does not discover it. Used by
  `test/e2e/scope` test `TestE2E_OrphanedKustomizationNotDiscovered`.

- `resourceset-documents/` - ResourceSet JSON and YAML admission through SDK and CLI output: incomplete identities, nonrecursive list items, directives, duplicates, sanitized ciphertext, authored and explicit namespaces, and one tracked child render.

ArtifactGenerator CLI fixtures are created in temporary repositories by
`test/e2e/artifactgenerator_test.go`. `AllStrategies` combines nested directory
copying, YAML merging and a local gzip tar archive, then compares two CLI runs.
`PartialFailure` publishes outputs A and B before output C fails and checks that
their consumers still render. `BootstrapInput` selects a static generator when
only its consumer declaration changes. `BuiltBootstrapInput` patches a generator
copy input and UID in a bare Kustomize entry, then checks consumed output and
deterministic bytes. `EmittedChangedOnlyDiff` renders the generator from
a Component, changes its selected copy input, and checks both changed-only build
and forward/reverse diff with shallow and absent history on opposite sides,
without selecting an unrelated consumer. Each case runs with
two workers and compares two equal CLI runs. Consumed-input successor scheduling
remains deferred to X2 and is not asserted by these fixtures.

## Upstream attribution

flate's controller architecture and behavior follow the design of
[`flux-local`](https://github.com/allenporter/flux-local). The flate
`testdata/simple` corpus is intentionally minimal and was authored from
scratch for the Go port; the larger `flux-local/tests/testdata/*` clusters
remain available in upstream for reference.
