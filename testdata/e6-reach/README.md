# Synthetic reach fixtures

These small input graphs reproduce eight cold diff scenarios in `TestRenderTrees_Reach`.
Each request uses two workers, fresh memory caches, and no render history.
The fixtures contain no copied repository snapshots, rendered output, or captured charts.
The original application names remain fixed test identities.
Self URLs identify the local bootstrap tree and cause no external requests.

| Directory | Edits | Structural purpose |
| --- | --- | --- |
| `plex` | Plex timeout and interval | An opaque chart emits a FluxInstance that applies the entry directory. Its discovery admits the declaration catalog and renders eight prerequisite Kustomizations and nine prerequisite releases. Plex adds one Kustomization and one release. Empty sibling packages preserve excluded controller identities. |
| `kopiur` | Shared snapshot schedule | Seventeen Kustomizations read one component. Their unchanged release declarations cause no Helm templates. |
| `shared` | Shared autoscaler policy | Eight Kustomizations read one component. Their unchanged release declarations cause no Helm templates. |
| `generator` | Generator content and input rename | One Kustomization generates a ConfigMap and emits one unchanged release. The rename preserves the generated key and bytes. |
| `global` | `NFS_MUSIC` in a ConfigMap | One shared substitution object reaches 62 Kustomizations. Sixty releases and seven empty ResourceSets preserve the controller catalog. A suspended sibling is admitted without rendering. |
| `crd` | Multus CRD description | A thin ancestor admits 186 Kustomizations and one unchanged release. Only that ancestor and the CRD owner build. Empty sibling packages preserve excluded identities. |
| `charts/reach` | Local OCI chart | An in-process TLS registry serves a tiny chart. It emits a FluxInstance for the Plex prerequisite and a values ConfigMap for other releases. |

The global fixture uses an unchanged substitution value in every release.
This preserves the current broad render reach without changing its effective output.
Four releases require absent values Secrets and retain their exact failure identities.
Seven releases use `NFS_MUSIC` and render different values on both sides.
The other 49 successful releases share unchanged template work across the two trees.
The joint template total is 63, with 56 distinct successful owners.

## Frozen effective users

Each Kustomization below uses namespace `flux-system`.
The independent universal renders show that exactly these seven users change after the variable edit.
The assertion excludes the variable provider, `cluster-vars`.

| Kustomization | HelmRelease namespace/name |
| --- | --- |
| `apache-musicindex` | `apache-musicindex/apache-musicindex` |
| `jellyfin` | `jellyfin/jellyfin` |
| `music-assistant` | `music-assistant/music-assistant` |
| `nextcloud` | `nextcloud/nextcloud` |
| `vpn-lidarr` | `vpn/lidarr` |
| `vpn-prowlarr` | `vpn/prowlarr` |
| `vpn-qbittorrent` | `vpn/qbittorrent` |

## Output and lifetime assertions

The test runs independent universal builds before each measured diff.
A fixed list of parent identities selects the expected output.
Prerequisite parents expose control declarations, and the CRD ancestor exposes only the selected child declaration.
The test compares that projection against the complete target output.
It also asserts admissions, physical engine owners, completion counts, failure counts, and attribution to each tree.
A second cold request must produce identical bytes.

Every full build and request row runs in a parallel subtest.
The parent stays serial because it changes the process GC percentage and samples retained heap.
The dedicated `test/reach` package gives this sample its own process.
Other packages run in parallel with it.
After each group of eight requests, two GC cycles discard temporary pools.
All weak references to results, stores, and orchestrators must clear.
The second group retains at most the larger of 1 MiB or two percent above the first group.

Plex timeout, interval, and global variable reach record current behavior.
U1.2 owns their key, variable, and render-stage precision.
Real repository runs belong to the corpus and benchmark harness.
