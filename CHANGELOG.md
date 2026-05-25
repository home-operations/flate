# Changelog

## [0.1.21](https://github.com/home-operations/flate/compare/0.1.20...0.1.21) (2026-05-25)


### Features

* **diff:** auto-detect baseline via git so --path-orig is optional ([#349](https://github.com/home-operations/flate/issues/349)) ([9f9cef6](https://github.com/home-operations/flate/commit/9f9cef6d3a45918a2b093c5804cb6ac34376f85d))
* **orchestrator,helm:** widen and surface the EnableOCI=false warnings ([#327](https://github.com/home-operations/flate/issues/327)) ([2e4fe7b](https://github.com/home-operations/flate/commit/2e4fe7b0c6cb1080af43634faaf12192e63ea91a))
* **orchestrator:** warn at bootstrap on OCI HelmRepository + SecretRef ([#329](https://github.com/home-operations/flate/issues/329)) ([2500e18](https://github.com/home-operations/flate/commit/2500e188b8c049e5cdb9f1fa9a3b302fe5bb53b6))


### Bug Fixes

* **cleanup:** bucket parallel + replaceValueAtPath panic + small fixes ([#340](https://github.com/home-operations/flate/issues/340)) ([cde049e](https://github.com/home-operations/flate/commit/cde049e4f049be3f7111cad4858eac7e9c3c75d0))
* **deferred:** six small bugs + fragility traps from the audit ([#341](https://github.com/home-operations/flate/issues/341)) ([9723540](https://github.com/home-operations/flate/commit/97235408f8748c8555b1091db0828140cb0e66cd))
* **depwait,task:** event-driven quiescence + missed-wake + cache + stack ([#333](https://github.com/home-operations/flate/issues/333)) ([2d59c7c](https://github.com/home-operations/flate/commit/2d59c7c59451924366e3f1f43ab39e844f46a91f))
* **kustomize:** yaml.Node edit + stage-root lock + cancel-safe FetchRemote ([#334](https://github.com/home-operations/flate/issues/334)) ([60044f4](https://github.com/home-operations/flate/commit/60044f4cf4fbaf7e36b6edd14b4e35bb129f9f07))
* **loader:** graph-driven walk skips orphan subtrees, not just same-dir orphans ([#347](https://github.com/home-operations/flate/issues/347)) ([9314106](https://github.com/home-operations/flate/commit/93141067db765733a810624ed91c11129a5335ed))
* **loader:** skip orphan YAMLs not referenced by their parent kustomization.yaml ([#346](https://github.com/home-operations/flate/issues/346)) ([09d10ed](https://github.com/home-operations/flate/commit/09d10ede7632e87d845032f6613a93aa7958712d))
* **orchestrator,cli:** ctx-aware drain + emit/run error join + flag validation ([#339](https://github.com/home-operations/flate/issues/339)) ([10ef9ae](https://github.com/home-operations/flate/commit/10ef9ae979cb8a7ed5c0ad3183b706fe64f9854a))
* **orchestrator:** widen change.Detect to per-side .git root for sibling-checkout diffs ([#348](https://github.com/home-operations/flate/issues/348)) ([522dfe1](https://github.com/home-operations/flate/commit/522dfe197ec759b6d9d8a11a05c4358e12f65f2f))
* **source/bucket:** reset slot before walk to avoid ghost files ([#324](https://github.com/home-operations/flate/issues/324)) ([24ed0a0](https://github.com/home-operations/flate/commit/24ed0a07684e167e544d593778036c96c2435b5f))
* **source/oci,helm:** offline-safe cosign verify + chart cache invalidation ([#336](https://github.com/home-operations/flate/issues/336)) ([2f91d24](https://github.com/home-operations/flate/commit/2f91d240fba092cc7bfb2e1cd72d9ed361c4a749))
* **source/oci:** atomic .flate-digest write + format validation + staged-layer sentinel ([#326](https://github.com/home-operations/flate/issues/326)) ([768ff9d](https://github.com/home-operations/flate/commit/768ff9d682a5e26fab120ac2cf367a35cc630b2f))
* **source/oci:** reset cache slot when leftover OCI layout artifacts found ([#322](https://github.com/home-operations/flate/issues/322)) ([9737dfc](https://github.com/home-operations/flate/commit/9737dfc58c3cce69d52f9ac8b531dae8d889e2cf))
* **source/oci:** tighten state machine — drop ref.Digest fallback, defer reset, cleanup zero-layer ([#325](https://github.com/home-operations/flate/issues/325)) ([cd3a87a](https://github.com/home-operations/flate/commit/cd3a87adf8ba5579c78dea79ecf7b08fdab4ab12))
* **store:** close listener-race + phantom-failure + index-drift gaps ([#331](https://github.com/home-operations/flate/issues/331)) ([1187bde](https://github.com/home-operations/flate/commit/1187bde8b36e0c04b7e00163cf09eb445bd22482))


### Performance Improvements

* **change,loader:** git-aware Detect + same-size hashing + components fold ([#332](https://github.com/home-operations/flate/issues/332)) ([3ba52d2](https://github.com/home-operations/flate/commit/3ba52d23a55f3a9e2b37b017ddbdd5e61b7d9ec1))
* **change,values:** memoize ownership lookups + in-place values merge ([#338](https://github.com/home-operations/flate/issues/338)) ([0aeee7c](https://github.com/home-operations/flate/commit/0aeee7c072e89d7f9e37e55976984522569204bc))
* **discovery,orchestrator:** skip converged RSes + parallel post-run RS render ([#343](https://github.com/home-operations/flate/issues/343)) ([ff1bee0](https://github.com/home-operations/flate/commit/ff1bee03b1bd27fe7ae077ee0be8a10e1bd53e2d))


### Code Refactoring

* **controllers:** base.Controller.Await + nil-safe filter + status preservation ([#337](https://github.com/home-operations/flate/issues/337)) ([532bce5](https://github.com/home-operations/flate/commit/532bce55e53f0ed9da8909dfc62cbf214cbfc99f))
* **helm:** unify OCI pulls through source/oci.Fetcher (the big lift) ([#345](https://github.com/home-operations/flate/issues/345)) ([f9ab065](https://github.com/home-operations/flate/commit/f9ab065cbbe813ca50df7e855bc7d83eebf01648))
* **manifest:** single-source helpers for RepoName, NamespacedName, secret-ref validation ([#335](https://github.com/home-operations/flate/issues/335)) ([c408973](https://github.com/home-operations/flate/commit/c408973e6c4802c1a37ffe922296db521a52048b))
* **source/cache:** atomic-rename slot staging ([#330](https://github.com/home-operations/flate/issues/330)) ([3d9ed6e](https://github.com/home-operations/flate/commit/3d9ed6e09bbccce11269b4e8b0e7150e0cfd948e))
* **source/cache:** slugify by repo name, not by tag ([#328](https://github.com/home-operations/flate/issues/328)) ([1a460fd](https://github.com/home-operations/flate/commit/1a460fdaf02a2d97dd0cfa4580117c115651f432))
* **source:** generic TypedFetcher[T] + Wrap adapter ([#344](https://github.com/home-operations/flate/issues/344)) ([1e61bc1](https://github.com/home-operations/flate/commit/1e61bc1207c6983141e9d9c4da679d234a5228da))

## [0.1.20](https://github.com/home-operations/flate/compare/0.1.19...0.1.20) (2026-05-25)


### Features

* **test:** hide SKIPPED rows by default; --show-skipped opts in ([#321](https://github.com/home-operations/flate/issues/321)) ([fde11d7](https://github.com/home-operations/flate/commit/fde11d737b5fd274436d326179a5b70b90218c97))


### Bug Fixes

* add auto-completions for brew ([aa52b9a](https://github.com/home-operations/flate/commit/aa52b9a17e3c8b7d314fd54cb4cbbac3e3edb895))

## [0.1.19](https://github.com/home-operations/flate/compare/0.1.18...0.1.19) (2026-05-25)


### Features

* **depwait:** add RenderInflight quiescence signal for step-2 fail-fast ([#319](https://github.com/home-operations/flate/issues/319)) ([10bf86e](https://github.com/home-operations/flate/commit/10bf86efef77d38587c0f6ec934f16b4d2d670ee))


### Bug Fixes

* **change:** close AddEmitted race + symmetric BFS upgrade + cascade depth test ([#312](https://github.com/home-operations/flate/issues/312)) ([41d025e](https://github.com/home-operations/flate/commit/41d025e2c82449b27ce67d1e842932523ebb7b4c))
* **change:** stop keep-set cascade through ancestor-only renders ([#308](https://github.com/home-operations/flate/issues/308)) ([76e065a](https://github.com/home-operations/flate/commit/76e065a14840458b25744863d65e3b84e1286114))
* **depwait:** cap step-2 render-only wait at RenderProducingTimeout ([#313](https://github.com/home-operations/flate/issues/313)) ([955b5bd](https://github.com/home-operations/flate/commit/955b5bde1a6f749596b08f7e7130793e41e5f786))
* **depwait:** route step-2 long-wait error through classify + missing tests ([#311](https://github.com/home-operations/flate/issues/311)) ([ac04f29](https://github.com/home-operations/flate/commit/ac04f2908e9fb05edb9e207778741cc5421143dc))
* **depwait:** wait beyond grace for render-only deps still in flight ([#310](https://github.com/home-operations/flate/issues/310)) ([00636bb](https://github.com/home-operations/flate/commit/00636bb553689d4fa36531b1cfa156aa54be0b19))


### Documentation

* **change:** sync filter doc comments with AddEmitted-based runtime API ([#315](https://github.com/home-operations/flate/issues/315)) ([4332576](https://github.com/home-operations/flate/commit/433257605546146a4db4896d0bf2638941685a0b))


### Code Refactoring

* **change:** unexport Filter.Add → addUngated ([#317](https://github.com/home-operations/flate/issues/317)) ([478307b](https://github.com/home-operations/flate/commit/478307b528c3ca9e9a28eada3f1dd1ab29283aad))
* **depwait:** bundle existence closures into ExistenceLookup interface ([#316](https://github.com/home-operations/flate/issues/316)) ([e0329fe](https://github.com/home-operations/flate/commit/e0329fe9c29d106c42942f5903deef3e09c403a8))
* **depwait:** rename IsKnown → IsFileIndexed for clarity ([#314](https://github.com/home-operations/flate/issues/314)) ([429a526](https://github.com/home-operations/flate/commit/429a526ec4aa5a4b5b88589836abb05d1ad68a15))

## [0.1.18](https://github.com/home-operations/flate/compare/0.1.17...0.1.18) (2026-05-25)


### Features

* **helm:** route OCI chart pulls through source.oci.Fetcher ([#269](https://github.com/home-operations/flate/issues/269)) ([be0feca](https://github.com/home-operations/flate/commit/be0fecaaa56c0ec287695a3158b536da6e4a3090))


### Bug Fixes

* **change:** Filter.Add walks transitiveDeps + refires source listeners ([#260](https://github.com/home-operations/flate/issues/260)) ([#261](https://github.com/home-operations/flate/issues/261)) ([eaca342](https://github.com/home-operations/flate/commit/eaca3422bfc70f1d260c6ed785ecb2eb5d522079))
* **discovery:** alias bootstrap OCIRepository sources too ([#263](https://github.com/home-operations/flate/issues/263)) ([0ac5ffe](https://github.com/home-operations/flate/commit/0ac5ffe07afe5814c3f495d913364a4ef56f93b4))
* **orchestrator:** assert WithFetcher pre-Bootstrap + warn on ignored OCI features ([#276](https://github.com/home-operations/flate/issues/276)) ([139bf0f](https://github.com/home-operations/flate/commit/139bf0ff3946990a18ecf3db1e0f9d67c6b4dd99))
* **source/oci:** write blobs without title annotation to slot ([#265](https://github.com/home-operations/flate/issues/265)) ([c395f08](https://github.com/home-operations/flate/commit/c395f08676afef8987a52b65e09fa3a6de018be5))
* **store:** close AddListener double-fire race + harden Refire/Snapshot ([#274](https://github.com/home-operations/flate/issues/274)) ([c814820](https://github.com/home-operations/flate/commit/c814820d381f7fd0097cf6e4df35bb3719c0742a))


### Miscellaneous Chores

* **diff:** default-strip checksum/secret annotation ([#264](https://github.com/home-operations/flate/issues/264)) ([8549315](https://github.com/home-operations/flate/commit/85493158b15f7fab6b872708398e2612f75ddf54))
* use existing error everywhere ([#307](https://github.com/home-operations/flate/issues/307)) ([6fc0e7b](https://github.com/home-operations/flate/commit/6fc0e7bb100921ff6cdd1aa728eddba3f725f542))


### Code Refactoring

* adopt modern stdlib helpers (maps.Copy, bytes.SplitSeq, errors.AsType) ([#299](https://github.com/home-operations/flate/issues/299)) ([ae68e91](https://github.com/home-operations/flate/commit/ae68e9151a10d2b6db4d0c72f9a8a94c44dd1db1))
* adopt slices.Sorted(maps.Keys()) for collect+sort patterns ([#303](https://github.com/home-operations/flate/issues/303)) ([dec80b4](https://github.com/home-operations/flate/commit/dec80b476d330042e064c9b8179c78605afaa370))
* **cli:** collapse rendersHelm to slices.Contains; inline single-kind sites ([#297](https://github.com/home-operations/flate/issues/297)) ([c1cb490](https://github.com/home-operations/flate/commit/c1cb49015b552a78ba0cfc07a498d3cf633f4d73))
* cmp.Or sweep — two more comparator cascades ([#289](https://github.com/home-operations/flate/issues/289)) ([55b53dd](https://github.com/home-operations/flate/commit/55b53dd97db209dace231ddfe37a399e35a480b6))
* cmp.Or sweep [#4](https://github.com/home-operations/flate/issues/4) across manifest, oci, cli, errors ([#293](https://github.com/home-operations/flate/issues/293)) ([ea13e46](https://github.com/home-operations/flate/commit/ea13e46b8f03bab394ad3ff743899476c45c6bc9))
* cmp.Or sweep [#5](https://github.com/home-operations/flate/issues/5) — chartRef version, dep error, git TLS ([#294](https://github.com/home-operations/flate/issues/294)) ([b0860b1](https://github.com/home-operations/flate/commit/b0860b18ee932123588e88fb4b87188589929b67))
* cmp.Or sweep round 3 — 7 more zero-fallback sites ([#291](https://github.com/home-operations/flate/issues/291)) ([aaf0b79](https://github.com/home-operations/flate/commit/aaf0b7972ea71465a642b2420caed1b9a267303c))
* dedup kustomize-builder filename list to manifest.KustomizeBuilderFilenames ([#283](https://github.com/home-operations/flate/issues/283)) ([5311c03](https://github.com/home-operations/flate/commit/5311c03a3e5ab38055ecb41c0f79f73fb7fc3424))
* dedup RS render-doc identity key (resourceset.DedupKey) ([#285](https://github.com/home-operations/flate/issues/285)) ([fec5616](https://github.com/home-operations/flate/commit/fec5616ebe4d99377f47fdd1b45aeda239e4722b))
* **depwait:** extract tryReadyExpr for the 3-site eval pattern ([#286](https://github.com/home-operations/flate/issues/286)) ([d46f86d](https://github.com/home-operations/flate/commit/d46f86d08e6f3bc18f8eb7ff420efca0cb2ee9d3))
* **diff:** collapse 6-level sort cascade with cmp.Or ([#288](https://github.com/home-operations/flate/issues/288)) ([f209e65](https://github.com/home-operations/flate/commit/f209e6523c50b0a6ea1c3e1d7dcf96a74e6b557f))
* **discovery:** drop dead alreadyAliased skip-set + free knownSourceIDs ([#271](https://github.com/home-operations/flate/issues/271)) ([6ad8555](https://github.com/home-operations/flate/commit/6ad8555adfb9b0f0848b23e8870722c31e746756))
* **discovery:** simplify stripDotSlash + pathUnderRoot with stdlib ([#284](https://github.com/home-operations/flate/issues/284)) ([a180d4a](https://github.com/home-operations/flate/commit/a180d4a41041ca45c61128e900e8ee7215408b25))
* **discovery:** split aliasBootstrapSources into focused passes ([#268](https://github.com/home-operations/flate/issues/268)) ([44f6014](https://github.com/home-operations/flate/commit/44f60148aed4d29e15ad867d136ed70d7dfd290a))
* **helm:** add private secretGetter() snapshot helper ([#302](https://github.com/home-operations/flate/issues/302)) ([b09095c](https://github.com/home-operations/flate/commit/b09095c8fa506660e33c669181f6cbfb2a024caf))
* **helm:** observability + semver guard + distinct ambiguous error ([#273](https://github.com/home-operations/flate/issues/273)) ([e1091d2](https://github.com/home-operations/flate/commit/e1091d2d420d79b67bc3473bdfc8ed7e87a20793))
* **helm:** route resolver snapshots through Client.Resolver() ([#301](https://github.com/home-operations/flate/issues/301)) ([deaf8e0](https://github.com/home-operations/flate/commit/deaf8e05801c40a5f61e3efc323eb50583e914b1))
* **helm:** split repo.go into auth.go + oci_chart.go ([#287](https://github.com/home-operations/flate/issues/287)) ([eb65b65](https://github.com/home-operations/flate/commit/eb65b6572c74ac87e8ca67959aee90536ab3ad88))
* **loader:** collapse BuildParentIndex wrappers + drop one-line helper ([#278](https://github.com/home-operations/flate/issues/278)) ([a9c603c](https://github.com/home-operations/flate/commit/a9c603ca83902b77e976a22004f5639e22933257))
* **loader:** export NormalizePrefix and reuse in orchestrator ([#296](https://github.com/home-operations/flate/issues/296)) ([0941e1b](https://github.com/home-operations/flate/commit/0941e1b26a7089e3b1ebf2f9eff50c7ef5358f3c))
* **manifest/helmrelease:** replace 5 zero-fallback patterns with cmp.Or ([#290](https://github.com/home-operations/flate/issues/290)) ([9679b60](https://github.com/home-operations/flate/commit/9679b606f3411ef3fe8562ea6ab6e64746cb8a3d))
* **manifest:** centralize doc field reads behind DocKind/DocAPIVersion/DocMetadata ([#298](https://github.com/home-operations/flate/issues/298)) ([a8e3a18](https://github.com/home-operations/flate/commit/a8e3a186c6c0799bd21e9eca451aa00f23396b4c))
* **manifest:** factor stripObjectMetadata + stripMetadataInList helpers ([#281](https://github.com/home-operations/flate/issues/281)) ([a85fa5c](https://github.com/home-operations/flate/commit/a85fa5c3b877070ec1a01209bab02b3ddef25695))
* **manifest:** unify EnsureMetadata + MergeStringMap helpers ([#300](https://github.com/home-operations/flate/issues/300)) ([7bc7e2e](https://github.com/home-operations/flate/commit/7bc7e2e3d1b6a3a2a2a091b2dce0813b01ff9ee7))
* **orchestrator:** cacheRoot + RS sourceFile lookup → cmp.Or ([#292](https://github.com/home-operations/flate/issues/292)) ([7f8ed56](https://github.com/home-operations/flate/commit/7f8ed5688422e17b075b0f6fd1b8ae7741d14053))
* **orchestrator:** use slices.SortedFunc + NamedResource.Compare in cycles ([#304](https://github.com/home-operations/flate/issues/304)) ([c90e16e](https://github.com/home-operations/flate/commit/c90e16e95d8a2edc70a1b34f05085336eecc1220))
* **resourceset:** factor ensureMetadata for the two writer paths ([#282](https://github.com/home-operations/flate/issues/282)) ([6a6cb7e](https://github.com/home-operations/flate/commit/6a6cb7e60a3890b7d00587c15b88687868fa4cb5))
* route same-kind comparators through NamedResource.Compare ([#305](https://github.com/home-operations/flate/issues/305)) ([37553a7](https://github.com/home-operations/flate/commit/37553a7c674e711aec3723e812ce76666a01b284))
* simplify OCIRepository.Version() — drop unused error, use cmp.Or ([#295](https://github.com/home-operations/flate/issues/295)) ([5036f5a](https://github.com/home-operations/flate/commit/5036f5a5b0c3a61288ecffaa79fd93794822e5f5))
* **source/bucket:** split bucket.go into endpoint.go + transport.go + traversal.go ([#279](https://github.com/home-operations/flate/issues/279)) ([57a9293](https://github.com/home-operations/flate/commit/57a92930b61ff92d6318691585ab51bb734e6519))
* **source/git:** inline refStr fallback with cmp.Or ([#306](https://github.com/home-operations/flate/issues/306)) ([d6bdaf8](https://github.com/home-operations/flate/commit/d6bdaf8c66da99c6b949ab74edc5d84dbea9a549))
* **source/git:** split git.go into auth.go + tls.go ([#277](https://github.com/home-operations/flate/issues/277)) ([ee47c39](https://github.com/home-operations/flate/commit/ee47c39a12628d767cf15db90160f50947044e3a))
* **source/oci:** harden cache invariants + extractTarGz traversal ([#272](https://github.com/home-operations/flate/issues/272)) ([25607bf](https://github.com/home-operations/flate/commit/25607bfb77ec818051deaba176bfcf79e1658132))
* **source/oci:** split out flatFallbackStorage + tighten test ([#266](https://github.com/home-operations/flate/issues/266)) ([8a40b3e](https://github.com/home-operations/flate/commit/8a40b3e2c6775ea4dc31ebc0390df966fe95bea4))
* **source/oci:** use OCI Image Layout via content/oci.Store ([#267](https://github.com/home-operations/flate/issues/267)) ([ea6fac3](https://github.com/home-operations/flate/commit/ea6fac3a60030c2e5dc32c8cfcf13fb7c12ecbe1))
* **source/oci:** zip-bomb cap, cosign Warn, multi-layer ambiguity guard ([#275](https://github.com/home-operations/flate/issues/275)) ([491a27f](https://github.com/home-operations/flate/commit/491a27f7991d87a3290208af1f14c619618250ae))
* **store:** encapsulate status reset inside Refire ([#270](https://github.com/home-operations/flate/issues/270)) ([88d1ad0](https://github.com/home-operations/flate/commit/88d1ad0dbfc9bee859077bdc018d33335c58cb5e))

## [0.1.17](https://github.com/home-operations/flate/compare/0.1.16...0.1.17) (2026-05-24)


### Bug Fixes

* action script point to wrong arch ([b4ddab2](https://github.com/home-operations/flate/commit/b4ddab21592f35f76372e2f4948b2f5d5c8c8206))


### Code Refactoring

* **controllers:** split KS + HR controllers into per-concern files ([#253](https://github.com/home-operations/flate/issues/253)) ([52203c1](https://github.com/home-operations/flate/commit/52203c1210ee95b39083209c90d31311eca6b03c))
* **discovery:** split discovery.go into per-concern files ([#256](https://github.com/home-operations/flate/issues/256)) ([f28a0b4](https://github.com/home-operations/flate/commit/f28a0b44a60100ebb7bb84e058612c14feed9048))
* **orchestrator:** split orchestrator.go into finalize.go + resourceset.go ([#255](https://github.com/home-operations/flate/issues/255)) ([49d00f9](https://github.com/home-operations/flate/commit/49d00f98310ea8935c3eaf172b6efdb8646dfba0))
* **resourceset:** rename doc.go → output.go to avoid package-doc collision ([#258](https://github.com/home-operations/flate/issues/258)) ([8c3b940](https://github.com/home-operations/flate/commit/8c3b940f9f3c7652753bdb80da169c30e5857cbd))
* **resourceset:** split render.go into per-concern files ([#257](https://github.com/home-operations/flate/issues/257)) ([c7c51fe](https://github.com/home-operations/flate/commit/c7c51fe2fd81a1c974ba4f68fb30fc471d176961))
* **store:** typed Get[T] + ListAs[T] generic helpers ([#259](https://github.com/home-operations/flate/issues/259)) ([d6610f5](https://github.com/home-operations/flate/commit/d6610f56948d879b51700a65ef31915148bd1c3c))

## [0.1.16](https://github.com/home-operations/flate/compare/0.1.15...0.1.16) (2026-05-24)


### Bug Fixes

* **controllers/helmrelease:** refresh HR after dependsOn wait ([#251](https://github.com/home-operations/flate/issues/251)) ([55c46ac](https://github.com/home-operations/flate/commit/55c46ac0111e4fa1ed7d5fd0902528d33395a002))

## [0.1.15](https://github.com/home-operations/flate/compare/0.1.14...0.1.15) (2026-05-24)


### Features

* **cli:** add `flate diff all` for combined KS + HR diff ([#250](https://github.com/home-operations/flate/issues/250)) ([b6e2092](https://github.com/home-operations/flate/commit/b6e2092b4df1a58a4aa87c3f9aac48118e7b2888))


### Bug Fixes

* **discovery:** alias in-tree GitRepository whose URL matches working tree ([#248](https://github.com/home-operations/flate/issues/248)) ([306d7b4](https://github.com/home-operations/flate/commit/306d7b40a5ebb3561887a355b96e9d11666d3a57))


### Miscellaneous Chores

* cleanup pass after render-driven discovery migration ([#243](https://github.com/home-operations/flate/issues/243)) ([0413460](https://github.com/home-operations/flate/commit/041346096b1ea39fe0642faeb1282410639b1bcd))
* round-2 correctness pass (sentinel wrapping + honest failures) ([#245](https://github.com/home-operations/flate/issues/245)) ([af27f3e](https://github.com/home-operations/flate/commit/af27f3ea8cd76f1daf6b657320d4c93b6634359d))
* round-3 polish — complete Clone helpers + coverage gaps ([#246](https://github.com/home-operations/flate/issues/246)) ([979f644](https://github.com/home-operations/flate/commit/979f644c821d9c48a7a5d7d43917d5f5356d7491))
* round-4 hardening — version stamp + HTTP cap + path-traversal guard ([#247](https://github.com/home-operations/flate/issues/247)) ([d35fc0b](https://github.com/home-operations/flate/commit/d35fc0bf2bad96cce86682af2d058dfccb815372))
* round-6 — harden normalizeGitURL + integration coverage ([#249](https://github.com/home-operations/flate/issues/249)) ([08524a8](https://github.com/home-operations/flate/commit/08524a84c872e2438bbbda0ae8ec57adb797ab30))

## [0.1.14](https://github.com/home-operations/flate/compare/0.1.13...0.1.14) (2026-05-24)


### Features

* **controllers/kustomization:** substituteFrom ConfigMap as depwait edge (step 3/4) ([#240](https://github.com/home-operations/flate/issues/240)) ([10c605f](https://github.com/home-operations/flate/commit/10c605f740b1fcfa4683211e939d15697c051184))
* **loader:** DiscoveryOnly with lazy promotion (step 4/4) ([#242](https://github.com/home-operations/flate/issues/242)) ([f66cb59](https://github.com/home-operations/flate/commit/f66cb597cffd6f333fa64033584593b200376a91))


### Bug Fixes

* **manifest:** Flux-native envsubst handling — defaults + template skip ([#236](https://github.com/home-operations/flate/issues/236)) ([d445088](https://github.com/home-operations/flate/commit/d4450880635ffc0dfa9a0a7851b59c360948868c))


### Code Refactoring

* **controllers:** ParentOf is a resolver func, queries both sources ([#239](https://github.com/home-operations/flate/issues/239)) ([572e622](https://github.com/home-operations/flate/commit/572e6227d562805080c9fef3efa7254fed158263))
* **orchestrator:** RenderTracker carries parent KS provenance ([#238](https://github.com/home-operations/flate/issues/238)) ([2803315](https://github.com/home-operations/flate/commit/2803315a41bb4ee6b2ecaf391f2bde09ed4a7325))

## [0.1.13](https://github.com/home-operations/flate/compare/0.1.12...0.1.13) (2026-05-24)


### Bug Fixes

* **helmrelease:** dispatch chart-rendered source CRs through AddObject ([#230](https://github.com/home-operations/flate/issues/230)) ([dcd385a](https://github.com/home-operations/flate/commit/dcd385a13ee538ffd26b9f34de3bd8e8add87b39))
* **kustomize:** pre-fetch URL resources to eliminate git binary dep ([#229](https://github.com/home-operations/flate/issues/229)) ([5aa0845](https://github.com/home-operations/flate/commit/5aa0845a0a77f32d98f428365031fd37e09271bc))
* **kustomize:** propagate URL fetch failures as real reconcile errors ([#235](https://github.com/home-operations/flate/issues/235)) ([c024d5d](https://github.com/home-operations/flate/commit/c024d5d6fd670735052474ddb78c6492d0ba6239))


### Performance Improvements

* **kustomize:** dedup remote-resource fetches across preflight passes ([#232](https://github.com/home-operations/flate/issues/232)) ([a4740bf](https://github.com/home-operations/flate/commit/a4740bf0302403a1e73d9e6702df5e7702533f3b))


### Miscellaneous Chores

* **oci:** demote cosign keyless-skip log from WARN to DEBUG ([#233](https://github.com/home-operations/flate/issues/233)) ([53ad9c1](https://github.com/home-operations/flate/commit/53ad9c1552ef147c515912535d331355fc44a141))

## [0.1.12](https://github.com/home-operations/flate/compare/0.1.11...0.1.12) (2026-05-24)


### Features

* **diff:** always emit per-resource header, even for single diffs ([#215](https://github.com/home-operations/flate/issues/215)) ([6c26379](https://github.com/home-operations/flate/commit/6c263798d9ecb6a42bf8b79405b3dd6b21d0fb13))
* **orchestrator:** surface RS-rendered non-Flux docs in parent KS output ([#218](https://github.com/home-operations/flate/issues/218)) ([e1ccae9](https://github.com/home-operations/flate/commit/e1ccae9a0d77250ab4f5b45a22a5e04530fbaf9f))
* **resourceset:** implement spec.inputStrategy: Permute ([#109](https://github.com/home-operations/flate/issues/109)) ([#217](https://github.com/home-operations/flate/issues/217)) ([a8054cc](https://github.com/home-operations/flate/commit/a8054ccede6b51da9827ad620b5110cec108859c))


### Bug Fixes

* bootstrap-GR test skip in changed-only mode + IsSuspended + log noise ([#214](https://github.com/home-operations/flate/issues/214)) ([3833c6e](https://github.com/home-operations/flate/commit/3833c6ec3dd0cead4e1bdb86c144596ad51c09dc))
* **change:** scope keepByName fallback to empty-namespace entries only ([#207](https://github.com/home-operations/flate/issues/207)) ([909c08c](https://github.com/home-operations/flate/commit/909c08c805b72b28ff69c211bfbf3a7a19d45939))
* **cli,testrunner:** diff-images coercion, bootstrap-GR test filter, get-helm gate ([#212](https://github.com/home-operations/flate/issues/212)) ([6e1d2ab](https://github.com/home-operations/flate/commit/6e1d2abc60b3d3e55e63728f2c0c338991f78f72))
* **cli:** surface Bootstrap errors instead of drowning them in test report ([#216](https://github.com/home-operations/flate/issues/216)) ([a3d6dc5](https://github.com/home-operations/flate/commit/a3d6dc52470276c35ba1c005f5931cd7b0b1caca))
* **cli:** validate -o uniformly across get all / get images / test ([#222](https://github.com/home-operations/flate/issues/222)) ([37ae742](https://github.com/home-operations/flate/commit/37ae7429f6f6d4e496996adcf534bc77b4661241))
* **kustomize:** skip broken symlinks during stage instead of aborting ([#228](https://github.com/home-operations/flate/issues/228)) ([42a6ee8](https://github.com/home-operations/flate/commit/42a6ee855b04db148132be51bf02d38e98fea1c1))
* **manifest:** extend strip walker to CronJob jobTemplate + StatefulSet PVCs ([#210](https://github.com/home-operations/flate/issues/210)) ([96f3ab3](https://github.com/home-operations/flate/commit/96f3ab32d23596d6d2b9f3c28257eb8ac8c8139e))


### Performance Improvements

* **helmrelease:** skip duplicate render via parent gate + fingerprint dedup ([#219](https://github.com/home-operations/flate/issues/219)) ([9daefba](https://github.com/home-operations/flate/commit/9daefba56f5072d0ccc07e5eb4d7735fd6ef8371))
* **kustomization:** skip duplicate render via fingerprint dedup ([#220](https://github.com/home-operations/flate/issues/220)) ([69744f3](https://github.com/home-operations/flate/commit/69744f3a17712155b34e888d404ae30fc4655172))


### Documentation

* **readme:** default output filters, build example, pipeline + lib API refresh ([#211](https://github.com/home-operations/flate/issues/211)) ([bb66014](https://github.com/home-operations/flate/commit/bb6601412763610ded9a7e0d44737d64871027b2))


### Miscellaneous Chores

* post-iter-N cleanup — dead format, dedupe, alias warn, README ([#209](https://github.com/home-operations/flate/issues/209)) ([7ad1aa8](https://github.com/home-operations/flate/commit/7ad1aa80efe377f0d0b8ef72a307f138e7fb0ae4))
* StatusReady message constants + Filter.Add ordering godoc ([#213](https://github.com/home-operations/flate/issues/213)) ([f4724f2](https://github.com/home-operations/flate/commit/f4724f2906009fd1c42debf53d77d8e6402d0d96))


### Code Refactoring

* **discovery:** unify ParentOf + HRParentOf into one index ([#227](https://github.com/home-operations/flate/issues/227)) ([3414530](https://github.com/home-operations/flate/commit/34145300b0f659d83a11a005765fde5eb507a334))
* **manifest:** unexport per-kind Parse helpers (internal-only API) ([#223](https://github.com/home-operations/flate/issues/223)) ([5d740c9](https://github.com/home-operations/flate/commit/5d740c9c7391853f7f7989ffc1512ef9547eb4a6))
* **source:** adopt RunWithStatus + YieldSlot to match KS/HR shape ([#221](https://github.com/home-operations/flate/issues/221)) ([1927fdc](https://github.com/home-operations/flate/commit/1927fdc8a20230914f89d0970c7a6e58cfd880f4))

## [0.1.11](https://github.com/home-operations/flate/compare/0.1.10...0.1.11) (2026-05-24)


### Bug Fixes

* **change:** extend keep set when KS controller emits rendered children ([#205](https://github.com/home-operations/flate/issues/205)) ([33fc7f3](https://github.com/home-operations/flate/commit/33fc7f358260d22a8f541e00b5ef93941d716dc5))

## [0.1.10](https://github.com/home-operations/flate/compare/0.1.9...0.1.10) (2026-05-24)


### Bug Fixes

* **discovery:** alias bootstrap GitRepositories in any namespace ([#202](https://github.com/home-operations/flate/issues/202)) ([de497c4](https://github.com/home-operations/flate/commit/de497c45d520ba43f5d29bb984ee35350f96ba3f))

## [0.1.9](https://github.com/home-operations/flate/compare/0.1.8...0.1.9) (2026-05-24)


### Features

* **diff:** emit per-resource header only when there's more than one ([#200](https://github.com/home-operations/flate/issues/200)) ([9f5e7aa](https://github.com/home-operations/flate/commit/9f5e7aa63c72ae2e6ee7c6d3085f9ae54a8c093e))

## [0.1.8](https://github.com/home-operations/flate/compare/0.1.7...0.1.8) (2026-05-24)


### Features

* **diff:** restore --strip-attr (same defaults) as a pre-dyff filter ([#197](https://github.com/home-operations/flate/issues/197)) ([88b13f6](https://github.com/home-operations/flate/commit/88b13f63e5845506a10d41a672e9b86d43fac01e))

## [0.1.7](https://github.com/home-operations/flate/compare/0.1.6...0.1.7) (2026-05-24)


### Features

* **diff:** render via dyff in github mode; drop --strip-attr / --limit-bytes ([#196](https://github.com/home-operations/flate/issues/196)) ([a3c28ed](https://github.com/home-operations/flate/commit/a3c28ed2584a9f3b45c34c1f9c7654188ce1655a))


### Miscellaneous Chores

* immutable releases are painfoil ([258cc10](https://github.com/home-operations/flate/commit/258cc10b0039fbc328c47a7560253307e05375b0))

## [0.1.6](https://github.com/home-operations/flate/compare/0.1.5...0.1.6) (2026-05-23)


### Features

* **orchestrator:** --allow-missing-secrets for ExternalSecret-shaped repos ([#191](https://github.com/home-operations/flate/issues/191)) ([665870e](https://github.com/home-operations/flate/commit/665870ec980dc6d28e4bee53e5da7dcedbcf21fb))


### Bug Fixes

* **helm:** use @ separator for OCIRepository digest refs ([#189](https://github.com/home-operations/flate/issues/189)) ([14d476b](https://github.com/home-operations/flate/commit/14d476b4956f31108c7e771e88e7a4a7f328b8b3))
* **loader:** skip configMapGenerator/secretGenerator data files ([#194](https://github.com/home-operations/flate/issues/194)) ([3bca98b](https://github.com/home-operations/flate/commit/3bca98b4adfd6119f9f6dfa7372d8c87acdd2ffa))

## [0.1.5](https://github.com/home-operations/flate/compare/0.1.4...0.1.5) (2026-05-23)


### Bug Fixes

* **bucket:** SECURITY — reject path-traversal escapes from S3 keys ([#171](https://github.com/home-operations/flate/issues/171)) ([42e177b](https://github.com/home-operations/flate/commit/42e177b031c6bc85d12c5aebf524ff7407348e8e))
* **bucket:** skip VCS/extension default excludes — match source-controller ([#174](https://github.com/home-operations/flate/issues/174)) ([76f7e67](https://github.com/home-operations/flate/commit/76f7e67a595d58c1587c65cefe45cdb9d71f2adc))
* **cli:** apply --skip-secrets/--skip-crds/--skip-kinds to KS-sourced output ([#182](https://github.com/home-operations/flate/issues/182)) ([dfc4f8f](https://github.com/home-operations/flate/commit/dfc4f8f8e66069a6ff397efe30d381a19bf67f3c))
* **cli:** gate helm-only flags off KS-only subcommands ([#186](https://github.com/home-operations/flate/issues/186)) ([25f92e5](https://github.com/home-operations/flate/commit/25f92e56afcdfef61b1975fe4d7602828fdb6d56))
* **depwait:** surface metadata.labels + metadata.annotations to readyExpr CEL ([#172](https://github.com/home-operations/flate/issues/172)) ([5dc3570](https://github.com/home-operations/flate/commit/5dc357077708b0f19b1a123e1c426492a213353d))
* **helm,values:** honor spec.install.replace; valuesFrom ignores ConfigMap.binaryData ([#180](https://github.com/home-operations/flate/issues/180)) ([bc35489](https://github.com/home-operations/flate/commit/bc35489eacc463d63c2331d33339609d0ef7ffc7))
* **helm:** exclude CRDs from commonMetadata stamping ([#175](https://github.com/home-operations/flate/issues/175)) ([632e917](https://github.com/home-operations/flate/commit/632e9177126882e03f912527175e65c2dc0dee5f))
* **helm:** exclude Helm hooks from commonMetadata + origin-label stamps ([#173](https://github.com/home-operations/flate/issues/173)) ([e08e20e](https://github.com/home-operations/flate/commit/e08e20e60c5a2a3538ce0e8dd7c747ceb1719d1f))
* **manifest:** reject Kustomization with empty sourceRef.name at parse time ([#179](https://github.com/home-operations/flate/issues/179)) ([70648e6](https://github.com/home-operations/flate/commit/70648e6464000755733b3513fdddd410ee57f941))
* **orchestrator:** apply --skip-kinds in Render() so embedders match CLI ([#183](https://github.com/home-operations/flate/issues/183)) ([87926cc](https://github.com/home-operations/flate/commit/87926ccff4b83c5f31aadd7948912262123df4c8))
* **orchestrator:** detect dependsOn cycles up-front, strip cycle edges ([#178](https://github.com/home-operations/flate/issues/178)) ([bd25c6b](https://github.com/home-operations/flate/commit/bd25c6b88c1f998c40b42018b2e1823baaf464f1))
* **resourceset:** reject inputStrategy=Permute instead of silent fall-through ([#176](https://github.com/home-operations/flate/issues/176)) ([ac548dc](https://github.com/home-operations/flate/commit/ac548dc026cc47fb9d39880ebbae687c7cafeeb9))
* **testrunner,orchestrator:** apply TrimSentinelPrefix at user-facing boundaries ([#185](https://github.com/home-operations/flate/issues/185)) ([d63ba9e](https://github.com/home-operations/flate/commit/d63ba9e655e0f8cb5f18c2e22f0269b78e781434))


### Documentation

* strip README to elegant, concise, digestible form ([#163](https://github.com/home-operations/flate/issues/163)) ([a5b3f3e](https://github.com/home-operations/flate/commit/a5b3f3eff679e8afa02ed0a29fa59beada923669))
* surface kustomize.Prepare alongside helm.Prepare ([#168](https://github.com/home-operations/flate/issues/168)) ([97f9116](https://github.com/home-operations/flate/commit/97f91169aa6c046a2a83b49c2b355adf7df943ee))


### Miscellaneous Chores

* add flate install gh action ([39ecdfe](https://github.com/home-operations/flate/commit/39ecdfeb38a313394f46c291475ca9d943dc6e20))


### Code Refactoring

* **cli:** consume Orchestrator.Render() instead of reaching into Store ([#166](https://github.com/home-operations/flate/issues/166)) ([03ee28d](https://github.com/home-operations/flate/commit/03ee28deeacfb87b12c349d0590cb5e77ee1a955))
* **helm:** route HelmChartSource through SourceResolver ([#184](https://github.com/home-operations/flate/issues/184)) ([2e212d9](https://github.com/home-operations/flate/commit/2e212d9daa6127a3e8d4a9fe6205a6ffe9345254))
* **kustomize:** add Prepare helper symmetric to helm.Prepare ([#167](https://github.com/home-operations/flate/issues/167)) ([00a5633](https://github.com/home-operations/flate/commit/00a563345b31ab9bfa96f22ed44154862e50a2be))
* **manifest:** move trimFluxPrefix → manifest.TrimSentinelPrefix ([#181](https://github.com/home-operations/flate/issues/181)) ([6135708](https://github.com/home-operations/flate/commit/61357088a2aeaeef92165ef45adc018cafcfa494))
* **manifest:** split helm.go into per-kind files ([#188](https://github.com/home-operations/flate/issues/188)) ([1126153](https://github.com/home-operations/flate/commit/1126153bcf214179868cdf11102229be76803d28))
* **orchestrator,kustomization:** extract finalize + emitRenderedChildren ([#187](https://github.com/home-operations/flate/issues/187)) ([3389fdf](https://github.com/home-operations/flate/commit/3389fdfb7e2445bc09509862b501565710510d14))
* **store:** move rendered-set bookkeeping out into orchestrator ([#177](https://github.com/home-operations/flate/issues/177)) ([4d92319](https://github.com/home-operations/flate/commit/4d92319ebfdce7c6d5e39dfd31a4e0341524b4ef))
* **store:** split methods across topic files (objects/status/artifacts/events/rendered) ([#170](https://github.com/home-operations/flate/issues/170)) ([372f366](https://github.com/home-operations/flate/commit/372f36613140474b954bd8e7a7451e5ecd074295))

## [0.1.4](https://github.com/home-operations/flate/compare/0.1.3...0.1.4) (2026-05-23)


### Features

* add homebrew support ([d5e88df](https://github.com/home-operations/flate/commit/d5e88df6ba63d4afea598e18c84906dc3f8fe378))
* add homebrew support ([3e1c190](https://github.com/home-operations/flate/commit/3e1c190d330b0aaccd6b76c60e3e9dadd3a0f752))
* **api:** Orchestrator.Render + typed Store.On* listeners ([#156](https://github.com/home-operations/flate/issues/156)) ([845b730](https://github.com/home-operations/flate/commit/845b73095ae32da2192c8bdfbb3417c62a1c54d3))
* **bucket:** support certSecretRef for mTLS endpoints ([#47](https://github.com/home-operations/flate/issues/47)) ([7c04ec2](https://github.com/home-operations/flate/commit/7c04ec25ce86b5cb373ab3ad0aebb56cc105a4a5))
* **cli:** broaden get hr/ks structured projection ([#93](https://github.com/home-operations/flate/issues/93)) ([5034bf8](https://github.com/home-operations/flate/commit/5034bf8a0ac2da4f311ad0fe44083e68e5869791))
* **controllers:** fail-loud on SOPS-encrypted rendered output ([#38](https://github.com/home-operations/flate/issues/38)) ([a25cbb5](https://github.com/home-operations/flate/commit/a25cbb5aaf4ee8e79969c3dc2ef795aa9c0a5be1))
* **depwait:** dependsOn ReadyExpr CEL evaluation ([#39](https://github.com/home-operations/flate/issues/39)) ([51052f7](https://github.com/home-operations/flate/commit/51052f7e7c14bb30eb5b8c35e45f0dcaa74dbba8))
* **depwait:** project observedGeneration/generation for CEL ReadyExpr ([#61](https://github.com/home-operations/flate/issues/61)) ([8970dce](https://github.com/home-operations/flate/commit/8970dceecfaa01018f9f66f5430ee444b62a5b4b))
* **get:** add `flate get images`; drop --enable-images / --only-images on `get all` ([#123](https://github.com/home-operations/flate/issues/123)) ([e3cc8ba](https://github.com/home-operations/flate/commit/e3cc8bae0faad6113164c611b2e3086323f45b9b))
* **git:** support ca.crt / caFile in GitRepository SecretRef ([#48](https://github.com/home-operations/flate/issues/48)) ([2906860](https://github.com/home-operations/flate/commit/290686090b058d0ad7ed524a08e3c31b2ca22fc4))
* **git:** support spec.verify (PGP commit/tag verification) ([#60](https://github.com/home-operations/flate/issues/60)) ([bb06f0b](https://github.com/home-operations/flate/commit/bb06f0bbf72fd0ee5cb28fb00be85b4167c06e30))
* **helm:** HelmRepository certSecretRef (mTLS) ([#45](https://github.com/home-operations/flate/issues/45)) ([fa33e01](https://github.com/home-operations/flate/commit/fa33e0187db8d6ce83dd6def91ab400445281f39))
* **helm:** HelmRepository SecretRef auth (HTTP basic + bearer) ([#37](https://github.com/home-operations/flate/issues/37)) ([924fb71](https://github.com/home-operations/flate/commit/924fb7140970aa10e887f993ab4edcd1a31bed1c))
* **helmrelease:** honor disableHooks, spec.test.enable, spec.commonMetadata ([#88](https://github.com/home-operations/flate/issues/88)) ([c57fa29](https://github.com/home-operations/flate/commit/c57fa293bd21712e19e2d376fde7468e2edd7394))
* **helmrelease:** honor spec.install.crds / spec.upgrade.crds policy ([#59](https://github.com/home-operations/flate/issues/59)) ([462e5df](https://github.com/home-operations/flate/commit/462e5df2b877af1a66d3974e9520fb8a6375b167))
* **helmrelease:** honor spec.postRenderers ([#80](https://github.com/home-operations/flate/issues/80)) ([efebeb8](https://github.com/home-operations/flate/commit/efebeb8618094a495b85ab9e5f08a642d75d26bd))
* **helmrelease:** honor spec.releaseName ([#76](https://github.com/home-operations/flate/issues/76)) ([72a8701](https://github.com/home-operations/flate/commit/72a8701cac8089912086a376672772f7f62f3f22))
* **kustomize:** honor Kustomization spec.commonMetadata ([#71](https://github.com/home-operations/flate/issues/71)) ([d90e5c2](https://github.com/home-operations/flate/commit/d90e5c22609e2da61fbc06dcb11d22000b204c28))
* **kustomize:** inject Flux owner labels on rendered children ([#81](https://github.com/home-operations/flate/issues/81)) ([a168f5c](https://github.com/home-operations/flate/commit/a168f5c333b35b7040e59340c79988929574ddd7))
* **manifest:** parse HelmRelease.serviceAccountName + HelmChart.reconcileStrategy ([#49](https://github.com/home-operations/flate/issues/49)) ([5c6e28a](https://github.com/home-operations/flate/commit/5c6e28a16d3722821555cd9cda71cb7c4c8398d1))
* **oci:** cosign keyed verification for OCIRepository ([#50](https://github.com/home-operations/flate/issues/50)) ([418caad](https://github.com/home-operations/flate/commit/418caad116d0d27457957a5f6e86c0b24cdce42c))
* **oci:** honor OCIRepository.spec.layerSelector ([#54](https://github.com/home-operations/flate/issues/54)) ([2e9cc8e](https://github.com/home-operations/flate/commit/2e9cc8eabcce32b63e8fce53ac3e610cb850d276))
* **resourceset:** expand flux-operator ResourceSet CRs at load time ([#106](https://github.com/home-operations/flate/issues/106)) ([5c41b12](https://github.com/home-operations/flate/commit/5c41b124a261e3e8f9d343f3bbbddcb71a70f024))
* **resourceset:** resolve spec.inputsFrom via ResourceSetInputProvider ([#122](https://github.com/home-operations/flate/issues/122)) ([3acbd34](https://github.com/home-operations/flate/commit/3acbd34102c4667d5037906c47afe47c2a4ecc85))
* **source/git:** RecurseSubmodules + spec.ref.name ([#41](https://github.com/home-operations/flate/issues/41)) ([0b664dd](https://github.com/home-operations/flate/commit/0b664dde73e13742910c15433175a1285383beef))
* **source/git:** spec.sparseCheckout ([#44](https://github.com/home-operations/flate/issues/44)) ([45f14bf](https://github.com/home-operations/flate/commit/45f14bf102ec3fd223135fad6478182b99248aa1))
* **source/oci:** certSecretRef (mTLS) + spec.insecure ([#46](https://github.com/home-operations/flate/issues/46)) ([2dbcee1](https://github.com/home-operations/flate/commit/2dbcee1c4ba7d7bd262d5797a26d240100ee173e))
* **source:** add Bucket support (generic / S3-compatible provider) ([#34](https://github.com/home-operations/flate/issues/34)) ([0840d4f](https://github.com/home-operations/flate/commit/0840d4ffb036ec472739f43039bed7faf5f301bd))
* **source:** add ExternalArtifact support ([#33](https://github.com/home-operations/flate/issues/33)) ([994d423](https://github.com/home-operations/flate/commit/994d4237a2cfe483be51f88c33406eabd77857ea))
* **source:** GitRepository SecretRef auth (HTTPS basic / bearer / SSH) ([#35](https://github.com/home-operations/flate/issues/35)) ([4089381](https://github.com/home-operations/flate/commit/4089381d5c5f75c0c6ef44b4b5a4be5fe0ca4205))
* **source:** honor spec.ignore on Git/OCI/Bucket sources ([#89](https://github.com/home-operations/flate/issues/89)) ([35dd6a8](https://github.com/home-operations/flate/commit/35dd6a8950a8e845d4f59468d848690da2d6fd3a))
* **source:** OCIRepository SecretRef auth (dockerconfigjson) ([#36](https://github.com/home-operations/flate/issues/36)) ([39d8094](https://github.com/home-operations/flate/commit/39d809409ac3d75f43d7c5b91b8555b425311738))
* **source:** support spec.proxySecretRef on Git/OCI/Bucket ([#55](https://github.com/home-operations/flate/issues/55)) ([ae28030](https://github.com/home-operations/flate/commit/ae28030ba7d420f99f7b9527858509e13698f2e2))
* **task:** bounded-concurrency Service via worker-pool semaphore ([#63](https://github.com/home-operations/flate/issues/63)) ([bbfbcad](https://github.com/home-operations/flate/commit/bbfbcadd6903c0fa88616820511b1272ee5ea820))
* thread ctx through RenderFlux and Loader.Load ([#62](https://github.com/home-operations/flate/issues/62)) ([f6f2c9c](https://github.com/home-operations/flate/commit/f6f2c9c1e1c4f141aaab756fa1900f2185d370b6))
* wipe SOPS-encrypted secrets to PLACEHOLDER instead of failing ([#53](https://github.com/home-operations/flate/issues/53)) ([2b085f2](https://github.com/home-operations/flate/commit/2b085f2f56236d862a356c94282e787ac3cdced2))


### Bug Fixes

* align with upstream Flux on CEL, helm strvals, slug, bucket, release-name ([#154](https://github.com/home-operations/flate/issues/154)) ([dfbcf15](https://github.com/home-operations/flate/commit/dfbcf15a37ae291cbdf8ae63b0868e5261c94947))
* **change:** keep ancestor Kustomizations in changed-only keep set ([#104](https://github.com/home-operations/flate/issues/104)) ([96752cd](https://github.com/home-operations/flate/commit/96752cd99b5d214409407a54f47bd7401063939f))
* **cli:** exit non-zero on per-resource reconcile failures ([#158](https://github.com/home-operations/flate/issues/158)) ([3aa8fa6](https://github.com/home-operations/flate/commit/3aa8fa68153857fbf9f3efc94c2e8a5781c44358))
* **cli:** gate --output per subcommand; honor -o json for build ([#159](https://github.com/home-operations/flate/issues/159)) ([d36f4c4](https://github.com/home-operations/flate/commit/d36f4c4460a2a7159cbea92d1a0dbb99a7ff0df1))
* **controllers:** typed errors with %w wrapping for dependency / source failures ([#57](https://github.com/home-operations/flate/issues/57)) ([4412cdc](https://github.com/home-operations/flate/commit/4412cdc292ff52ce540830d7d2779155f3e62ddc))
* correctness pass — git race, depwait drop, HR dependsOn pruning ([#124](https://github.com/home-operations/flate/issues/124)) ([cc71799](https://github.com/home-operations/flate/commit/cc7179986606a48ac021ca54b72d5fb48c5b6688))
* **cosign:** ed25519 verify on raw payload; -l scoped to get; deterministic test ([#144](https://github.com/home-operations/flate/issues/144)) ([4b22a75](https://github.com/home-operations/flate/commit/4b22a757d768782a2833aafc165793f4e7607ee0))
* **cosign:** skip keyless verification with warn instead of failing fetch ([#121](https://github.com/home-operations/flate/issues/121)) ([1bd3860](https://github.com/home-operations/flate/commit/1bd3860c41758ff5be9fa1a7adce0740af702f10))
* ctx-aware per-key locks for kustomize render + helm chart cache ([#74](https://github.com/home-operations/flate/issues/74)) ([035b263](https://github.com/home-operations/flate/commit/035b263f97d3aff8e224960d8c65dfe413ae268a))
* decode YAML the way Flux does; widen change-filter to structural parent ([#120](https://github.com/home-operations/flate/issues/120)) ([4d9ec67](https://github.com/home-operations/flate/commit/4d9ec67ef7645dff63cfb01351607c9a78a3bfe9))
* **depwait:** honor spec.timeout on Kustomization + HelmRelease ([#142](https://github.com/home-operations/flate/issues/142)) ([869bba1](https://github.com/home-operations/flate/commit/869bba150a18d550d9bc373fc252ccc09724f486))
* **depwait:** ReadyExpr non-additive default + status event for any condition ([#43](https://github.com/home-operations/flate/issues/43)) ([75f47c6](https://github.com/home-operations/flate/commit/75f47c6f8ebe768b22ff8c3b230f0c329d381de7))
* **depwait:** recover panics in per-dep watch goroutine ([#73](https://github.com/home-operations/flate/issues/73)) ([6c3bc6d](https://github.com/home-operations/flate/commit/6c3bc6d7a4337be73bc6dce6ab3373cb7e9ff51d))
* **flux-fidelity:** correctness bugs C1-C5 ([#26](https://github.com/home-operations/flate/issues/26)) ([41a90ac](https://github.com/home-operations/flate/commit/41a90ac9586196f901dd1d8a36a14bdde1429af7))
* **helm:** skip schema validation when values contain wipe placeholders ([#138](https://github.com/home-operations/flate/issues/138)) ([0395e1d](https://github.com/home-operations/flate/commit/0395e1dea3c93746cd07b17f35c5351c7464d2eb))
* **helm:** stamp helm.toolkit.fluxcd.io origin labels on rendered docs ([#113](https://github.com/home-operations/flate/issues/113)) ([3b71293](https://github.com/home-operations/flate/commit/3b712937f8cae4b63baad8df08ef282a07879eed))
* iter-7 — CLI UX, store replay panic, helm listener payload, perf ([#145](https://github.com/home-operations/flate/issues/145)) ([6bf9fbf](https://github.com/home-operations/flate/commit/6bf9fbfcf55f34f3293024838bf96883ac87b913))
* **kustomization:** emit render output before SOPS abort + dispatch child events ([#52](https://github.com/home-operations/flate/issues/52)) ([264b9d6](https://github.com/home-operations/flate/commit/264b9d6115ac160aa3ff91b60e4f786654cd6fa6))
* **kustomization:** fall back to bootstrap source when sourceRef is empty ([#107](https://github.com/home-operations/flate/issues/107)) ([093d222](https://github.com/home-operations/flate/commit/093d222862e8115c0c2b9ca56fb5965cf9f48408))
* **kustomization:** wait for structural parent before child reconcile ([#103](https://github.com/home-operations/flate/issues/103)) ([47f319c](https://github.com/home-operations/flate/commit/47f319c2bcb627bb8dd296a2cd95416d2a9cc329))
* **kustomize:** match upstream non-strict postBuild substitution default ([#119](https://github.com/home-operations/flate/issues/119)) ([e016444](https://github.com/home-operations/flate/commit/e016444a2e562a05a20ab34c92b870411a9e96ea))
* **loader:** inherit Flux KS effective namespace onto cross-tree spec.path resources ([#101](https://github.com/home-operations/flate/issues/101)) ([898c385](https://github.com/home-operations/flate/commit/898c38542d69c2e8211238bf86e7ce61e9674f5a))
* **loader:** skip kustomize Component subtrees during manifest discovery ([#136](https://github.com/home-operations/flate/issues/136)) ([00f923c](https://github.com/home-operations/flate/commit/00f923cc556d716fd662dae1ea432a2805f20a6f))
* **manifest:** wrap underlying err with %w in inputf call sites ([#127](https://github.com/home-operations/flate/issues/127)) ([8512c62](https://github.com/home-operations/flate/commit/8512c62d59e3a3ec02a64c0f3af52a0c131ed5f1))
* **orchestrator:** strip flux:input: prefix; include source file in failure messages ([#160](https://github.com/home-operations/flate/issues/160)) ([5b85975](https://github.com/home-operations/flate/commit/5b85975f6ac964a3315da4bc308f4df1a68a509b))
* pass [#5](https://github.com/home-operations/flate/issues/5) follow-ups — kustomize label precedence, cache.Reset lock, filterShowOnly tests ([#116](https://github.com/home-operations/flate/issues/116)) ([d7840e3](https://github.com/home-operations/flate/commit/d7840e3ee4f9bbe2d19107b7a82b61ecc7a871cf))
* postBuild precedence + bootstrap aliases + git slot lock + dead code ([#143](https://github.com/home-operations/flate/issues/143)) ([9498cb4](https://github.com/home-operations/flate/commit/9498cb4d14ac0c269dfbc6bc8a63291a63beb0b1))
* **resourceset:** align renderer with upstream flux-operator semantics ([#111](https://github.com/home-operations/flate/issues/111)) ([2519509](https://github.com/home-operations/flate/commit/25195097a742c159e2d518cb2828d62c616bad77))
* **source,manifest:** sourceignore defaults + ResolveChartRef HelmChart hole ([#90](https://github.com/home-operations/flate/issues/90)) ([0b5b6ea](https://github.com/home-operations/flate/commit/0b5b6eafb075a128b403b0a3c412192ba2feffbb))
* **source/cache:** per-slot locking; OCI defer-order on error paths ([#140](https://github.com/home-operations/flate/issues/140)) ([c63e389](https://github.com/home-operations/flate/commit/c63e3899784faf75142cc79284943abc3441ce73))
* **source/git:** preserve cache-hit signal across ApplyIgnore ([#137](https://github.com/home-operations/flate/issues/137)) ([a71830c](https://github.com/home-operations/flate/commit/a71830ceb684670e1ce3d1360b0ffd8cca80f0b7))
* **source:** base64-decode Secret.Data values in StringFromSecret ([#133](https://github.com/home-operations/flate/issues/133)) ([c5eab31](https://github.com/home-operations/flate/commit/c5eab310706623e62e6649a87c61f4fc8aea11ff))
* **source:** coalesce per-id fetches ([#75](https://github.com/home-operations/flate/issues/75)) ([4970e9a](https://github.com/home-operations/flate/commit/4970e9a32724721d6f8e1c8fed90d69c61d0a0bd))
* **source:** walk all files instead of SkipDir on excluded parents ([#135](https://github.com/home-operations/flate/issues/135)) ([b5c4840](https://github.com/home-operations/flate/commit/b5c48403a7208bbfe1a88d7e70dbe4cb55eb05ed))
* store-immutability violations + 4 resource-leak / cleanup bugs ([#129](https://github.com/home-operations/flate/issues/129)) ([1e6da3e](https://github.com/home-operations/flate/commit/1e6da3ee88f3df370b44fb89e2f3f49d1932214a))
* **store:** document immutability contract; clone-then-add at mutation sites ([#56](https://github.com/home-operations/flate/issues/56)) ([011bfa0](https://github.com/home-operations/flate/commit/011bfa0b0b6bd2dc242b3c15c2cf62f0580ed70a))
* **task:** reset Coalescer slot when fn panics ([#67](https://github.com/home-operations/flate/issues/67)) ([95e71d6](https://github.com/home-operations/flate/commit/95e71d65d7c6554dc0a053e1d8a283d625084dbd))
* **task:** YieldSlot to break parent/child KS pool deadlock ([#72](https://github.com/home-operations/flate/issues/72)) ([2d5d09a](https://github.com/home-operations/flate/commit/2d5d09afea6ac43c86e455bdc60353c6df769c4e))
* three correctness bugs from post-merge architecture review ([#86](https://github.com/home-operations/flate/issues/86)) ([8716471](https://github.com/home-operations/flate/commit/8716471ea25f20381e916208e11248e53899c9e5))


### Performance Improvements

* **build:** stream per-artifact, sort for deterministic output ([#146](https://github.com/home-operations/flate/issues/146)) ([0ec623b](https://github.com/home-operations/flate/commit/0ec623b10f669fba3ca9306bcf5a344c677201f2))
* **helm:** coalesce concurrent first-load of the same chart ([#153](https://github.com/home-operations/flate/issues/153)) ([b016d56](https://github.com/home-operations/flate/commit/b016d561ba9e2bec3b669f0b94b271e98d360fc8))
* list-objects by-kind index + skip envsubst round-trip on docs with no `${` ([#125](https://github.com/home-operations/flate/issues/125)) ([e35262c](https://github.com/home-operations/flate/commit/e35262c82cecd874bb6d54dabc7bf9d82642cf92))


### Documentation

* document Phase 3 capabilities ([#42](https://github.com/home-operations/flate/issues/42)) ([46adbee](https://github.com/home-operations/flate/commit/46adbee904fd8b399e8c471b3b6f4b6c547b3c69))
* document the library/embed API; refresh post-iter-12 surface ([#161](https://github.com/home-operations/flate/issues/161)) ([bad11d6](https://github.com/home-operations/flate/commit/bad11d6cd40c92a3a536612d0b21ccec221a08ef))
* prune stale comments and outdated references ([#118](https://github.com/home-operations/flate/issues/118)) ([682dd18](https://github.com/home-operations/flate/commit/682dd1860c52c3f56d09f78de34b8546bc030cba))
* refresh README + TESTDATA for SOPS placeholder, cosign, fixtures ([#66](https://github.com/home-operations/flate/issues/66)) ([0daacd6](https://github.com/home-operations/flate/commit/0daacd6831b6513be1bf871e5baeb02e7fcf6773))


### Miscellaneous Chores

* camelCase log keys + README drift fixes ([#128](https://github.com/home-operations/flate/issues/128)) ([19d42d4](https://github.com/home-operations/flate/commit/19d42d47adf7aeb98991f52f622f733f45706366))
* clean up 5 revive lint warnings ([#134](https://github.com/home-operations/flate/issues/134)) ([1f362d8](https://github.com/home-operations/flate/commit/1f362d8c672c502c3e12cf3286d81f413d7b23a5))
* consolidate test cert helpers + drop unused IDName ([#70](https://github.com/home-operations/flate/issues/70)) ([5b0f81e](https://github.com/home-operations/flate/commit/5b0f81ec3c4969dea32fb6f2c2df10333a6132ce))
* **format:** drop unused OutputWide constant ([#115](https://github.com/home-operations/flate/issues/115)) ([de4a3fe](https://github.com/home-operations/flate/commit/de4a3fe359a86bdc11c59295fc14a14da8ce8e2a))
* **lint:** drop unused ctx/obj parameters in base_test ([#32](https://github.com/home-operations/flate/issues/32)) ([2e76027](https://github.com/home-operations/flate/commit/2e76027ac102d3a816b5384712dbb45822a1a840))
* **manifest,store,helm:** prune dead code and redundant defensive clones ([#91](https://github.com/home-operations/flate/issues/91)) ([4add754](https://github.com/home-operations/flate/commit/4add754ce15a7a5025add65a459e3588df89d9e7))
* post-embed cleanup + controller unit tests ([#87](https://github.com/home-operations/flate/issues/87)) ([542c421](https://github.com/home-operations/flate/commit/542c4213c56f4ab076838bfeaba3aba3bf61b75a))
* purge dead code from review pass ([#126](https://github.com/home-operations/flate/issues/126)) ([671d5f6](https://github.com/home-operations/flate/commit/671d5f6d56c6b3baca25145a46cdbe686e374078))
* remove dead code ([#77](https://github.com/home-operations/flate/issues/77)) ([0c40ffa](https://github.com/home-operations/flate/commit/0c40ffaae04cdbd0ea1ab424caa9d1a7cf8f3650))
* review follow-ups — mergeChartValuesFiles coverage, dedupe proxy tests, name slug cap ([#94](https://github.com/home-operations/flate/issues/94)) ([bda7a07](https://github.com/home-operations/flate/commit/bda7a077bfa10648d1c6e01718da97ea83270001))


### Code Refactoring

* **api:** dedupe SecretGetter, simplify discovery.Config, document shadow-fields rule ([#152](https://github.com/home-operations/flate/issues/152)) ([ccfac8e](https://github.com/home-operations/flate/commit/ccfac8ed64bff800175e0574456974c0ddfe1ebc))
* code-quality pass — bugs, perf, DRY, structural cleanup ([#23](https://github.com/home-operations/flate/issues/23)) ([d868aec](https://github.com/home-operations/flate/commit/d868aec683be0e42507dfffadb27c247000a313a))
* **controllers:** consolidate lifecycle into base.Controller; fix(oci): cache-poison ([#150](https://github.com/home-operations/flate/issues/150)) ([8ba6a5c](https://github.com/home-operations/flate/commit/8ba6a5c7774d87b24e8a23be194a155e7b4886fe))
* **controllers:** extract base.Recover + base.RunWithStatus ([#30](https://github.com/home-operations/flate/issues/30)) ([3b17ae7](https://github.com/home-operations/flate/commit/3b17ae7cb06efa481af524f78d7b4397a85d9b24))
* ExistenceFetcher in source controller; canonical BootstrapSourceID ([#132](https://github.com/home-operations/flate/issues/132)) ([bbc95f1](https://github.com/home-operations/flate/commit/bbc95f167e544849d20af02a2ea8ca965e339361))
* extract pkg/discovery; slices.SortFunc + store.Mutate[T] ([#149](https://github.com/home-operations/flate/issues/149)) ([32cc081](https://github.com/home-operations/flate/commit/32cc0819886fb70322315daed74999da07792930))
* **helm:** delete legacy Add* push API; SourceResolver is the only path ([#162](https://github.com/home-operations/flate/issues/162)) ([d7c2baf](https://github.com/home-operations/flate/commit/d7c2baf35a4fbd47efeb365d78bdfe0ac253289a))
* **helm:** route source-CR lookups through SourceResolver ([#155](https://github.com/home-operations/flate/issues/155)) ([742b75e](https://github.com/home-operations/flate/commit/742b75e71dd02ccb944ab904b2f8dd41b229b2f9))
* **manifest:** alias local ref types to fluxcd upstream ([#68](https://github.com/home-operations/flate/issues/68)) ([03e114e](https://github.com/home-operations/flate/commit/03e114e7d83b0c895b127c5499624333f3d199c3))
* **manifest:** alias value-twin source types to upstream ([#83](https://github.com/home-operations/flate/issues/83)) ([7eac7c1](https://github.com/home-operations/flate/commit/7eac7c17dad0f1e5dbed43956a2db38fe0368cc8))
* **manifest:** embed upstream Specs into all 8 top-level CRs ([#85](https://github.com/home-operations/flate/issues/85)) ([061c2f2](https://github.com/home-operations/flate/commit/061c2f2dc8f55a73f2e56d2c7f2a4f2ed78bcceb))
* **manifest:** replace provider/verify-mode shadows with upstream ([#79](https://github.com/home-operations/flate/issues/79)) ([9ab91da](https://github.com/home-operations/flate/commit/9ab91da4816eaca2c5af6b73f316990663cf79b1))
* **manifest:** use typed Flux APIs for Parse* functions ([#24](https://github.com/home-operations/flate/issues/24)) ([c79f6d2](https://github.com/home-operations/flate/commit/c79f6d251a9246e6d64dc89cc23209ce6336f7ae))
* **source:** extract BuildTLSConfig helper; delete duplicate secret reader ([#64](https://github.com/home-operations/flate/issues/64)) ([1ce7e82](https://github.com/home-operations/flate/commit/1ce7e827fbd2951f4c0a84d706cbc06c7edb48c6))
* **source:** hoist cert-secret resolution into ResolveCertSecret ([#78](https://github.com/home-operations/flate/issues/78)) ([cab3318](https://github.com/home-operations/flate/commit/cab3318ae6ec8ae081da1c95992acadfc0873a15))
* **source:** per-kind subdirectories ([#40](https://github.com/home-operations/flate/issues/40)) ([3cddc9d](https://github.com/home-operations/flate/commit/3cddc9dc1496bb0edfdbeca85a9fc0a8838bfdb9))
* **source:** unified SourceArtifact + Fetcher interface ([#31](https://github.com/home-operations/flate/issues/31)) ([3dc0375](https://github.com/home-operations/flate/commit/3dc03750def17444c3bf11349cd7a5481aa7278b))
* **store:** switch internal status to []metav1.Condition ([#28](https://github.com/home-operations/flate/issues/28)) ([d3318a5](https://github.com/home-operations/flate/commit/d3318a585dfab8af7b3b7291611bcc911eebbd38))

## [0.1.3](https://github.com/home-operations/flate/compare/0.1.2...0.1.3) (2026-05-22)


### Bug Fixes

* create tag in release please workflow ([5726b91](https://github.com/home-operations/flate/commit/5726b9179f10a2a007a9cafa7215e614b031cf7c))

## [0.4.0](https://github.com/home-operations/flate/compare/0.3.0...0.4.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* have goreleaser manage releases ([c9b7fe4](https://github.com/home-operations/flate/commit/c9b7fe40799c5d05b87475f7c8333c6453a54caf))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* **main:** release 0.2.0 ([#16](https://github.com/home-operations/flate/issues/16)) ([3ed0289](https://github.com/home-operations/flate/commit/3ed0289ef82e411609d0766f698b2d6458bd62f9))
* **main:** release 0.3.0 ([#17](https://github.com/home-operations/flate/issues/17)) ([1064a80](https://github.com/home-operations/flate/commit/1064a80257b53e5c4ed3a5a6f25f75396f0ecd95))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.3.0](https://github.com/home-operations/flate/compare/0.2.0...0.3.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* **main:** release 0.2.0 ([#16](https://github.com/home-operations/flate/issues/16)) ([3ed0289](https://github.com/home-operations/flate/commit/3ed0289ef82e411609d0766f698b2d6458bd62f9))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.2.0](https://github.com/home-operations/flate/compare/0.1.3...0.2.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.1.3](https://github.com/home-operations/flate/compare/0.1.2...0.1.3) (2026-05-22)


### Bug Fixes

* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))

## [0.1.2](https://github.com/home-operations/flate/compare/0.1.1...0.1.2) (2026-05-22)


### Bug Fixes

* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))

## [0.1.1](https://github.com/home-operations/flate/compare/0.1.0...0.1.1) (2026-05-22)


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))

## [0.1.0](https://github.com/home-operations/flate/compare/0.0.4...0.1.0) (2026-05-21)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))

## [0.0.4](https://github.com/home-operations/flate/compare/0.0.3...0.0.4) (2026-05-21)


### Features

* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))

## [0.0.3](https://github.com/home-operations/flate/compare/0.0.2...0.0.3) (2026-05-21)


### Bug Fixes

* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))

## [0.0.2](https://github.com/home-operations/flate/compare/0.0.1...0.0.2) (2026-05-21)


### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))


### Bug Fixes

* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))


### Miscellaneous Chores

* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))
