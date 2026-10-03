{
  buildGoModule,
  go,
  lib,
  installShellFiles,

  version ? "git",
}:
(buildGoModule.override { inherit go; }) (finalAttrs: {
  pname = "flate";

  inherit version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.fileFilter (
      file:
      file.name == "go.mod"
      || file.name == "go.sum"
      || file.hasExt "go"
      || file.hasExt "proto"
      || file.hasExt "tmpl"
    ) ../.;
  };

  vendorHash = "sha256-N2BuuBSBzLf87g98FlTdWDFpeegsNmXLaxHPJHXRHQQ=";

  nativeBuildInputs = [
    installShellFiles
  ];

  postInstall = ''
    installShellCompletion --cmd flate \
      --bash <($out/bin/flate completion bash) \
      --fish <($out/bin/flate completion fish) \
      --zsh <($out/bin/flate completion zsh) \
  '';

  env.CGO_ENABLED = 0;

  # tests try to break out of the sandbox
  doCheck = false;

  ldflags = [
    "-s"
    "-w"

    "-X main.version=${finalAttrs.version}"
    "-X main.date=19700101"

    # if there exists a better way to do this, I don't know how
    "-X main.commit=HEAD"
  ];

  meta = {
    mainProgram = "flate";
    description = "A Flux resource validator and inflator";
    longDescription = ''
      Render and diff Flux GitOps repositories fully offline
      — one static binary, no cluster, no `kubectl`, no shellouts.
    '';
    license = lib.licenses.agpl3Only;
    homepage = "https://github.com/home-operations/flate";
    platforms = lib.platforms.unix;
  };
})
