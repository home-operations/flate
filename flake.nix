{
  description = "A Flux resource validator and inflator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    {
      self,
      ...
    }@inputs:
    let
      inherit (inputs.nixpkgs) lib;

      # flate probably works for many more systems, but I can only confidently verify
      # this arch myself
      systems = [
        "x86_64-linux"
      ];

      systemPackages = lib.getAttrs systems inputs.nixpkgs.legacyPackages;

    in
    {
      packages = builtins.mapAttrs (system: pkgs: {
        default = self.packages.${system}.flate;

        # parse out the correct go derivation to use from go.mod
        # "go MAJOR.MINOR.PATCH" -> pkgs.go_MAJOR_MINOR, PATCH information is lost
        _projectGo =
          let
            goModLines = lib.splitString "\n" (builtins.readFile ./go.mod);
            findGoVersionStr =
              idx:
              let
                ln = builtins.elemAt goModLines idx;
                match = builtins.match "go ([0-9]+\.[0-9]+\.[0-9]+)" ln;
              in
              if match != null then builtins.head match else findGoVersionStr (idx + 1);
          in
          pkgs."go_${builtins.replaceStrings [ "." ] [ "_" ] (lib.versions.majorMinor (findGoVersionStr 0))}";

        flate = pkgs.callPackage ./nix/flate.nix {
          go = self.packages.${system}._projectGo;
          version = (builtins.fromJSON (builtins.readFile ./.release-please-manifest.json)).".";
        };
      }) systemPackages;

      devShells = builtins.mapAttrs (system: pkgs: {
        default = pkgs.mkShell {
          packages = [
            self.packages.${system}._projectGo
          ];
        };
      }) systemPackages;
    };
}
