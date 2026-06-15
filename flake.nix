{
  description = "lojix — daemon-based deploy orchestrator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        toolchain = fenix.packages.${system}.stable.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        source = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            (craneLib.filterCargoSources path type)
            || (type == "regular" && pkgs.lib.hasSuffix ".schema" path)
            || (
              type == "regular"
              && builtins.elem (baseNameOf path) [
                "AGENTS.md"
                "ARCHITECTURE.md"
                "INTENT.md"
                "skills.md"
              ]
            );
        };
        commonArguments = {
          src = source;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly (
          commonArguments
          // {
            cargoExtraArgs = "--features nota-text";
          }
        );
        daemonCargoArtifacts = craneLib.buildDepsOnly commonArguments;
      in
      {
        packages = {
          default = craneLib.buildPackage (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--features nota-text";
            }
          );

          daemon-binary = craneLib.buildPackage (
            commonArguments
            // {
              cargoArtifacts = daemonCargoArtifacts;
              cargoExtraArgs = "--bin lojix-daemon";
            }
          );
        };

        checks = {
          build = self.packages.${system}.default;

          daemon-binary = self.packages.${system}.daemon-binary;

          test = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--features nota-text";
            }
          );

          daemon-startup-rejects-nota =
            let
              package = self.packages.${system}.default;
            in
            pkgs.runCommand "lojix-daemon-startup-rejects-nota" { } ''
              set +e
              ${package}/bin/lojix-daemon '(ConfigurationWriteRequest (/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix /run/lojix/startup.rkyv))' >stdout 2>stderr
              status=$?
              set -e
              if [ "$status" -eq 0 ]; then
                echo 'lojix-daemon accepted inline NOTA startup' >&2
                exit 1
              fi
              if ! grep -Eq 'ExpectedSignalFile|signal file|DaemonRejected' stderr; then
                echo 'lojix-daemon rejection did not name the signal-file startup boundary' >&2
                cat stderr >&2
                exit 1
              fi
              printf 'lojix daemon rejects NOTA startup\n' > "$out"
            '';

          fmt = craneLib.cargoFmt {
            src = source;
          };

          clippy = craneLib.cargoClippy (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--all-targets --features nota-text -- -D warnings";
            }
          );
        };

        formatter = pkgs.nixfmt;

        devShells.default = pkgs.mkShell {
          name = "lojix";
          packages = [
            pkgs.jujutsu
            toolchain
          ];
        };
      }
    );
}
