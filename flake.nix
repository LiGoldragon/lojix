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
        toolchain = fenix.packages.${system}.complete.withComponents [
          "cargo"
          "rustc"
          "rustfmt"
          "clippy"
          "rust-analyzer"
          "rust-src"
        ];
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
        source = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            (craneLib.filterCargoSources path type)
            || (type == "regular" && pkgs.lib.hasSuffix ".schema" path)
            || (type == "regular" && pkgs.lib.hasSuffix ".dotos" path)
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
          # The bounded-effect runner starts every external command with
          # `setsid`; package checks need that executable in their sandbox.
          nativeBuildInputs = [ pkgs.util-linux ];
        };
        cargoArtifacts = craneLib.buildDepsOnly (
          commonArguments
          // {
            cargoExtraArgs = "--features dotos-text";
          }
        );
        daemonCargoArtifacts = craneLib.buildDepsOnly commonArguments;
        bootstrapBinary = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
            cargoExtraArgs = "--bin lojix-bootstrap --features dotos-text";
          }
        );
        bootstrapPackage = pkgs.symlinkJoin {
          name = "lojix-bootstrap";
          paths = [ bootstrapBinary ];
          nativeBuildInputs = [ pkgs.makeWrapper ];
          postBuild = ''
            wrapProgram "$out/bin/lojix-bootstrap" \
              --prefix PATH : ${
                pkgs.lib.makeBinPath [
                  pkgs.nix
                  pkgs.openssh
                  pkgs.systemd
                ]
              } \
              --set LOJIX_BOOTSTRAP_OPENSSH ${pkgs.openssh}/bin/ssh
          '';
        };
      in
      {
        packages = {
          default = craneLib.buildPackage (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--features dotos-text";
            }
          );

          daemon-binary = craneLib.buildPackage (
            commonArguments
            // {
              cargoArtifacts = daemonCargoArtifacts;
              cargoExtraArgs = "--bin lojix-daemon";
            }
          );

          # A maintained flake-owned bootstrap program.  The wrapper keeps the
          # exact Nix/systemd executables in the app closure; it never depends
          # on an installed Lojix daemon, old socket, or ambient Lojix store.
          lojix-bootstrap = bootstrapPackage;
        };

        apps.lojix-bootstrap = {
          type = "app";
          program = "${self.packages.${system}.lojix-bootstrap}/bin/lojix-bootstrap";
        };

        checks = {
          build = self.packages.${system}.default;

          daemon-binary = self.packages.${system}.daemon-binary;

          test = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--features dotos-text";
            }
          );

          daemon-startup-rejects-dotos =
            let
              package = self.packages.${system}.default;
            in
            pkgs.runCommand "lojix-daemon-startup-rejects-dotos" { } ''
              set +e
              ${package}/bin/lojix-daemon '(NotAStartupArchive)' >stdout 2>stderr
              status=$?
              set -e
              if [ "$status" -eq 0 ]; then
                echo 'lojix-daemon accepted inline DOTOS startup' >&2
                exit 1
              fi
              if ! grep -Eq 'ExpectedSignalFile|signal file|DaemonRejected' stderr; then
                echo 'lojix-daemon rejection did not name the signal-file startup boundary' >&2
                cat stderr >&2
                exit 1
              fi
              printf 'lojix daemon rejects DOTOS startup\n' > "$out"
            '';

          bootstrap-rejects-flags =
            let
              package = self.packages.${system}.lojix-bootstrap;
            in
            pkgs.runCommand "lojix-bootstrap-rejects-flags" { } ''
              set +e
              ${package}/bin/lojix-bootstrap --help >stdout 2>stderr
              status=$?
              set -e
              if [ "$status" -eq 0 ]; then
                echo 'lojix-bootstrap accepted a flag' >&2
                exit 1
              fi
              if ! grep -q 'BootstrapRejected' stderr; then
                echo 'lojix-bootstrap did not report its strict inline boundary' >&2
                cat stderr >&2
                exit 1
              fi
              printf 'lojix bootstrap rejects flags before effects\n' > "$out"
            '';

          fmt = craneLib.cargoFmt {
            src = source;
          };

          clippy = craneLib.cargoClippy (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets --features dotos-text -- -D warnings";
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
