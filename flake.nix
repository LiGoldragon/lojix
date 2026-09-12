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
    # The same horizon-rs revision every workspace manifest pins. The VM
    # fixture below is composed by this revision's own `horizon-compose`, so a
    # fixture that has gone stale against the pinned schema fails the check at
    # build time instead of inside a booted guest.
    horizon = {
      url = "github:LiGoldragon/horizon-rs/40d04d2504fee619e9b2b2564b8a769a3a9d6049";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
      crane,
      horizon,
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
        # Read rather than repeat: the joined package's name drifted to 1.0.1
        # while the workspace moved on without it.
        workspaceVersion = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;
        source = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            (type == "directory")
            || (craneLib.filterCargoSources path type)
            || (type == "regular" && baseNameOf path == "horizon-definition.datom")
            || (type == "regular" && pkgs.lib.hasSuffix ".ethos" path)
            || (type == "regular" && pkgs.lib.hasSuffix ".sema" path)
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
        # The two trait laws are read off the Rust text, so their check needs
        # every `.rs` file plus the shell the check itself is written in —
        # a different set from what crane compiles.
        lawSource = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter =
            path: type:
            (type == "directory")
            || (type == "regular" && pkgs.lib.hasSuffix ".rs" path)
            || (type == "regular" && pkgs.lib.hasSuffix ".sh" path);
        };
        commonArguments = {
          src = source;
          strictDeps = true;
          # The bounded-effect runner starts every external command with
          # `setsid`; package checks need that executable in their sandbox.
          nativeBuildInputs = [ pkgs.util-linux ];
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArguments;
        nexusCargoArtifacts = craneLib.buildDepsOnly (
          commonArguments // { cargoExtraArgs = "-p lojix-nexus"; }
        );
        nexusPackage = craneLib.buildPackage (
          commonArguments
          // {
            cargoArtifacts = nexusCargoArtifacts;
            cargoExtraArgs = "-p lojix-nexus";
          }
        );
        ordinaryClientPackage = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
            cargoExtraArgs = "-p lojix-client";
          }
        );
        metaClientPackage = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
            cargoExtraArgs = "-p meta-lojix-client";
          }
        );
        offlineToolsPackage = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
            cargoExtraArgs = "-p lojix-offline-tools --bin lojix-write-configuration --bin lojix-inspect-store --bin lojix-reset-store --bin lojix-migrate-configuration";
          }
        );
        bootstrapBinary = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
            cargoExtraArgs = "-p lojix-offline-tools --bin lojix-bootstrap";
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
        completePackage = pkgs.symlinkJoin {
          name = "lojix-${workspaceVersion}";
          paths = [
            nexusPackage
            ordinaryClientPackage
            metaClientPackage
            offlineToolsPackage
            bootstrapPackage
          ];
        };
      in
      {
        packages = {
          default = completePackage;
          lojix-nexus = nexusPackage;
          lojix = ordinaryClientPackage;
          lojix-meta = metaClientPackage;
          offline-tools = offlineToolsPackage;

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

          nexus-binary = self.packages.${system}.lojix-nexus;

          test = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
            }
          );

          # Process-level zero-argument startup witness with isolated XDG
          # roots, a fresh Sema, both authority tiers, and restart persistence.
          fresh-daemon-startup = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p lojix-nexus --test daemon_configuration";
            }
          );

          # What a failed deployment leaves behind: the command that failed,
          # its exit status, and the bounded redacted detail, in the durable
          # record and in the event log.
          failure-evidence = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p lojix --test failure_evidence";
            }
          );

          # What the Nexus says when it cannot do what was asked: a copy
          # failure naming the copy stage, and a deploy refusal that names no
          # deployment instead of aborting the daemon.
          deploy-honesty = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "-p lojix --test deploy_honesty";
            }
          );

          nexus-startup-rejects-arguments =
            let
              package = self.packages.${system}.default;
            in
            pkgs.runCommand "lojix-nexus-startup-rejects-arguments" { } ''
              set +e
              ${package}/bin/lojix-nexus unexpected >stdout 2>stderr
              status=$?
              set -e
              if [ "$status" -eq 0 ]; then
                echo 'lojix-nexus accepted a startup argument' >&2
                exit 1
              fi
              if ! grep -q 'starts with no arguments' stderr; then
                echo 'lojix-nexus rejection did not name the zero-argument boundary' >&2
                cat stderr >&2
                exit 1
              fi
              printf 'lojix nexus rejects startup arguments\n' > "$out"
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

          no-free-functions =
            pkgs.runCommand "lojix-no-free-functions" { src = lawSource; }
              (builtins.readFile ./checks/no-free-functions.sh);

          clippy = craneLib.cargoClippy (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            }
          );

          # A retained transient is the PID-1 handoff receipt available after
          # the initiating service cgroup dies. `--wait` intentionally remains
          # blocked while `RemainAfterExit=yes` retains that receipt; stopping
          # the unit is the event that releases the waiter.
          retained-transient-semantics = pkgs.testers.nixosTest {
            name = "lojix-retained-transient-semantics";
            nodes.machine = { ... }: { };
            testScript = ''
              start_all()
              machine.succeed(
                  "systemd-run --unit=lojix-retained-result --no-block --service-type=oneshot --remain-after-exit ${pkgs.coreutils}/bin/true"
              )
              machine.wait_for_unit("lojix-retained-result.service")
              machine.succeed(
                  "systemctl show lojix-retained-result.service --property=LoadState --property=ActiveState --property=Result | grep -Fx 'LoadState=loaded' && systemctl show lojix-retained-result.service --property=LoadState --property=ActiveState --property=Result | grep -Fx 'ActiveState=active' && systemctl show lojix-retained-result.service --property=LoadState --property=ActiveState --property=Result | grep -Fx 'Result=success'"
              )
              machine.succeed(
                  "systemd-run --unit=lojix-retained-waiter --wait --service-type=oneshot --remain-after-exit ${pkgs.coreutils}/bin/true >/run/lojix-retained-waiter.out 2>&1 & echo $! >/run/lojix-retained-waiter.pid"
              )
              machine.wait_for_unit("lojix-retained-waiter.service")
              machine.succeed("kill -0 $(cat /run/lojix-retained-waiter.pid)")
              machine.succeed("systemctl stop lojix-retained-waiter.service")
              machine.wait_until_succeeds("! kill -0 $(cat /run/lojix-retained-waiter.pid)")
            '';
          };

          # An actual owner request drives the real daemon through Nix/SSH into
          # a same-host `test` candidate which replaces `lojix.service`. The
          # successor must expose both typed sockets, terminalize this exact
          # TestActivation, and leave the persistent system profile unchanged.
          # Nix and SSH are the only fake effect boundaries.
          same-host-test-activation =
            let
              package = self.packages.${system}.default;
              candidate = pkgs.writeShellScriptBin "switch-to-configuration" ''
                if [ "$1" = test ]; then
                  : > /run/lojix-testactivation-candidate-entered
                  systemctl restart lojix.service
                fi
              '';
              fake-nix = pkgs.writeShellScriptBin "nix" ''
                printf '%s\n' "$@" > /run/lojix-fake-nix-argv
                case "$1" in
                  flake) printf '%s\n' '{"url":"github:fixture-owner/fixture-flake?ref=main","locked":{"rev":"0123456789abcdef0123456789abcdef01234567"}}' ;;
                  eval) printf '%s\n' '/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-lojix-test-candidate.drv' ;;
                  build) printf '%s\n' '${candidate}' ;;
                  copy) exit 0 ;;
                  *) exit 89 ;;
                esac
              '';
              fake-ssh = pkgs.writeShellScriptBin "ssh" ''
                for argument in "$@"; do command="$argument"; done
                exec /bin/sh -c "$command"
              '';
              # The fixture Horizon definition is produced by the real
              # producer: the pinned horizon-rs `horizon-compose` reading the
              # authored HorizonConfiguration and ClusterDefinition beside this
              # flake. Nothing here restates the Horizon schema, so the fixture
              # cannot drift from the revision lojix pins.
              horizonDefinition =
                pkgs.runCommand "lojix-fixture-horizon-definition"
                  {
                    nativeBuildInputs = [ horizon.packages.${system}.horizon-compose ];
                  }
                  ''
                    mkdir -p "$out"
                    horizon-compose "Compose.{ ${./checks/horizon/horizon-configuration.datom} ${./checks/horizon/cluster-definition.datom} }" \
                      > "$out/horizon-definition.datom"
                  '';
            in
            pkgs.testers.nixosTest {
              name = "lojix-same-host-test-activation";
              nodes.machine = { ... }: {
                environment.systemPackages = [
                  package
                  fake-nix
                  fake-ssh
                  candidate
                ];
                systemd.services.lojix = {
                  wantedBy = [ "multi-user.target" ];
                  serviceConfig = {
                    ExecStart = "${package}/bin/lojix-nexus";
                    Restart = "always";
                    KillMode = "control-group";
                    StateDirectory = "lojix";
                    Environment = "PATH=${
                      pkgs.lib.makeBinPath [
                        fake-nix
                        fake-ssh
                        pkgs.systemd
                        pkgs.coreutils
                        candidate
                      ]
                    }";
                  };
                  preStart = ''
                    install -m 0644 ${horizonDefinition}/horizon-definition.datom /var/lib/lojix/horizon-definition.datom
                  '';
                };
              };
              testScript = ''
                start_all()
                machine.wait_for_unit("lojix.service")
                machine.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/meta.sock")
                # The composed fixture is read back through lojix's own
                # Horizon reader before anything else runs: a definition the
                # pinned horizon-lib refuses fails here, named, rather than
                # sitting inert in a Direct-mode request that never reads it.
                machine.succeed("${package}/bin/lojix-write-configuration 'ConfigurationWriteRequest.{/run/fixture-lojix/ordinary.sock 432 /run/fixture-lojix/owner.sock 384 /var/lib/fixture-lojix /var/lib/fixture-lojix/configured-lojix-store.db fixture-daemon TestDefaults.{fixture-cluster atlas Hermetic github:fixture-owner/fixture-flake x86_64-linux checks.fixture-a /var/lib/lojix/horizon-definition.datom} /run/lojix-fixture-startup.rkyv}' | grep -F 'ConfigurationWritten.'")
                machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Configure.{ /run/lojix/ordinary.sock 432 /run/lojix/meta.sock 384 /var/lib/lojix atlas NoTestDefaults }' | grep -F Configured")
                machine.succeed("systemctl restart lojix.service")
                machine.wait_for_unit("lojix.service")
                machine.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/meta.sock")
                profile_before = machine.succeed("readlink -f /nix/var/nix/profiles/system").strip()
                predecessor_invocation = machine.succeed("systemctl show lojix.service --property=InvocationID --value").strip()
                machine.succeed("LOJIX_OWNER_SOCKET=/run/lojix/meta.sock ${package}/bin/lojix-meta 'Deploy.Host.{ fixture-cluster atlas BaseHost /var/lib/lojix/horizon-definition.datom NoSecrets github:fixture-owner/fixture-flake?ref=main { ssh-ng://fixture-copy.invalid fixture-login@fixture-activate.invalid } Direct { checks.fixture-a } NixosSystemdBootV1 TestActivation ResolveAndRecord None [] }' >/run/lojix-admission")
                machine.log(machine.succeed("cat /run/lojix-admission"))
                machine.log(machine.succeed("cat /run/lojix-fake-nix-argv"))
                machine.succeed("grep -F 'DeployAccepted.' /run/lojix-admission")
                machine.wait_until_succeeds("test -e /run/lojix-testactivation-candidate-entered")
                machine.wait_until_succeeds("test \"$(systemctl show lojix.service --property=InvocationID --value)\" != '" + predecessor_invocation + "'")
                machine.wait_for_unit("lojix.service")
                machine.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/meta.sock")
                machine.wait_until_succeeds("systemctl show lojix-self-switch-deploy-1.service --property=Result --value | grep -Fx success")
                machine.log(machine.succeed("systemctl show lojix-self-switch-deploy-1.service --property=LoadState --property=ActiveState --property=SubState --property=Result"))
                machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.{ 1 }' >/run/lojix-deployment-before-wait")
                machine.log(machine.succeed("cat /run/lojix-deployment-before-wait"))
                machine.log(machine.succeed("journalctl -u lojix.service --since '2 minutes ago' --no-pager"))
                machine.wait_until_succeeds("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.{ 1 }' | grep -Fq Succeeded")
                machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.{ 1 }' >/run/lojix-deployment")
                machine.succeed("grep -Fq Succeeded /run/lojix-deployment")
                assert profile_before == machine.succeed("readlink -f /nix/var/nix/profiles/system").strip()
              '';
            };
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
