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
            || (type == "regular" && baseNameOf path == "horizon-definition.datom")
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
        cargoArtifacts = craneLib.buildDepsOnly commonArguments;
        daemonCargoArtifacts = craneLib.buildDepsOnly commonArguments;
        bootstrapBinary = craneLib.buildPackage (
          commonArguments
          // {
            inherit cargoArtifacts;
              cargoExtraArgs = "--bin lojix-bootstrap";
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
            }
          );

          # Process-level startup witness for the exact CriomOS writer archive,
          # a missing configured SEMA store, both public authority tiers, and
          # a service-style terminate/restart cycle.
          fresh-daemon-startup = craneLib.cargoTest (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--test daemon_configuration";
            }
          );

          daemon-startup-rejects-inline-input =
            let
              package = self.packages.${system}.default;
            in
            pkgs.runCommand "lojix-daemon-startup-rejects-inline-input" { } ''
              set +e
              ${package}/bin/lojix-daemon '(NotAStartupArchive)' >stdout 2>stderr
              status=$?
              set -e
              if [ "$status" -eq 0 ]; then
                echo 'lojix-daemon accepted inline startup input' >&2
                exit 1
              fi
              if ! grep -Eq 'ExpectedSignalFile|signal file|DaemonRejected' stderr; then
                echo 'lojix-daemon rejection did not name the signal-file startup boundary' >&2
                cat stderr >&2
                exit 1
              fi
              printf 'lojix daemon rejects inline startup input\n' > "$out"
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
              startup = pkgs.runCommand "lojix-testactivation-startup.rkyv" {
                nativeBuildInputs = [ package ];
              } ''
                lojix-write-configuration "ConfigurationWriteRequest.{/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix /var/lib/lojix/lojix.sema atlas NoTestDefaults $out}"
              '';
              proposal = "{«atlas {EdgeTesting Large Max {Metal Some.X86_64 4 None None None None None None None None []} {Qwerty Uefi «» [] None} {AAA= Some.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Some.{aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa “200::1” 300:ca41:6b12:fba}} [] None None False False [] False False None None []} beacon {EdgeTesting Large Max {Pod Some.X86_64 4 None None Some.atlas Some.operator None None None None []} {Qwerty Uefi «» [] None} {AAA= Some.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA Some.{aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa “200::1” 300:ca41:6b12:fba}} [] None None False False [] False False None None []}» «» «» {Max «» «» «»} {criome []}}";
            in
            pkgs.testers.nixosTest {
              name = "lojix-same-host-test-activation";
              nodes.machine = { ... }: {
                environment.systemPackages = [ package fake-nix fake-ssh candidate ];
                systemd.services.lojix = {
                  wantedBy = [ "multi-user.target" ];
                  serviceConfig = {
                    ExecStart = "${package}/bin/lojix-daemon ${startup}";
                    Restart = "always";
                    KillMode = "control-group";
                    StateDirectory = "lojix";
                    Environment = "PATH=${pkgs.lib.makeBinPath [ fake-nix fake-ssh pkgs.systemd pkgs.coreutils candidate ]}";
                  };
                  preStart = ''
                    printf '%s' '${proposal}' > /var/lib/lojix/horizon-definition.datom
                  '';
                };
              };
              testScript = ''
                start_all()
                machine.wait_for_unit("lojix.service")
                machine.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/owner.sock")
                profile_before = machine.succeed("readlink -f /nix/var/nix/profiles/system").strip()
                predecessor_invocation = machine.succeed("systemctl show lojix.service --property=InvocationID --value").strip()
                machine.succeed("LOJIX_OWNER_SOCKET=/run/lojix/owner.sock ${package}/bin/meta-lojix 'Deploy.Host.(fixture-cluster atlas BaseHost /var/lib/lojix/horizon-definition.datom github:fixture-owner/fixture-flake?ref=main (ssh-ng://fixture-copy.invalid fixture-login@fixture-activate.invalid) Direct (checks.fixture-a) NixosSystemdBootV1 TestActivation ResolveAndRecord None [])' >/run/lojix-admission")
                machine.log(machine.succeed("cat /run/lojix-admission"))
                machine.log(machine.succeed("cat /run/lojix-fake-nix-argv"))
                machine.succeed("grep -F 'DeployAccepted.' /run/lojix-admission")
                machine.wait_until_succeeds("test -e /run/lojix-testactivation-candidate-entered")
                machine.wait_until_succeeds("test \"$(systemctl show lojix.service --property=InvocationID --value)\" != '" + predecessor_invocation + "'")
                machine.wait_for_unit("lojix.service")
                machine.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/owner.sock")
                machine.wait_until_succeeds("systemctl show lojix-self-switch-deploy-1.service --property=Result --value | grep -Fx success")
                machine.log(machine.succeed("systemctl show lojix-self-switch-deploy-1.service --property=LoadState --property=ActiveState --property=SubState --property=Result"))
                machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.(1)' >/run/lojix-deployment-before-wait")
                machine.log(machine.succeed("cat /run/lojix-deployment-before-wait"))
                machine.log(machine.succeed("journalctl -u lojix.service --since '2 minutes ago' --no-pager"))
                machine.wait_until_succeeds("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.(1)' | grep -Fq Succeeded")
                machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.(1)' >/run/lojix-deployment")
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
