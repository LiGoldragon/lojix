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
            || (type == "regular" && pkgs.lib.hasSuffix ".nota" path)
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

          # ============================================================
          # LIVE deploy-into-VM bracket proof (report 147 Track A).
          #
          # The full lojix live bracket — bring-up -> deploy -> assert ->
          # teardown — driven through the REAL daemon against a THROWAWAY
          # runNixOSTest guest, KVM-accelerated, host-untouched: the
          # `runNixOSTest` harness owns both transient guests, no SSH to
          # prometheus, no real host, no networking change (Spirit
          # 5hir5bnz/7let).
          #
          # `deployer` runs the lojix daemon (configured live_enabled = true,
          # live_guest_ip = <target>, live_closure = <a real store path the
          # deployer holds>) and submits a Live `(Test (Run …))` over the owner
          # socket via `meta-lojix`; the daemon copies the closure into `target`
          # over `nix copy --to ssh-ng://root@<target>`, activates it with
          # `switch-to-configuration test`, runs the guest-side assertion that
          # `/run/current-system` IS the deployed closure, tears the bracket
          # down, and records `Passed`. The deployer polls `(Query (ByTestRun
          # …))` via `lojix` until the durable row reaches `Passed`.
          #
          # x86_64-linux only — runNixOSTest needs KVM.
          live-deploy-bracket =
            let
              package = self.packages.${system}.default;
              # The target's reachable address on the test vlan. Pinned static so
              # the deployed closure can carry the IDENTICAL network config — the
              # deploy must keep the target reachable at the same IP after
              # `switch-to-configuration test`, or the assert's ssh (and the test
              # driver's backdoor) is severed. This exact config is shared between
              # the `target` node and `deployedSystem` below.
              targetAddress = "192.168.1.2";
              targetNetworking = {
                networking.useDHCP = false;
                networking.interfaces.eth1.ipv4.addresses = [
                  {
                    address = targetAddress;
                    prefixLength = 24;
                  }
                ];
              };
              # The target node's own configuration, factored out so the closure
              # under test is built from the EXACT same module the running target
              # boots — differing only by a marker file. That near-identity is what
              # keeps `switch-to-configuration test` a (near) no-op: the test
              # framework's serial-console backdoor service, sshd, networking, and
              # the nix daemon are byte-identical between the running generation and
              # the deployed closure, so the switch does not stop them and the box
              # stays both reachable (for the assert) and instrumentable (for the
              # driver's post-test `sync`). The marker proves a real switch happened
              # (the closure path changes), without disrupting the live services.
              targetBase = {
                services.openssh.enable = true;
                services.openssh.settings.PermitRootLogin = "yes";
                # The deploy step activates a closure on the guest, so the guest
                # carries switch-to-configuration (the nixosTest default strips it).
                system.switch.enable = true;
                # The ssh-ng receive runs `nix` on the guest; accept unsigned paths
                # pushed by the trusted deployer (throwaway sandbox, no real trust
                # boundary) and enable the nix-command feature.
                nix.settings.trusted-users = [ "root" ];
                nix.settings.require-sigs = false;
                nix.settings.experimental-features = [
                  "nix-command"
                  "flakes"
                ];
                # Pin eth1 static so the deployed closure carries the IDENTICAL
                # address and the switch keeps the box reachable.
                inherit (targetNetworking) networking;
              };
              # The closure under test: the target's own config plus a distinguishing
              # marker, evaluated through the SAME nixos-test node machinery (qemu-vm
              # + test instrumentation) the running target uses, so the deployed
              # toplevel is near-identical to what is already running. Built on the
              # deployer; the assert checks the target's /run/current-system becomes
              # exactly this path.
              deployedSystem =
                (pkgs.testers.runNixOSTest {
                  name = "lojix-deployed-closure-eval";
                  nodes.target =
                    { ... }:
                    {
                      imports = [ targetBase ];
                      environment.etc."lojix-live-deployed".text = "live-bracket-proof\n";
                    };
                  testScript = "";
                }).nodes.target.system.build.toplevel;
            in
            pkgs.testers.runNixOSTest {
              name = "lojix-live-deploy-bracket";
              nodes = {
                deployer =
                  { ... }:
                  {
                    virtualisation.cores = 2;
                    virtualisation.memorySize = 3072;
                    # The daemon + CLIs, and the closure under test, in the
                    # deployer's store so `nix copy --to` has it locally. The
                    # daemon spawns `nix` and `ssh` by name (as a real deploy
                    # host provides them), so both must be on its PATH.
                    environment.systemPackages = [
                      package
                      pkgs.openssh
                      pkgs.nix
                    ];
                    # The daemon runs `nix copy` (a `nix` subcommand) — enable the
                    # nix-command experimental feature system-wide so the daemon's
                    # spawned `nix` picks it up from /etc/nix/nix.conf, exactly as a
                    # real deploy host running modern nix commands does.
                    nix.settings.experimental-features = [
                      "nix-command"
                      "flakes"
                    ];
                    system.extraDependencies = [ deployedSystem ];
                  };
                target =
                  { ... }:
                  {
                    imports = [ targetBase ];
                    virtualisation.cores = 2;
                    virtualisation.memorySize = 2048;
                  };
              };
              testScript = ''
                start_all()
                deployer.wait_for_unit("multi-user.target")
                target.wait_for_unit("sshd.service")

                # Provision the deployer's root ssh key onto the target (the
                # deploy-key the real cycle would distribute).
                deployer.succeed('mkdir -p /root/.ssh && (test -f /root/.ssh/id_ed25519 || ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519)')
                pubkey = deployer.succeed("cat /root/.ssh/id_ed25519.pub").strip()
                target.succeed(f"mkdir -p /root/.ssh && echo '{pubkey}' >> /root/.ssh/authorized_keys")

                # The target's address is pinned static (targetNetworking) and
                # carried identically in the deployed closure, so the switch keeps
                # it reachable. Confirm the running target actually holds it before
                # the daemon deploys to it.
                target_ip = "${targetAddress}"
                target.wait_until_succeeds(f"ip -4 -o addr show eth1 | grep -qw {target_ip}", timeout=30)
                print(f"target_ip={target_ip}")

                # The closure under test — a real store path the deployer holds.
                closure = "${deployedSystem}"

                # Write the daemon's binary startup config: live ENABLED, the
                # target's IP, the closure under test. (The bring-up/teardown
                # runner is empty — the harness owns the guest.)
                deployer.succeed("mkdir -p /var/lib/lojix /run/lojix")
                write_request = (
                    "(ConfigurationWriteRequest ("
                    "/run/lojix/ordinary.sock 432 /run/lojix/owner.sock 384 /var/lib/lojix "
                    "(goldragon deployer Live github:LiGoldragon/CriomOS-test-cluster/horizon-test-vm "
                    f"[] True {target_ip} {closure}) /run/lojix/startup.rkyv)"
                    ")"
                )
                deployer.succeed(f"lojix-write-configuration '{write_request}'")

                # Start the daemon from its binary startup file. The daemon
                # spawns `nix` / `ssh` by name to deploy into and assert on the
                # guest, so it inherits the NixOS system PATH (systemd-run's
                # default minimal PATH lacks them) — exactly the environment a
                # real deploy host gives the daemon.
                # NIX_SSHOPTS so the daemon's `nix copy --to ssh-ng://…` accepts
                # the throwaway guest's fresh host key non-interactively (the
                # guest's key is new every boot; accepting it on first contact
                # does not weaken any real trust — same rationale as the
                # activate/assert ssh's StrictHostKeyChecking=accept-new).
                deployer.succeed(
                    "systemd-run --unit=lojix-daemon --collect "
                    "--setenv=PATH=/run/current-system/sw/bin "
                    "'--setenv=NIX_SSHOPTS=-o StrictHostKeyChecking=accept-new -o BatchMode=yes' "
                    "lojix-daemon /run/lojix/startup.rkyv"
                )
                deployer.wait_until_succeeds("test -S /run/lojix/owner.sock", timeout=30)
                deployer.wait_until_succeeds("test -S /run/lojix/ordinary.sock", timeout=30)

                # Submit a LIVE (Run …) over the owner socket via meta-lojix.
                # The CLIs take exactly one NOTA argument and resolve the socket
                # from LOJIX_OWNER_SOCKET / LOJIX_ORDINARY_SOCKET (no flags); the
                # daemon's config bound the default /run/lojix/{owner,ordinary}.sock,
                # which the CLI defaults to, so no override is needed.
                accepted = deployer.succeed(
                    "meta-lojix "
                    "'(Test (Run (goldragon (Nodes [target]) (OnHost deployer) Live)))'"
                )
                print(f"accepted={accepted}")
                assert "Tested" in accepted or "AcceptedTest" in accepted, f"submit not accepted: {accepted}"

                # Poll the durable test-run row over the ordinary socket until it
                # reaches a terminal outcome; assert it is Passed.
                import time
                deadline = time.time() + 180
                terminal = ""
                while time.time() < deadline:
                    reply = deployer.succeed(
                        "lojix "
                        "'(Query (ByTestRun (goldragon target None)))'"
                    )
                    if "Passed" in reply or "Failed" in reply:
                        terminal = reply
                        break
                    time.sleep(2)
                print(f"terminal={terminal}")
                assert "Passed" in terminal, f"live bracket did not record Passed: {terminal}"
                # The daemon only records Passed after its OWN guest-side assert
                # (readlink /run/current-system == closure) succeeded, so the
                # closure is genuinely the deployed one in the durable row.
                assert closure in terminal, (
                    f"Passed row does not carry the deployed closure: {terminal}"
                )

                # Independent witness: re-check the deploy from the DEPLOYER over a
                # fresh ssh into the target (not the target's serial backdoor — the
                # switch churned the target's services, so its backdoor shell is
                # unreliable; the network path is exactly what the deploy used).
                ssh = (
                    "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "
                    "-o ConnectTimeout=15 -o ServerAliveInterval=5 "
                    f"root@{target_ip}"
                )
                active = deployer.succeed(f"{ssh} readlink -f /run/current-system").strip()
                assert active == closure, f"target current-system {active} != deployed {closure}"
                deployer.succeed(f"{ssh} test -f /etc/lojix-live-deployed")
                print("LIVE BRACKET: submit -> deploy -> assert -> Passed, target activated")
              '';
            };

          fmt = craneLib.cargoFmt {
            src = source;
          };

          clippy = craneLib.cargoClippy (
            commonArguments
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets --features nota-text -- -D warnings";
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
