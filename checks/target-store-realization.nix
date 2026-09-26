# Two machines, real Nix and real SSH: an owner request on `operator` deploys
# the node `target`, which is not the daemon host. Lojix must evaluate on the
# operator, copy only the derivation closure to the target, have the target's
# daemon build it, root the output on the target, skip the closure transfer,
# and activate. The request names a builder, which must be ignored and logged.
#
# The only fake boundary is the flake source: a thin `nix` wrapper records
# every call and points the fixture's `github:` reference at a local git
# repository, because the test machines have no network.
{
  pkgs,
  package,
  horizonDefinition,
}:
let
  sshKeys = import "${pkgs.path}/nixos/tests/ssh-keys.nix" pkgs;
  privateKey = sshKeys.snakeOilEd25519PrivateKey;
  busybox = pkgs.busybox;
  # The fixture output carries a `switch-to-configuration` that records the
  # activation it was asked for. The target has no sandbox so the builder can
  # be a store path that is not a derivation input.
  fixtureFlake = pkgs.writeText "flake.nix" ''
    {
      outputs = _: {
        packages.x86_64-linux.fixture = derivation {
          name = "lojix-target-store-fixture";
          system = "x86_64-linux";
          builder = "${busybox}/bin/sh";
          args = [
            "-c"
            "${busybox}/bin/mkdir -p $out/bin; ${busybox}/bin/printf '#!/bin/sh\\necho \"$1\" > /run/lojix-target-store-activated\\n' > $out/bin/switch-to-configuration; ${busybox}/bin/chmod +x $out/bin/switch-to-configuration"
          ];
        };
      };
    }
  '';
  recordingNix = pkgs.writeShellScriptBin "nix" ''
    printf '%s\n' "$*" >> /run/lojix-nix-calls
    arguments=()
    for argument in "$@"; do
      arguments+=("''${argument//github:fixture-owner\/fixture-flake/git+file:///var/lib/fixture-flake}")
    done
    exec ${pkgs.nix}/bin/nix "''${arguments[@]}"
  '';
  noSubstituters = {
    substituters = pkgs.lib.mkForce [ ];
  };
in
pkgs.testers.nixosTest {
  name = "lojix-target-store-realization";
  nodes = {
    operator =
      { ... }:
      {
        environment.systemPackages = [
          package
          pkgs.git
        ];
        nix.settings = noSubstituters // {
          experimental-features = [
            "nix-command"
            "flakes"
          ];
          # The operator never builds: a local build would fail here.
          max-jobs = 0;
        };
        programs.ssh.extraConfig = ''
          StrictHostKeyChecking no
          UserKnownHostsFile /dev/null
          IdentityFile /root/.ssh/fixture_key
        '';
        systemd.services.lojix = {
          wantedBy = [ "multi-user.target" ];
          serviceConfig = {
            ExecStart = "${package}/bin/lojix-nexus";
            Restart = "always";
            StateDirectory = "lojix";
            Environment = [
              "HOME=/root"
              "PATH=${
                pkgs.lib.makeBinPath [
                  recordingNix
                  pkgs.openssh
                  pkgs.git
                  pkgs.systemd
                  pkgs.coreutils
                ]
              }"
            ];
          };
          preStart = ''
            install -m 0644 ${horizonDefinition}/horizon-definition.datom /var/lib/lojix/horizon-definition.datom
          '';
        };
      };
    target =
      { ... }:
      {
        services.openssh.enable = true;
        users.users.root.openssh.authorizedKeys.keys = [ sshKeys.snakeOilEd25519PublicKey ];
        nix.settings = noSubstituters // {
          sandbox = false;
        };
        system.extraDependencies = [ busybox ];
      };
  };
  testScript = ''
    start_all()
    target.wait_for_unit("sshd.service")
    operator.succeed("install -D -m 0600 ${privateKey} /root/.ssh/fixture_key")
    operator.wait_until_succeeds("ssh -o BatchMode=yes root@target true")

    operator.succeed(
        "mkdir -p /var/lib/fixture-flake"
        " && cp ${fixtureFlake} /var/lib/fixture-flake/flake.nix"
        " && cd /var/lib/fixture-flake"
        " && git init -q -b main"
        " && git add flake.nix"
        " && git -c user.name=fixture -c user.email=fixture@fixture.invalid commit -q -m fixture"
    )

    operator.wait_for_unit("lojix.service")
    operator.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/meta.sock")
    operator.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock lojix 'Configure.{ /run/lojix/ordinary.sock 432 /run/lojix/meta.sock 384 /var/lib/lojix operator NoTestDefaults }' | grep -F Configured")
    operator.succeed("systemctl restart lojix.service")
    operator.wait_for_unit("lojix.service")
    operator.wait_until_succeeds("test -S /run/lojix/ordinary.sock && test -S /run/lojix/meta.sock")
    operator.succeed("rm -f /run/lojix-nix-calls")

    operator.succeed("LOJIX_OWNER_SOCKET=/run/lojix/meta.sock lojix-meta 'Deploy.Host.{ fixture-cluster target BaseHost /var/lib/lojix/horizon-definition.datom NoSecrets github:fixture-owner/fixture-flake?ref=main { ssh-ng://root@target root@target } Direct { packages.x86_64-linux.fixture } NixosSystemdBootV1 TestActivation ResolveAndRecord Some.@/etc/nix/machines [] }' >/run/lojix-admission")
    operator.log(operator.succeed("cat /run/lojix-admission"))
    operator.wait_until_succeeds("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock lojix 'Query.ByDeployment.{ 1 }' | grep -Eq 'Succeeded|Failed|Rejected'", timeout=600)
    record = operator.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock lojix 'Query.ByDeployment.{ 1 }'")
    calls = operator.succeed("cat /run/lojix-nix-calls")
    operator.log(record)
    operator.log(calls)
    operator.log(operator.succeed("journalctl -u lojix.service --no-pager | tail -n 60"))
    assert "Succeeded" in record, record

    # The target-side root names the output, which only the target holds.
    roots = target.succeed("ls /nix/var/nix/gcroots/lojix/operator").split()
    assert len(roots) == 1, roots
    output = target.succeed("readlink /nix/var/nix/gcroots/lojix/operator/" + roots[0]).strip()
    assert output.startswith("/nix/store/") and output.endswith("-lojix-target-store-fixture"), output
    target.succeed("nix-store --check-validity " + output)
    operator.fail("nix-store --check-validity " + output)

    # Evaluation stayed on the operator; the derivation closure went over; the
    # target built; the copy stage only checked presence; no builder was used.
    lines = calls.splitlines()
    evals = [line for line in lines if line.startswith("eval ")]
    assert evals and all("--store" not in line and "ssh-ng" not in line for line in evals), evals
    assert any(line.startswith("copy --derivation --to ssh-ng://root@target /nix/store/") for line in lines), lines
    assert any(line.startswith("build --no-link --print-out-paths --store ssh-ng://root@target /nix/store/") for line in lines), lines
    assert ("path-info --store ssh-ng://root@target " + output) in lines, lines
    assert not any("--substitute-on-destination" in line for line in lines), lines
    assert not any("--builders" in line or "/etc/nix/machines" in line for line in lines), lines

    # Activation ran on the target with the target-side output.
    target.succeed("grep -Fx test /run/lojix-target-store-activated")

    # The root holds the output through a garbage collection on the target.
    target.succeed("nix-collect-garbage")
    target.succeed("nix-store --check-validity " + output)

    journal = operator.succeed("journalctl -u lojix.service --no-pager")
    assert "BuilderIgnored.{ target TargetStore «@/etc/nix/machines» }" in journal, journal
    assert "TargetStoreRealized.{ target ssh-ng://root@target " + output in journal, journal
  '';
}
