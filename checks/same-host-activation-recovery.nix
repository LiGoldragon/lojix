{
  pkgs,
  package,
  horizonDefinition,
  completionTimeout ? 120,
}:
let
  candidate = pkgs.writeShellScriptBin "switch-to-configuration" ''
    printf '%s\n' entered > /run/activation-entered
    read -r outcome < /run/activation-release
    printf 'fixture activation exit=%s\n' "$outcome" >&2
    exit "$outcome"
  '';
  fakeNix = pkgs.writeShellScriptBin "nix" ''
    case "$1" in
      flake) printf '%s\n' '{"url":"github:fixture-owner/fixture-flake?ref=main","locked":{"rev":"0123456789abcdef0123456789abcdef01234567"}}' ;;
      eval) printf '%s\n' '/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-activation-candidate.drv' ;;
      build) printf '%s\n' '${candidate}' ;;
      copy) exit 0 ;;
      *) exit 89 ;;
    esac
  '';
  fakeSsh = pkgs.writeShellScriptBin "ssh" ''
    for argument in "$@"; do command="$argument"; done
    exec /bin/sh -c "$command"
  '';
  # Subscribe before checking the socket. A refused stale socket waits for its
  # replacement event; there is no clock-driven connection loop.
  socketReady = pkgs.writeText "activation-socket-ready.py" ''
    import ctypes, os, socket
    library = ctypes.CDLL(None, use_errno=True)
    descriptor = library.inotify_init1(os.O_CLOEXEC)
    assert descriptor >= 0
    assert library.inotify_add_watch(descriptor, b"/run/lojix", 0x100 | 0x200 | 0x80) >= 0
    while True:
        connection = socket.socket(socket.AF_UNIX)
        try:
            connection.connect("/run/lojix/ordinary.sock")
            connection.close()
            break
        except (FileNotFoundError, ConnectionRefusedError):
            connection.close()
            os.read(descriptor, 65536)
    os.close(descriptor)
  '';
  # The real ordinary client supplies both the subscription-ready event and
  # the terminal event. FIFOs make each test boundary an announced event.
  watchDeployment = pkgs.writeText "activation-watch.py" ''
    import os, re, subprocess, sys
    identifier = sys.argv[1]
    client = "${package}/bin/lojix"
    process = subprocess.Popen([client, "WatchDeployments.{ None Some.fixture-cluster Some.atlas }"],
                               stdout=subprocess.PIPE, text=True, bufsize=1)
    opening = process.stdout.readline()
    match = re.search(r"Watching\.\{\s*(\d+)", opening)
    assert match, opening
    with open("/run/activation-watch-ready", "w") as ready:
        ready.write(opening)
    try:
        for event in process.stdout:
            if re.search(r"DeploymentPhaseObserved\.\{\s*" + identifier + r"\s", event) and (
                "Some.Succeeded" in event or "Some.Failed." in event):
                with open("/run/activation-terminal", "w") as terminal:
                    terminal.write(event)
                break
        else:
            raise AssertionError("watch ended before the terminal event")
    finally:
        subprocess.run([client, "Unwatch.{ " + match.group(1) + " }"], check=True)
        process.wait()
  '';
in
pkgs.testers.nixosTest {
  name = "lojix-same-host-activation-recovery";
  nodes.machine = {
    environment.systemPackages = [
      package
      fakeNix
      fakeSsh
      candidate
      pkgs.python3
    ];
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
              fakeNix
              fakeSsh
              pkgs.systemd
              pkgs.coreutils
              candidate
            ]
          }"
        ];
      };
      preStart = ''
        mkdir -p /run/lojix
        install -m 0644 ${horizonDefinition}/horizon-definition.datom /var/lib/lojix/horizon-definition.datom
      '';
    };
  };
  testScript = ''
    import json

    start_all()
    machine.wait_for_unit("lojix.service")
    machine.succeed("systemctl stop lojix.service")
    machine.succeed("${package}/bin/lojix-write-configuration 'ConfigurationWriteRequest.{/run/lojix/ordinary.sock 432 /run/lojix/meta.sock 384 /var/lib/lojix /var/lib/lojix/configured-lojix-store.db atlas TestDefaults.{fixture-cluster atlas Hermetic github:fixture-owner/fixture-flake x86_64-linux checks.fixture-a /var/lib/lojix/horizon-definition.datom} /run/lojix-startup.rkyv}'")
    machine.succeed("mkdir -p /etc/systemd/system/lojix.service.d; printf '%s\n' '[Service]' 'Environment=LOJIX_CONFIGURATION=/run/lojix-startup.rkyv' >/etc/systemd/system/lojix.service.d/configuration.conf")
    machine.succeed("systemctl daemon-reload; systemctl start lojix.service")

    def ready():
        machine.succeed("timeout ${toString completionTimeout} ${pkgs.python3}/bin/python ${socketReady}")

    def query(identifier):
        return machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByDeployment.{ " + str(identifier) + " }'")

    ready()
    # Each request reaches a held candidate only after the real profile write.
    # Killing the admitting daemon must neither kill that unit nor turn the
    # profile match into a public Succeeded.
    for identifier, candidate_exit in [(1, 0), (2, 37), (3, 0)]:
        if identifier == 3:
            machine.succeed("mount -t tmpfs tmpfs /sys/firmware; mkdir -p /sys/firmware/efi/efivars")
            machine.succeed("printf loader >/sys/firmware/efi/efivars/LoaderInfo-4a67b082-0a4c-41cf-b6c7-440b29bb8c4f; printf variable >/sys/firmware/efi/efivars/LoaderEntryDefault-4a67b082-0a4c-41cf-b6c7-440b29bb8c4f; mount -o remount,ro /sys/firmware")
        machine.succeed("mkfifo /run/activation-entered /run/activation-release /run/activation-watch-ready /run/activation-terminal")
        request = "Deploy.Host.{ fixture-cluster atlas BaseHost /var/lib/lojix/horizon-definition.datom NoSecrets github:fixture-owner/fixture-flake?ref=main { ssh-ng://fixture-copy.invalid root@fixture-activate.invalid } Direct { checks.fixture-a } NixosSystemdBootV1 ActivateNow ResolveAndRecord None [] }"
        admission = machine.succeed("LOJIX_OWNER_SOCKET=/run/lojix/meta.sock ${package}/bin/lojix-meta " + json.dumps(request))
        assert "DeployAccepted.{ " + str(identifier) + " " in admission, admission
        machine.succeed("timeout ${toString completionTimeout} cat /run/activation-entered")
        assert machine.succeed("readlink -f /nix/var/nix/profiles/system").strip() == "${candidate}"
        before = query(identifier)
        assert "Succeeded" not in before and "Failed" not in before, before
        old_pid = machine.succeed("systemctl show lojix.service -p MainPID --value").strip()
        machine.succeed("systemctl kill --kill-whom=all --signal=KILL lojix.service; systemctl start lojix.service")
        ready()
        assert machine.succeed("systemctl show lojix.service -p MainPID --value").strip() != old_pid
        held = query(identifier)
        assert "Succeeded" not in held and "Failed" not in held, held
        machine.succeed("systemctl show lojix-self-switch-deploy-" + str(identifier) + ".service -p ActiveState --value | grep -Fx activating")
        machine.succeed("systemd-run --unit=fixture-watch-" + str(identifier) + " --setenv=LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${pkgs.python3}/bin/python ${watchDeployment} " + str(identifier))
        machine.succeed("timeout ${toString completionTimeout} cat /run/activation-watch-ready")
        machine.succeed("printf '%s\n' " + str(candidate_exit) + " >/run/activation-release")
        event = machine.succeed("timeout ${toString completionTimeout} cat /run/activation-terminal")
        terminal = query(identifier)
        journal = machine.succeed("journalctl -u lojix-self-switch-deploy-" + str(identifier) + ".service --no-pager -o cat")
        if identifier == 1:
            assert "Some.Succeeded" in event and "Some.Succeeded" in terminal, terminal
            assert "not-applicable" in journal and "LoaderInfo-absent" in journal, journal
            generation = machine.succeed("LOJIX_ORDINARY_SOCKET=/run/lojix/ordinary.sock ${package}/bin/lojix 'Query.ByGeneration.{ 1 }'")
            assert "BaseHost" in generation and "Current" in generation, generation
        else:
            assert "Some.Failed." in event and "Some.Failed." in terminal, terminal
            assert "Succeeded" not in terminal, terminal
            assert "activation step failed command=" in journal, journal
            if identifier == 2:
                assert "exit=37" in journal and "switch-to-configuration switch" in journal, journal
            else:
                assert "bootctl set-default" in journal and "Read-only file system" in journal, journal
        # A second successor must preserve the already acknowledged outcome.
        machine.succeed("systemctl restart lojix.service")
        ready()
        assert query(identifier) == terminal
        machine.succeed("rm /run/activation-entered /run/activation-release /run/activation-watch-ready /run/activation-terminal")
  '';
}
