set -euo pipefail

required_environment() {
  name="$1"
  value="${!name:-}"
  if [ -z "$value" ]; then
    printf 'missing required environment variable: %s\n' "$name" >&2
    exit 2
  fi
}

required_environment LOJIX_SMOKE_CLUSTER
required_environment LOJIX_SMOKE_NODE
required_environment LOJIX_SMOKE_BUILDER
required_environment LOJIX_SMOKE_PROPOSAL_SOURCE
required_environment LOJIX_SMOKE_FLAKE_REFERENCE

root_directory="${LOJIX_SMOKE_ROOT:-$(mktemp -d /tmp/lojix-real-build-smoke-XXXXXX)}"
socket_path="$root_directory/lojix.sock"
daemon_configuration_path="$root_directory/daemon.nota"
cli_configuration_path="$root_directory/cli.nota"
state_directory="$root_directory/state"
gc_root_directory="$root_directory/gcroots"
daemon_log_path="$root_directory/daemon.log"
daemon_process_identifier=""
deployment=""

cleanup() {
  if [ -n "$daemon_process_identifier" ]; then
    kill "$daemon_process_identifier" 2>/dev/null || true
    wait "$daemon_process_identifier" 2>/dev/null || true
  fi
  if [ "${LOJIX_SMOKE_KEEP_ROOT:-0}" != "1" ] && [ -z "${LOJIX_SMOKE_ROOT:-}" ]; then
    rm -rf "$root_directory"
  else
    printf 'SMOKE_ROOT=%s\n' "$root_directory"
  fi
}

fail_with_daemon_log() {
  printf '%s\n' "$1" >&2
  if [ -s "$daemon_log_path" ]; then
    printf '%s\n' "daemon log:" >&2
    sed -n '1,200p' "$daemon_log_path" >&2
  fi
  exit 1
}

wait_for_socket() {
  for _attempt_number in $(seq 1 100); do
    if [ -S "$socket_path" ]; then
      return
    fi
    if ! kill -0 "$daemon_process_identifier" 2>/dev/null; then
      fail_with_daemon_log "lojix-daemon exited before creating its socket"
    fi
    sleep 0.05
  done
  fail_with_daemon_log "lojix-daemon did not create its socket"
}

run_lojix() {
  lojix "$cli_configuration_path" "$1"
}

trap cleanup EXIT

mkdir -p "$root_directory"
printf '(LojixDaemonConfiguration "%s" 384 None "%s" "%s" [] operator %s)\n' \
  "$socket_path" \
  "$state_directory" \
  "$gc_root_directory" \
  "$LOJIX_SMOKE_CLUSTER" \
  > "$daemon_configuration_path"
printf '(LojixCliConfiguration "%s" Compact)\n' "$socket_path" > "$cli_configuration_path"

lojix-daemon "$daemon_configuration_path" >"$daemon_log_path" 2>&1 &
daemon_process_identifier="$!"
wait_for_socket

request="$(printf '(DeploymentSubmission %s %s "%s" "%s" (FullOsDeployment Build) (NamedBuilder %s) [])' \
  "$LOJIX_SMOKE_CLUSTER" \
  "$LOJIX_SMOKE_NODE" \
  "$LOJIX_SMOKE_PROPOSAL_SOURCE" \
  "$LOJIX_SMOKE_FLAKE_REFERENCE" \
  "$LOJIX_SMOKE_BUILDER")"
reply="$(run_lojix "$request")"
printf 'ACCEPT_REPLY=%s\n' "$reply"
deployment="$(printf '%s\n' "$reply" | sed -n 's/^(DeploymentAccepted \([^)]*\))$/\1/p')"
if [ -z "$deployment" ]; then
  fail_with_daemon_log "expected DeploymentAccepted reply"
fi

last_observations=""
for _attempt_number in $(seq 1 "${LOJIX_SMOKE_MAX_POLLS:-240}"); do
  last_observations="$(run_lojix "(DeploymentObservationSubscription None None $deployment)")"
  if printf '%s\n' "$last_observations" | grep --fixed-strings 'DeploymentBuilt' >/dev/null; then
    printf 'FINAL_OBSERVATIONS=%s\n' "$last_observations"
    break
  fi
  if printf '%s\n' "$last_observations" | grep --fixed-strings 'DeploymentFailed' >/dev/null; then
    printf 'FAILED_OBSERVATIONS=%s\n' "$last_observations" >&2
    fail_with_daemon_log "deployment failed"
  fi
  sleep "${LOJIX_SMOKE_POLL_SECONDS:-5}"
done

if ! printf '%s\n' "$last_observations" | grep --fixed-strings 'DeploymentBuilt' >/dev/null; then
  printf 'LAST_OBSERVATIONS=%s\n' "$last_observations" >&2
  fail_with_daemon_log "timed out waiting for DeploymentBuilt"
fi

generation_listing="$(run_lojix "(GenerationQuery $LOJIX_SMOKE_CLUSTER $LOJIX_SMOKE_NODE FullOs)")"
printf 'GENERATION_LISTING=%s\n' "$generation_listing"
if ! printf '%s\n' "$generation_listing" | grep --fixed-strings ' Built)' >/dev/null; then
  fail_with_daemon_log "generation query did not return a built generation"
fi

gc_root_path="$gc_root_directory/$LOJIX_SMOKE_CLUSTER/$LOJIX_SMOKE_NODE/full-os/built/$deployment"
if [ ! -L "$gc_root_path" ]; then
  fail_with_daemon_log "missing built-output GC root: $gc_root_path"
fi
gc_root_target="$(readlink "$gc_root_path")"
printf 'GC_ROOT=%s -> %s\n' "$gc_root_path" "$gc_root_target"
if ! printf '%s\n' "$generation_listing" | grep --fixed-strings "$gc_root_target" >/dev/null; then
  fail_with_daemon_log "generation query and GC root target disagree"
fi
