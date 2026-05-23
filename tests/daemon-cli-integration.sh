set -euo pipefail

socket_path="$TMPDIR/lojix-daemon's.sock"
daemon_log_path="$TMPDIR/lojix-daemon.log"
daemon_configuration_path="$TMPDIR/lojix-daemon.nota"
cli_configuration_path="$TMPDIR/lojix-cli.nota"
state_directory="$TMPDIR/lojix-state's"
gc_root_directory="$TMPDIR/lojix-gcroots"
horizon_configuration_source="$TMPDIR/horizon.nota"
daemon_process_identifier=
stalled_connection_process_identifier=

cleanup() {
  if [ -n "$stalled_connection_process_identifier" ]; then
    kill "$stalled_connection_process_identifier" 2>/dev/null || true
  fi
  if [ -n "$daemon_process_identifier" ]; then
    kill "$daemon_process_identifier" 2>/dev/null || true
    wait "$daemon_process_identifier" 2>/dev/null || true
  fi
  rm -f "$socket_path"
}

write_configuration_files() {
  printf '(LojixDaemonConfiguration [%s] 384 None [%s] [%s] [%s] [] operator goldragon)\n' \
    "$socket_path" \
    "$horizon_configuration_source" \
    "$state_directory" \
    "$gc_root_directory" \
    > "$daemon_configuration_path"
  printf '(LojixCliConfiguration [%s] Compact)\n' \
    "$socket_path" \
    > "$cli_configuration_path"
}

fail_with_daemon_log() {
  printf '%s\n' "$1" >&2
  if [ -s "$daemon_log_path" ]; then
    printf '%s\n' "daemon log:" >&2
    cat "$daemon_log_path" >&2
  fi
  exit 1
}

wait_for_socket() {
  for attempt_number in $(seq 1 100); do
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

expect_reply() {
  request_text="$1"
  expected_reply_text="$2"
  actual_reply_text="$(lojix "$cli_configuration_path" "$request_text")"
  if [ "$actual_reply_text" != "$expected_reply_text" ]; then
    printf '%s\n' "unexpected lojix reply" >&2
    printf '%s\n' "request:  $request_text" >&2
    printf '%s\n' "expected: $expected_reply_text" >&2
    printf '%s\n' "actual:   $actual_reply_text" >&2
    exit 1
  fi
}

expect_reply_from_stdin() {
  request_text="$1"
  expected_reply_text="$2"
  actual_reply_text="$(printf '%s\n' "$request_text" | lojix "$cli_configuration_path")"
  if [ "$actual_reply_text" != "$expected_reply_text" ]; then
    printf '%s\n' "unexpected lojix stdin reply" >&2
    printf '%s\n' "request:  $request_text" >&2
    printf '%s\n' "expected: $expected_reply_text" >&2
    printf '%s\n' "actual:   $actual_reply_text" >&2
    exit 1
  fi
}

trap cleanup EXIT

write_configuration_files
lojix-daemon "$daemon_configuration_path" >"$daemon_log_path" 2>&1 &
daemon_process_identifier="$!"
wait_for_socket

socket_file_mode="$(stat --format '%a' "$socket_path")"
if [ "$socket_file_mode" != "600" ]; then
  fail_with_daemon_log "expected daemon socket mode 600, got $socket_file_mode"
fi
if [ ! -d "$state_directory" ]; then
  fail_with_daemon_log "daemon did not create configured state directory"
fi
if [ ! -d "$gc_root_directory" ]; then
  fail_with_daemon_log "daemon did not create configured GC-root directory"
fi

expect_reply \
  "(GenerationQuery None None HomeOnly)" \
  "(GenerationListing [])"

expect_reply_from_stdin \
  "(GenerationQuery None None None)" \
  "(GenerationListing [])"

expect_reply \
  "(CacheRetentionObservationSubscription None)" \
  "(CacheRetentionObservationSubscriptionOpened (CacheRetentionObservationToken 1) [])"

tail -f /dev/null | socat - "UNIX-CONNECT:$socket_path" &
stalled_connection_process_identifier="$!"
sleep 0.1

timeout 2 lojix "$cli_configuration_path" "(GenerationQuery None None HomeOnly)" \
  | grep --fixed-strings --line-regexp "(GenerationListing [])" >/dev/null

mkdir "$out"
printf '%s\n' "daemon CLI integration passed" >"$out/result"
