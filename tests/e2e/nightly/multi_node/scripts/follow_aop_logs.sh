#!/usr/bin/env bash
# Follow only the original AOP leader, stopping as soon as its result is known.
set -euo pipefail

pod=$1
namespace=$2
run_prefix=$3
fail_tag=$4
output=$5
scratch=$(mktemp -d)
stream_pid=""
display_pid=""
stop_stream() {
    if [ -n "$stream_pid" ]; then
        kill "$stream_pid" 2>/dev/null || true
        wait "$stream_pid" 2>/dev/null || true
        stream_pid=""
    fi
    if [ -n "$display_pid" ]; then
        kill "$display_pid" 2>/dev/null || true
        wait "$display_pid" 2>/dev/null || true
        display_pid=""
    fi
}
cleanup() {
    stop_stream
    cat "$scratch"/logs >> "$output" 2>/dev/null || true
    rm -rf "$scratch"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

snapshot() {
    local retry state
    for ((retry=0; retry<3; retry++)); do
        if state=$(kubectl --request-timeout=30s get pod "$pod" -n "$namespace" \
            -o jsonpath='{.metadata.uid}{"|"}{.status.containerStatuses[?(@.name=="vllm-leader")].containerID}{"|"}{.status.containerStatuses[?(@.name=="vllm-leader")].restartCount}{"|"}{.status.containerStatuses[?(@.name=="vllm-leader")].state.running.startedAt}'); then
            printf '%s\n' "$state"
            return 0
        fi
        sleep 1
    done
    echo "::error::Unable to query leader state after three attempts." >&2
    return 1
}
initial=$(snapshot)
IFS='|' read -r initial_uid initial_id initial_restarts _ <<< "$initial"
if [ -z "$initial_uid" ] || [ -z "$initial_id" ] || [ -z "$initial_restarts" ]; then
    echo "::error::Cannot identify the AOP leader container."
    exit 1
fi
finished="AOP_RUN_FINISHED:${run_prefix}:"
check_result() {
    local line result=""
    # Only inspect a finite log snapshot whose container identity was checked
    # both before and after retrieval. Live stream data is not authoritative.
    if grep -Fq "$fail_tag" "$scratch/verified"; then
        exit 1
    fi
    # Inspect complete lines once; separate greps could race a newly written
    # success marker and misclassify it as a nonzero result.
    while IFS= read -r line; do
        if [[ "$line" == "$finished"* ]]; then
            result=${line#"$finished"}
        fi
    done < "$scratch/verified"
    if [ "$result" = 0 ]; then exit 0; fi
    if [ -n "$result" ]; then
        echo "::error::AOP leader reported failure."
        exit 1
    fi
}
recover_result() {
    local current after uid id restarts running retry
    local previous
    for ((retry=0; retry<3; retry++)); do
        current=$(snapshot)
        IFS='|' read -r uid id restarts running <<< "$current"
        previous=()
        if [ "$uid" != "$initial_uid" ]; then
            echo "::error::Leader pod was replaced; its original result is unavailable."
            exit 1
        fi
        if [ "$id" != "$initial_id" ] || [ "$restarts" != "$initial_restarts" ]; then
            if [[ "$restarts" =~ ^[0-9]+$ ]] && [ "$restarts" -eq "$((initial_restarts + 1))" ]; then
                previous=(--previous)
            else
                echo "::error::Original leader container logs are no longer available."
                exit 1
            fi
        fi
        if ! kubectl --request-timeout=30s logs "$pod" -c vllm-leader -n "$namespace" \
            "${previous[@]}" > "$scratch/candidate"; then
            sleep 1
            continue
        fi
        after=$(snapshot)
        if [ "$after" != "$current" ]; then
            # The fetch may contain a replacement container's result. Retry
            # with the appropriate --previous selection; never accept it.
            sleep 1
            continue
        fi
        cp "$scratch/candidate" "$scratch/verified"
        cat "$scratch/verified" >> "$output"
        check_result
        if [ "$current" != "$initial" ] || [ -z "$running" ]; then
            echo "::error::Leader ended without a completion result."
            exit 1
        fi
        return 0
    done
    echo "::error::Unable to retrieve stable leader logs after three attempts."
    exit 1
}

MAX_RECONNECTS=12
for ((attempt=0; attempt<=MAX_RECONNECTS; attempt++)); do
    : > "$scratch/logs"
    recover_result
    # Use a dedicated process so a result can stop a still-open log connection.
    kubectl logs -f "$pod" -c vllm-leader -n "$namespace" >> "$scratch/logs" &
    stream_pid=$!
    tail -n +1 -f "$scratch/logs" &
    display_pid=$!
    while kill -0 "$stream_pid" 2>/dev/null; do
        current=$(snapshot)
        if [ "$current" != "$initial" ] || grep -Fq -e "$finished" -e "$fail_tag" "$scratch/logs"; then
            stop_stream
            recover_result
        fi
        sleep 1
    done
    stop_stream
    recover_result
    cat "$scratch/logs" >> "$output"
    if [ "$attempt" -lt "$MAX_RECONNECTS" ]; then
        echo "[stream] reconnecting to the original AOP leader..."
        sleep 5
    fi
done
echo "::error::AOP log reconnection limit reached."
exit 1
