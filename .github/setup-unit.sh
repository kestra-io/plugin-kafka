#!/usr/bin/env bash
set -euo pipefail

compose_file="docker-compose-ci.yml"
bootstrap_server="localhost:9092"

# Reset containers and volumes to ensure a clean Kafka state for each local/CI setup.
echo "[setup-unit] Resetting Kafka and Schema Registry containers..."
docker compose -f "${compose_file}" down -v --remove-orphans >/dev/null 2>&1 || true

# Start Kafka and Schema Registry used by plugin unit/integration tests.
echo "[setup-unit] Starting Kafka and Schema Registry containers..."
docker compose -f "${compose_file}" up -d

# Wait until Kafka accepts metadata commands before trying to inspect/enable features.
echo "[setup-unit] Waiting for Kafka to become ready..."
for attempt in $(seq 1 30); do
    if docker compose -f "${compose_file}" exec -T kafka kafka-topics --bootstrap-server "${bootstrap_server}" --list >/dev/null 2>&1; then
        echo "[setup-unit] Kafka is ready."
        break
    fi

    if [ "${attempt}" -eq 30 ]; then
        echo "[setup-unit] Kafka did not become ready in time." >&2
        exit 1
    fi

    echo "[setup-unit] Kafka not ready yet (attempt ${attempt}/30), retrying in 2s..."
    sleep 2
done

# `kafka-features describe` can transiently fail in CI even after basic metadata calls are ready.
get_share_feature_line() {
    local attempts=30
    local attempt describe_output share_line

    for attempt in $(seq 1 "${attempts}"); do
        if describe_output="$(docker compose -f "${compose_file}" exec -T kafka kafka-features --bootstrap-server "${bootstrap_server}" describe 2>&1)"; then
            share_line="$(printf '%s\n' "${describe_output}" | awk '/Feature: share.version/ {print; exit}')"
            if [ -n "${share_line}" ]; then
                printf '%s\n' "${share_line}"
                return 0
            fi

            echo "[setup-unit] share.version not visible yet (attempt ${attempt}/${attempts}), retrying in 2s..."
        else
            echo "[setup-unit] kafka-features describe failed (attempt ${attempt}/${attempts}), retrying in 2s..." >&2
            echo "${describe_output}" >&2
        fi

        sleep 2
    done

    echo "[setup-unit] Unable to retrieve share.version from kafka-features describe." >&2
    docker compose -f "${compose_file}" logs --no-color --tail=80 kafka >&2 || true
    return 1
}

# SHARE group tests require the broker share protocol feature to be enabled.
# Kafka 4.1+ supports it, but it may still be disabled depending on cluster state.
if ! share_feature_line_before="$(get_share_feature_line)"; then
    exit 1
fi

share_feature_level_before="$(printf '%s\n' "${share_feature_line_before}" | sed -n 's/.*FinalizedVersionLevel: \([0-9]\+\).*/\1/p')"
echo "[setup-unit] share.version before setup: ${share_feature_line_before}"

if [ -z "${share_feature_level_before}" ]; then
    echo "[setup-unit] Unable to parse FinalizedVersionLevel for share.version (before)." >&2
    exit 1
fi

if [ "${share_feature_level_before}" -lt 1 ]; then
    echo "[setup-unit] Enabling share group protocol (share.version=1)..."
    docker compose -f "${compose_file}" exec -T kafka kafka-features --bootstrap-server "${bootstrap_server}" upgrade --feature share.version=1
else
    echo "[setup-unit] share.version is already enabled."
fi

if ! share_feature_line_after="$(get_share_feature_line)"; then
    exit 1
fi

share_feature_level_after="$(printf '%s\n' "${share_feature_line_after}" | sed -n 's/.*FinalizedVersionLevel: \([0-9]\+\).*/\1/p')"
echo "[setup-unit] share.version after setup: ${share_feature_line_after}"

if [ -z "${share_feature_level_after}" ] || [ "${share_feature_level_after}" -lt 1 ]; then
    echo "[setup-unit] share.version is not enabled; share groups will not work." >&2
    exit 1
fi

# Ensure broker side protocols include SHARE support.
broker_protocols_line="$(docker compose -f "${compose_file}" exec -T kafka bash -lc "grep -E '^group.coordinator.rebalance.protocols=' /etc/kafka/kafka.properties /etc/kafka/server.properties 2>/dev/null | head -n 1" || true)"
echo "[setup-unit] broker protocols: ${broker_protocols_line}"
if ! printf '%s\n' "${broker_protocols_line}" | grep -q "share"; then
    echo "[setup-unit] SHARE is missing from group.coordinator.rebalance.protocols." >&2
    exit 1
fi

# Smoke test: a share consumer should receive a newly produced message.
smoke_topic="qa_share_smoke_$(date +%s)"
smoke_group="qa_share_smoke_group_$(date +%s)"
smoke_output_file="/tmp/share-smoke.out"

docker compose -f "${compose_file}" exec -T kafka kafka-topics \
    --bootstrap-server "${bootstrap_server}" \
    --create --if-not-exists --topic "${smoke_topic}" --partitions 1 --replication-factor 1 >/dev/null

docker compose -f "${compose_file}" exec -T kafka bash -lc "
    kafka-console-share-consumer \
      --bootstrap-server ${bootstrap_server} \
      --topic ${smoke_topic} \
      --group ${smoke_group} \
      --max-messages 1 \
      --timeout-ms 25000 \
      --property print.value=true \
      --property print.key=false > ${smoke_output_file} 2>&1
" &
smoke_consumer_pid=$!

# Share group activation can take a few seconds in a fresh KRaft node.
# Produce several messages over a short window so at least one arrives after activation.
for i in $(seq 1 12); do
    sleep 1
    docker compose -f "${compose_file}" exec -T kafka bash -lc "
        printf 'smoke-message-${i}\n' | kafka-console-producer --bootstrap-server ${bootstrap_server} --topic ${smoke_topic} >/dev/null
    "
done

wait "${smoke_consumer_pid}" || true

smoke_output="$(docker compose -f "${compose_file}" exec -T kafka bash -lc "cat ${smoke_output_file} 2>/dev/null || true")"
echo "[setup-unit] share smoke output:"
echo "${smoke_output}"

if ! printf '%s\n' "${smoke_output}" | grep -q "Processed a total of 1 messages"; then
    echo "[setup-unit] Share smoke test failed; share consumer did not read produced message." >&2
    exit 1
fi

echo "[setup-unit] Share group protocol is enabled and verified."
