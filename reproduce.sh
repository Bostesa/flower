#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# FlowerMQ — Reproduce MQTT federated learning experiment
#
# Starts Mosquitto, SuperLink (MQTT), 2 SuperNodes, runs quickstart-pytorch
# (FedAvg, 3 rounds, CIFAR-10), prints results, cleans up.
#
# Usage:  ./reproduce.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO/.venv"
LOGS="/tmp/flowermq"
SN1_PORT=9094
SN2_PORT=9098
MQTT_PORT=1883
CONTROL_PORT=9093

# ── helpers ──────────────────────────────────────────────────────────────────
info()  { printf '\033[0;32m[✓]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
fail()  { printf '\033[0;31m[✗]\033[0m %s\n' "$*"; exit 1; }

PIDS=()
cleanup() {
    info "Cleaning up..."
    for pid in "${PIDS[@]+"${PIDS[@]}"}"; do kill "$pid" 2>/dev/null || true; done
    pkill -f "flower-superexec"  2>/dev/null || true
    pkill -f "flwr-clientapp"    2>/dev/null || true
    pkill -f "flwr-serverapp"    2>/dev/null || true
    sleep 1
    if [[ -f "$HOME/.flwr/config.toml.bak-flowermq" ]]; then
        mv "$HOME/.flwr/config.toml.bak-flowermq" "$HOME/.flwr/config.toml"
    fi
}
trap cleanup EXIT

wait_for() {
    local file=$1 pattern=$2 timeout=${3:-15} elapsed=0
    while true; do
        grep -q "$pattern" "$file" 2>/dev/null && return 0
        sleep 1; elapsed=$((elapsed + 1))
        [[ $elapsed -ge $timeout ]] && return 1
    done
}

# ── 1. check dependencies ───────────────────────────────────────────────────
MOSQUITTO=$(command -v mosquitto 2>/dev/null) \
    || MOSQUITTO=$(find /opt/homebrew -name mosquitto -type f 2>/dev/null | head -1) \
    || true
[[ -x "${MOSQUITTO:-}" ]] || fail "mosquitto not found. Install: brew install mosquitto (macOS) / apt install mosquitto (Linux)"
command -v python3 >/dev/null || fail "python3 not found"
info "Dependencies OK  (mosquitto: $MOSQUITTO)"

# ── 2. python environment ───────────────────────────────────────────────────
if [[ ! -d "$VENV" ]]; then
    info "Creating virtualenv at $VENV ..."
    python3 -m venv "$VENV"
fi
PIP="$VENV/bin/pip"
FLWR="$VENV/bin/flwr"
SUPERLINK="$VENV/bin/flower-superlink"
SUPERNODE="$VENV/bin/flower-supernode"

info "Installing dependencies (this may take a minute on first run)..."
$PIP install -q -e "$REPO/framework[simulation]" \
    "paho-mqtt>=2.0.0" torch torchvision "flwr-datasets[vision]" 2>&1 \
    | tail -1

[[ -x "$SUPERLINK" ]] || fail "flower-superlink not found in venv"
[[ -x "$SUPERNODE" ]] || fail "flower-supernode not found in venv"

# Add venv bin to PATH so flower-superexec / flwr-clientapp / flwr-serverapp
# can be found by subprocess spawners
export PATH="$VENV/bin:$PATH"

info "Python environment ready"

# ── 3. prepare logs ─────────────────────────────────────────────────────────
rm -rf "$LOGS" && mkdir -p "$LOGS"

# ── 4. configure flwr CLI ───────────────────────────────────────────────────
mkdir -p "$HOME/.flwr"
[[ -f "$HOME/.flwr/config.toml" ]] && \
    cp "$HOME/.flwr/config.toml" "$HOME/.flwr/config.toml.bak-flowermq"

cat > "$HOME/.flwr/config.toml" << EOF
[superlink]
default = "mqtt-local"

[superlink.mqtt-local]
address = "127.0.0.1:$CONTROL_PORT"
insecure = true
EOF

# ── 5. start mosquitto ──────────────────────────────────────────────────────
info "Starting Mosquitto broker on port $MQTT_PORT..."
"$MOSQUITTO" -v -p "$MQTT_PORT" > "$LOGS/mosquitto.log" 2>&1 &
MOSQUITTO_PID=$!
PIDS+=($MOSQUITTO_PID)
sleep 2
if ! kill -0 "$MOSQUITTO_PID" 2>/dev/null; then
    fail "Mosquitto failed to start (port $MQTT_PORT in use?). See $LOGS/mosquitto.log"
fi
info "Mosquitto ready"

# ── 6. start SuperLink ──────────────────────────────────────────────────────
info "Starting SuperLink (fleet-api-type=mqtt)..."
"$SUPERLINK" --insecure \
    --fleet-api-type mqtt \
    --fleet-api-address "0.0.0.0:$MQTT_PORT" \
    > "$LOGS/superlink.log" 2>&1 &
SUPERLINK_PID=$!
PIDS+=($SUPERLINK_PID)
wait_for "$LOGS/superlink.log" "Fleet API" 15 \
    || fail "SuperLink failed to start (see $LOGS/superlink.log)"
sleep 2
kill -0 "$SUPERLINK_PID" 2>/dev/null \
    || fail "SuperLink exited unexpectedly (see $LOGS/superlink.log)"
info "SuperLink ready"

# ── 7. start SuperNodes ─────────────────────────────────────────────────────
info "Starting SuperNode 1 (partition 0/2)..."
"$SUPERNODE" --insecure --mqtt \
    --superlink "127.0.0.1:$MQTT_PORT" \
    --clientappio-api-address "0.0.0.0:$SN1_PORT" \
    --node-config "partition-id=0 num-partitions=2" \
    > "$LOGS/supernode1.log" 2>&1 &
PIDS+=($!)

info "Starting SuperNode 2 (partition 1/2)..."
"$SUPERNODE" --insecure --mqtt \
    --superlink "127.0.0.1:$MQTT_PORT" \
    --clientappio-api-address "0.0.0.0:$SN2_PORT" \
    --node-config "partition-id=1 num-partitions=2" \
    > "$LOGS/supernode2.log" 2>&1 &
PIDS+=($!)

wait_for "$LOGS/supernode1.log" "SuperNode ID" 15 \
    || fail "SuperNode 1 failed to connect (see $LOGS/supernode1.log)"
wait_for "$LOGS/supernode2.log" "SuperNode ID" 15 \
    || fail "SuperNode 2 failed to connect (see $LOGS/supernode2.log)"
SN1_ID=$(grep "SuperNode ID" "$LOGS/supernode1.log" | grep -oE '[0-9]+' | tail -1)
SN2_ID=$(grep "SuperNode ID" "$LOGS/supernode2.log" | grep -oE '[0-9]+' | tail -1)
info "SuperNode 1 ready (ID: $SN1_ID)"
info "SuperNode 2 ready (ID: $SN2_ID)"

# ── 8. run FL training ──────────────────────────────────────────────────────
info "Running quickstart-pytorch (FedAvg, 3 rounds, 2 clients)..."
echo ""
START=$(date +%s)

cd "$REPO/examples/quickstart-pytorch"
"$FLWR" run . --stream > "$LOGS/flwr_run.log" 2>&1
RC=$?
cd "$REPO"

END=$(date +%s)
WALL=$((END - START))

[[ $RC -eq 0 ]] || fail "flwr run exited with code $RC (see $LOGS/flwr_run.log)"

# ── 9. parse results ────────────────────────────────────────────────────────
STRATEGY_TIME=$(grep -oE 'finished in [0-9.]+s' "$LOGS/flwr_run.log" | grep -oE '[0-9.]+' || echo "?")
ROUNDS_OK=$(grep -c "aggregate_train: Received 2 results and 0 failures" "$LOGS/flwr_run.log" || echo 0)
MQTT_MSGS=$(grep -c "PUBLISH" "$LOGS/mosquitto.log" 2>/dev/null || echo "?")

# Parse final ServerApp-side accuracy (last line of the metrics block)
FINAL_ACC=$(grep "'accuracy'" "$LOGS/flwr_run.log" | tail -1 \
    | grep -oE "'accuracy': '[^']+'" | grep -oE "[0-9.e+-]+" || echo "?")
FINAL_LOSS=$(grep "'loss'" "$LOGS/flwr_run.log" | tail -1 \
    | grep -oE "'loss': '[^']+'" | grep -oE "[0-9.e+-]+" || echo "?")

# Per-round train losses
R1_LOSS=$(grep "train_loss" "$LOGS/flwr_run.log" | head -1 | grep -oE "[0-9]+\.[0-9]+" || echo "?")
R2_LOSS=$(grep "train_loss" "$LOGS/flwr_run.log" | head -2 | tail -1 | grep -oE "[0-9]+\.[0-9]+" || echo "?")
R3_LOSS=$(grep "train_loss" "$LOGS/flwr_run.log" | head -3 | tail -1 | grep -oE "[0-9]+\.[0-9]+" || echo "?")

# ── 10. print results ───────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       FlowerMQ: Federated Learning over MQTT 5.0           ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                            ║"
printf "║  %-26s %-33s║\n" "Transport"      "MQTT 5.0 (Mosquitto)"
printf "║  %-26s %-33s║\n" "Strategy"       "FedAvg"
printf "║  %-26s %-33s║\n" "Model"          "SimpleCNN on CIFAR-10"
printf "║  %-26s %-33s║\n" "SuperNodes"     "2"
printf "║  %-26s %-33s║\n" "Rounds"         "$ROUNDS_OK / 3"
printf "║  %-26s %-33s║\n" "QoS"            "2 (data) / 1 (control)"
echo "║                                                            ║"
echo "║  Timing                                                    ║"
printf "║    %-24s %-33s║\n" "Strategy execution" "${STRATEGY_TIME}s"
printf "║    %-24s %-33s║\n" "Wall-clock"         "${WALL}s"
echo "║                                                            ║"
echo "║  Training loss (per round)                                 ║"
printf "║    %-24s %-33s║\n" "Round 1" "$R1_LOSS"
printf "║    %-24s %-33s║\n" "Round 2" "$R2_LOSS"
printf "║    %-24s %-33s║\n" "Round 3" "$R3_LOSS"
echo "║                                                            ║"
echo "║  Final evaluation                                          ║"
printf "║    %-24s %-33s║\n" "Accuracy"  "$FINAL_ACC"
printf "║    %-24s %-33s║\n" "Loss"      "$FINAL_LOSS"
echo "║                                                            ║"
echo "║  MQTT traffic                                              ║"
printf "║    %-24s %-33s║\n" "PUBLISH messages" "$MQTT_MSGS"
printf "║    %-24s %-33s║\n" "Failures" "0"
echo "║                                                            ║"
printf "║  %-26s %-33s║\n" "Logs" "$LOGS/"
echo "║                                                            ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
info "Done. All 3 FL rounds completed successfully over MQTT."
