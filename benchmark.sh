#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# FlowerMQ Benchmark — gRPC vs MQTT (vs REST), with optional scale test
#
# Runs quickstart-pytorch (FedAvg, 3 rounds) over each transport on the same
# machine and prints a comparison table.
#
# Usage:  ./benchmark.sh                # 2 nodes: gRPC + MQTT
#         ./benchmark.sh --scale        # 2 nodes + 10 nodes
#         ./benchmark.sh --rest         # include REST transport
#         ./benchmark.sh --scale --rest # everything
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
VENV="$REPO/.venv"
LOGS="/tmp/flowermq-bench"
CONTROL_PORT=9093
MQTT_PORT=1883
INCLUDE_REST=false
INCLUDE_SCALE=false
for arg in "$@"; do
    [[ "$arg" == "--rest" ]]  && INCLUDE_REST=true
    [[ "$arg" == "--scale" ]] && INCLUDE_SCALE=true
done

# ── helpers ──────────────────────────────────────────────────────────────────
info()  { printf '\033[0;32m[✓]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
fail()  { printf '\033[0;31m[✗]\033[0m %s\n' "$*"; exit 1; }

PIDS=()
kill_all() {
    for pid in "${PIDS[@]+"${PIDS[@]}"}"; do kill "$pid" 2>/dev/null || true; done
    PIDS=()
    pkill -f "flower-superexec"  2>/dev/null || true
    pkill -f "flwr-clientapp"    2>/dev/null || true
    pkill -f "flwr-serverapp"    2>/dev/null || true
    sleep 2
}

cleanup() {
    kill_all
    pkill -f "mosquitto" 2>/dev/null || true
    if [[ -f "$HOME/.flwr/config.toml.bak-bench" ]]; then
        mv "$HOME/.flwr/config.toml.bak-bench" "$HOME/.flwr/config.toml"
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

wait_port_free() {
    local port=$1 timeout=${2:-10} elapsed=0
    while lsof -i ":$port" >/dev/null 2>&1; do
        sleep 1; elapsed=$((elapsed + 1))
        [[ $elapsed -ge $timeout ]] && return 1
    done
}

# ── setup ────────────────────────────────────────────────────────────────────
MOSQUITTO=$(command -v mosquitto 2>/dev/null) \
    || MOSQUITTO=$(find /opt/homebrew -name mosquitto -type f 2>/dev/null | head -1) \
    || true
[[ -x "${MOSQUITTO:-}" ]] || fail "mosquitto not found"

if [[ ! -d "$VENV" ]]; then
    info "Creating virtualenv..."
    python3 -m venv "$VENV"
fi
PIP="$VENV/bin/pip"
FLWR="$VENV/bin/flwr"
SUPERLINK="$VENV/bin/flower-superlink"
SUPERNODE="$VENV/bin/flower-supernode"

EXTRAS="simulation"
$INCLUDE_REST && EXTRAS="simulation,rest"
info "Installing dependencies..."
$PIP install -q -e "$REPO/framework[$EXTRAS]" \
    "paho-mqtt>=2.0.0" torch torchvision "flwr-datasets[vision]" 2>&1 | tail -1

export PATH="$VENV/bin:$PATH"

mkdir -p "$HOME/.flwr"
[[ -f "$HOME/.flwr/config.toml" ]] && \
    cp "$HOME/.flwr/config.toml" "$HOME/.flwr/config.toml.bak-bench"
cat > "$HOME/.flwr/config.toml" << EOF
[superlink]
default = "bench"
[superlink.bench]
address = "127.0.0.1:$CONTROL_PORT"
insecure = true
EOF

rm -rf "$LOGS" && mkdir -p "$LOGS"

# ── results arrays ──────────────────────────────────────────────────────────
declare -a NAMES WALLS STIMES ACCS LOSSES NNODES

# ── run one experiment ──────────────────────────────────────────────────────
#  $1  label       e.g. "gRPC×2"
#  $2  fleet_type  e.g. "grpc-rere"
#  $3  sn_flag     e.g. "--grpc-rere"
#  $4  fleet_addr  e.g. "0.0.0.0:9092"
#  $5  sn_addr     e.g. "127.0.0.1:9092"
#  $6  num_nodes   e.g. 2
run_experiment() {
    local name=$1 fleet_type=$2 sn_flag=$3 fleet_addr=$4 sn_addr=$5 num_nodes=$6
    local run_dir="$LOGS/$name"
    mkdir -p "$run_dir"

    info "━━━ $name ($num_nodes nodes) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Mosquitto (only for MQTT)
    if [[ "$fleet_type" == "mqtt" ]]; then
        info "Starting Mosquitto..."
        "$MOSQUITTO" -v -p "$MQTT_PORT" > "$run_dir/mosquitto.log" 2>&1 &
        local mosq_pid=$!
        PIDS+=($mosq_pid)
        sleep 2
        kill -0 "$mosq_pid" 2>/dev/null || fail "Mosquitto failed to start"
    fi

    # SuperLink
    info "Starting SuperLink ($fleet_type)..."
    "$SUPERLINK" --insecure \
        --fleet-api-type "$fleet_type" \
        --fleet-api-address "$fleet_addr" \
        > "$run_dir/superlink.log" 2>&1 &
    PIDS+=($!)
    sleep 4

    # SuperNodes
    info "Starting $num_nodes SuperNodes..."
    for i in $(seq 0 $((num_nodes - 1))); do
        local port=$((9094 + i * 2))
        "$SUPERNODE" --insecure $sn_flag \
            --superlink "$sn_addr" \
            --clientappio-api-address "0.0.0.0:$port" \
            --node-config "partition-id=$i num-partitions=$num_nodes" \
            > "$run_dir/supernode${i}.log" 2>&1 &
        PIDS+=($!)
    done

    # Wait for all nodes to connect
    local connected=0
    for attempt in $(seq 1 30); do
        connected=0
        for i in $(seq 0 $((num_nodes - 1))); do
            grep -q "SuperNode ID" "$run_dir/supernode${i}.log" 2>/dev/null \
                && connected=$((connected + 1))
        done
        [[ $connected -ge $num_nodes ]] && break
        sleep 1
    done
    [[ $connected -ge $num_nodes ]] \
        || fail "$name: Only $connected/$num_nodes nodes connected"
    info "$connected SuperNodes connected"

    # FL training
    info "Running quickstart-pytorch..."
    local t_start t_end
    t_start=$(date +%s)

    cd "$REPO/examples/quickstart-pytorch"
    "$FLWR" run . --stream > "$run_dir/flwr_run.log" 2>&1 || true
    cd "$REPO"

    t_end=$(date +%s)
    local wall=$((t_end - t_start))

    # Parse results
    local stime acc loss rounds
    stime=$(grep -oE 'finished in [0-9.]+s' "$run_dir/flwr_run.log" \
        | grep -oE '[0-9.]+' || echo "?")
    acc=$(grep "'accuracy'" "$run_dir/flwr_run.log" | tail -1 \
        | grep -oE "'accuracy': '[^']+'" | grep -oE "[0-9.e+-]+" || echo "?")
    loss=$(grep "'loss'" "$run_dir/flwr_run.log" | tail -1 \
        | grep -oE "'loss': '[^']+'" | grep -oE "[0-9.e+-]+" || echo "?")
    rounds=$(grep -c "aggregate_train: Received" "$run_dir/flwr_run.log" || echo 0)

    info "$name: $rounds/3 rounds, ${stime}s strategy, ${wall}s wall, acc=$acc"

    NAMES+=("$name")
    WALLS+=("$wall")
    STIMES+=("$stime")
    ACCS+=("$acc")
    LOSSES+=("$loss")
    NNODES+=("$num_nodes")

    # Teardown
    kill_all
    if [[ "$fleet_type" == "mqtt" ]]; then
        pkill -f "mosquitto" 2>/dev/null || true
        wait_port_free "$MQTT_PORT" 10 || true
    fi
    wait_port_free "$CONTROL_PORT" 10 || warn "Port $CONTROL_PORT still in use"
    for i in $(seq 0 $((num_nodes - 1))); do
        wait_port_free $((9094 + i * 2)) 5 || true
    done
}

# ── run experiments ──────────────────────────────────────────────────────────
echo ""
echo "FlowerMQ Benchmark"
echo "════════════════════════════════════════════════════════════════════"
echo "  Workload:  quickstart-pytorch | FedAvg | 3 rounds | CIFAR-10"
echo ""

# --- 2-node experiments ---
run_experiment "gRPC"  "grpc-rere"  "--grpc-rere"  "0.0.0.0:9092"  "127.0.0.1:9092"  2
run_experiment "MQTT"  "mqtt"       "--mqtt"       "0.0.0.0:$MQTT_PORT"  "127.0.0.1:$MQTT_PORT"  2

if $INCLUDE_REST; then
    run_experiment "REST"  "rest"  "--rest"  "0.0.0.0:9095"  "http://127.0.0.1:9095"  2
fi

# --- 10-node experiments ---
if $INCLUDE_SCALE; then
    run_experiment "gRPC-10n"  "grpc-rere"  "--grpc-rere"  "0.0.0.0:9092"  "127.0.0.1:9092"  10
    run_experiment "MQTT-10n"  "mqtt"       "--mqtt"       "0.0.0.0:$MQTT_PORT"  "127.0.0.1:$MQTT_PORT"  10

    if $INCLUDE_REST; then
        run_experiment "REST-10n"  "rest"  "--rest"  "0.0.0.0:9095"  "http://127.0.0.1:9095"  10
    fi
fi

# ── comparison table ─────────────────────────────────────────────────────────
echo ""
echo "╔════════════╤═══════╤═══════════════╤════════════╤══════════════╤══════════════╗"
echo "║ Transport  │ Nodes │ Strategy (s)  │  Wall (s)  │   Accuracy   │     Loss     ║"
echo "╠════════════╪═══════╪═══════════════╪════════════╪══════════════╪══════════════╣"

for i in "${!NAMES[@]}"; do
    printf "║ %-10s │ %5s │ %13s │ %10s │ %12s │ %12s ║\n" \
        "${NAMES[$i]}" "${NNODES[$i]}" "${STIMES[$i]}" "${WALLS[$i]}" "${ACCS[$i]}" "${LOSSES[$i]}"
done

echo "╚════════════╧═══════╧═══════════════╧════════════╧══════════════╧══════════════╝"
echo ""
echo "Workload: quickstart-pytorch | FedAvg | 3 rounds | CIFAR-10"
echo "Logs: $LOGS/"
echo ""
info "Benchmark complete."
