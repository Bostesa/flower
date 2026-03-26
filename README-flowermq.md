# FlowerMQ: Native MQTT 5.0 Transport for Flower Federated Learning

A fourth Fleet API transport for the [Flower](https://flower.ai) federated
learning framework.  SuperLink and SuperNodes communicate over an MQTT 5.0
broker instead of gRPC or REST, enabling federated learning on IoT
infrastructure without a gRPC-to-MQTT proxy.

## Architecture

```
SuperLink                    MQTT Broker              SuperNode
   │                       (Mosquitto)                   │
   ├─ publish request ──────► flower/fleet/{method} ────►│
   │                                                     │
   │◄── publish response ── flower/fleet/response/{id} ◄─┤
   │   (MQTT 5.0 ResponseTopic + CorrelationData)        │
```

- **12 Fleet API methods** mapped to MQTT topics (`flower/fleet/{method}`)
- **MQTT 5.0 request-response** pattern with `ResponseTopic` and
  `CorrelationData` properties
- **QoS 2** for model weight messages (`pull_messages`, `push_messages`,
  `push_object`, `pull_object`) — exactly-once delivery prevents aggregation
  corruption
- **QoS 1** for control messages (heartbeat, node lifecycle, metadata)
- **Zero application changes** — existing Flower apps run unmodified

## Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| Python | >= 3.10 | Runtime |
| Flower | 1.20+ (this repo) | FL framework |
| paho-mqtt | >= 2.0.0 | MQTT 5.0 client |
| Mosquitto | any | MQTT broker |
| PyTorch + torchvision | any recent | quickstart-pytorch example |

Install Mosquitto:
```bash
brew install mosquitto        # macOS
sudo apt install mosquitto    # Ubuntu/Debian
```

## Quick start

```bash
# Terminal 1 — broker
mosquitto -v -p 1883

# Terminal 2 — server
pip install -e "./framework[simulation]" "paho-mqtt>=2.0.0"
flower-superlink --insecure --fleet-api-type mqtt --fleet-api-address 0.0.0.0:1883

# Terminal 3 — client 1
flower-supernode --insecure --mqtt --superlink 127.0.0.1:1883

# Terminal 4 — client 2
flower-supernode --insecure --mqtt --superlink 127.0.0.1:1883 \
    --clientappio-api-address 0.0.0.0:9098

# Terminal 5 — run training
cd examples/quickstart-pytorch && flwr run . --stream
```

## One-command reproduction

```bash
./reproduce.sh
```

Starts Mosquitto, SuperLink, 2 SuperNodes, runs quickstart-pytorch (FedAvg,
3 rounds, CIFAR-10), prints a results table, and cleans up.  Takes ~2 minutes.

## Benchmark (gRPC vs MQTT)

```bash
./benchmark.sh                # 2-node:  gRPC + MQTT
./benchmark.sh --scale        # 2-node + 10-node comparison
./benchmark.sh --scale --rest # include REST transport
```

Runs the same experiment over each transport sequentially on the same machine
and prints a comparison table:

```
╔════════════╤═══════╤═══════════════╤════════════╤══════════════╤══════════════╗
║ Transport  │ Nodes │ Strategy (s)  │  Wall (s)  │   Accuracy   │     Loss     ║
╠════════════╪═══════╪═══════════════╪════════════╪══════════════╪══════════════╣
║ gRPC       │     2 │         76.19 │         81 │   1.9050e-01 │   2.1157e+00 ║
║ MQTT       │     2 │         82.11 │         86 │   1.6960e-01 │   2.1531e+00 ║
║ gRPC-10n   │    10 │         76.03 │         80 │   2.1080e-01 │   2.1815e+00 ║
║ MQTT-10n   │    10 │         73.84 │         76 │   1.7460e-01 │   2.1860e+00 ║
╚════════════╧═══════╧═══════════════╧════════════╧══════════════╧══════════════╝
```

## Docker Compose

No local dependencies needed beyond Docker:

```bash
docker compose up --build                    # 2 SuperNodes
docker compose up --build --scale supernode=10   # 10 SuperNodes
```

Spins up Mosquitto broker, SuperLink, SuperNodes, and a runner that executes
quickstart-pytorch automatically.

## File structure

```
# New files (MQTT transport)
framework/py/flwr/server/superlink/fleet/mqtt/
├── __init__.py
└── mqtt_fleet_api.py              # Server-side: MQTT Fleet API (12 handlers)

framework/py/flwr/client/mqtt_client/
├── __init__.py
└── connection.py                  # Client-side: drop-in for grpc_request_response

# Modified files
framework/py/flwr/common/constant.py             # TRANSPORT_TYPE_MQTT, default address
framework/py/flwr/server/app.py                   # --fleet-api-type mqtt
framework/py/flwr/supernode/cli/flower_supernode.py   # --mqtt flag
framework/py/flwr/supernode/start_client_internal.py  # MQTT in _init_connection

# Scripts
reproduce.sh                      # One-command end-to-end demo
benchmark.sh                      # Transport comparison benchmark

# Docker
docker/Dockerfile                 # Python 3.12 + Flower + MQTT + PyTorch (CPU)
docker/mosquitto.conf             # Broker config (remote connections enabled)
docker-compose.yml                # Full stack: broker + superlink + supernodes + runner
```

## How it works

The MQTT transport reuses Flower's existing `message_handler` module — the
same business logic that powers gRPC and REST.  The server subscribes to
12 request topics and dispatches to the shared handlers.  Responses are
published to the client's `ResponseTopic` with matching `CorrelationData`.

Flower's `ObjectStore` already chunks large model weights into 5 MB
`ArrayChunk` objects, so individual MQTT payloads stay well within typical
broker limits (Mosquitto default: 256 MB `max_packet_size`).

The SuperNode main loop (`start_client_internal.py`) works unchanged — the
MQTT connection provides the same 8-function interface as gRPC:
`receive`, `send`, `get_run`, `get_fab`, `pull_object`, `push_object`,
`confirm_message_received`, plus the node ID.

## License

Apache-2.0 (same as Flower)
