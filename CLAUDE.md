# CLAUDE.md — FlowerMQ: Native MQTT Transport for Flower

## What we are building

A fourth Fleet API transport for the Flower federated learning framework that uses MQTT 5.0 instead of gRPC or REST. When complete, users will be able to run:

```bash
flower-superlink --fleet-api-type mqtt --fleet-api-address localhost:1883
flower-supernode --mqtt --superlink localhost:1883
```

This enables federated learning over existing IoT infrastructure without a gRPC-to-MQTT proxy.

## Why this matters

Most IoT devices speak MQTT natively. Flower currently only supports gRPC and REST. Running FL on IoT devices today requires an awkward proxy (see AWS blog: https://aws.amazon.com/blogs/architecture/applying-federated-learning-for-ml-at-the-edge/). We eliminate that proxy by making Flower speak MQTT natively.

## Architecture overview

### Current Flower architecture (understand this first)

```
SuperLink (server)                    SuperNode (client)
├── ServerApp logic                   ├── ClientApp logic (user's ML code)
├── Strategy (FedAvg, etc.)           ├── Mods (middleware interceptors)
├── Fleet API ← THIS IS WHAT WE TOUCH├── Connection layer ← AND THIS
│   ├── grpc-rere (default)           │   ├── grpc-rere connection
│   ├── grpc-adapter                  │   ├── rest connection
│   └── rest (experimental)           │   └── [mqtt connection — NEW]
│   └── [mqtt — NEW]                  │
├── ServerAppIo API                   └── Communicates via Message objects
└── State (SQLite/in-memory)
```

### Our MQTT transport design

```
SuperLink                              MQTT Broker              SuperNode
   │                                  (Mosquitto)                  │
   │── publish TaskIns ──────────────► topic:                      │
   │   (model weights, config)         flower/{run_id}/            │
   │                                   tasks/{node_id}  ──────────►│
   │                                                               │
   │                                                    ◄── subscribe
   │                                                               │
   │                                   topic:                      │
   │◄─────────────────────────────── flower/{run_id}/   ◄──────────│
   │   receive TaskRes                 results/{node_id}           │
   │   (updated weights, metrics)      publish TaskRes ────────────│
```

Key inversion: Flower's Fleet API is PULL-based (SuperNodes poll for tasks). MQTT is PUSH-based (broker pushes to subscribers). We invert the communication pattern.

## Critical files to read (in this order)

### 1. Understand the Message abstraction (transport-agnostic)

These are the data structures we serialize over MQTT. They don't care about transport.

```
src/py/flwr/common/message.py          # Message class — the envelope
src/py/flwr/common/record/             # RecordDict, ParametersRecord, MetricRecord, ConfigRecord
src/py/flwr/common/serde.py            # Serialization/deserialization (protobuf currently)
```

### 2. Understand the current Fleet API server side

This is where SuperLink serves tasks to SuperNodes. We add an MQTT equivalent.

```
src/py/flwr/server/superlink/fleet/    # Fleet API implementations
src/py/flwr/server/superlink/fleet/grpc_rere/  # The gRPC-rere transport (study this most)
src/py/flwr/server/superlink/fleet/rest/       # REST transport (study for comparison)
```

Look for:
- How tasks (TaskIns) are created and queued
- How the server waits for SuperNodes to pull tasks
- How results (TaskRes) are received back
- The `start_*_fleet_api_grpc` or equivalent entry functions

### 3. Understand the SuperNode connection (client side)

This is the SuperNode's pull loop that we replace with MQTT subscription.

```
src/py/flwr/supernode/                  # SuperNode entry point and logic
src/py/flwr/client/                     # Client-side connection implementations
```

Look for:
- The connection loop: poll server → receive task → execute ClientApp → send result
- How `Message` objects are sent/received
- How reconnection and error handling work

### 4. Understand the CLI entry points

This is where `--fleet-api-type` is parsed and the correct transport is started.

```
src/py/flwr/server/app.py              # or wherever SuperLink CLI is defined
src/py/flwr/supernode/app.py           # SuperNode CLI
src/py/flwr/common/constant.py         # Transport type constants (TRANSPORT_TYPE_GRPC_RERE, etc.)
```

### 5. Understand the State layer

The SuperLink stores tasks in a state backend. Our MQTT transport interacts with this.

```
src/py/flwr/server/superlink/state/    # InMemoryState, SqliteState
```

Look for: how TaskIns and TaskRes are stored and retrieved.

### 6. Understand the new ObjectStore (Flower 1.20+)

Flower 1.20 introduced content-addressable object storage for handling large models (beyond gRPC's 2GB limit). Messages are broken into a tree of SHA256-hashed objects.

```
src/py/flwr/common/inflatable.py       # InflatableObject base class
src/py/flwr/server/superlink/fleet/    # Look for object push/pull RPCs
```

This is relevant because model weights are large and MQTT has payload limits too. Flower's chunking may already handle part of our problem.

## Technical challenges to solve

### Challenge 1: Pull-to-push inversion
- Current: SuperNode polls SuperLink via gRPC asking "any tasks?"
- MQTT: SuperLink publishes to topic, SuperNode subscribes
- Solution: SuperLink publishes TaskIns to `flower/{run_id}/tasks/{node_id}`. SuperNode subscribes on connect. When task arrives, SuperNode processes it and publishes TaskRes to `flower/{run_id}/results/{node_id}`.

### Challenge 2: Large payload chunking
- Model weights can be several MB to GB
- MQTT brokers have configurable max payload (Mosquitto default: 256MB, but practical limits are lower)
- Solution: Chunk large payloads into multiple MQTT messages with sequence numbers and reassembly. OR leverage Flower's existing ObjectStore chunking from v1.20.

### Challenge 3: QoS mapping
- MQTT QoS 0: fire and forget (unreliable)
- MQTT QoS 1: at least once (may duplicate)
- MQTT QoS 2: exactly once (highest overhead)
- Mapping: QoS 2 for model weight messages (aggregation correctness), QoS 1 for heartbeats/status, QoS 0 for monitoring/metrics

### Challenge 4: Synchronous FL over asynchronous MQTT
- FL rounds are synchronous: server waits for all selected clients before aggregating
- MQTT is asynchronous pub/sub
- Solution: MQTT 5.0 request-response correlation. SuperLink publishes task with correlation data, waits for matching response on results topic. Timeout handling for stragglers.

### Challenge 5: Node discovery and registration
- Current: SuperNodes connect to SuperLink via gRPC and register
- MQTT: SuperNodes subscribe to a registration topic, publish their capabilities
- Solution: `flower/{run_id}/register` topic for node announcements, `flower/{run_id}/heartbeat/{node_id}` for liveness

## MQTT topic hierarchy design

```
flower/
├── {run_id}/
│   ├── tasks/{node_id}        # SuperLink → SuperNode (TaskIns)
│   ├── results/{node_id}      # SuperNode → SuperLink (TaskRes)
│   ├── register/              # Node registration announcements
│   ├── heartbeat/{node_id}    # Liveness signals
│   ├── control/               # Training control (start/stop/abort)
│   └── status/                # Run status updates
```

## Dependencies to add

```
paho-mqtt>=2.0.0    # Python MQTT 5.0 client
```

## Files we will create

```
src/py/flwr/server/superlink/fleet/mqtt/
├── __init__.py
├── mqtt_fleet_api.py          # MQTT Fleet API server (SuperLink side)
├── mqtt_connection.py         # MQTT connection manager
├── mqtt_serde.py              # Message serialization for MQTT payloads
└── mqtt_config.py             # MQTT-specific configuration (broker, QoS, topics)

src/py/flwr/supernode/mqtt/    # or wherever client connections live
├── __init__.py
└── mqtt_connection.py         # MQTT connection for SuperNode (subscribe + publish)
```

## How to test during development

```bash
# Terminal 1: Start Mosquitto broker
mosquitto -v -p 1883

# Terminal 2: Start SuperLink with MQTT
flower-superlink --fleet-api-type mqtt --fleet-api-address localhost:1883

# Terminal 3: Start SuperNode 1
flower-supernode --mqtt --superlink localhost:1883

# Terminal 4: Start SuperNode 2
flower-supernode --mqtt --superlink localhost:1883

# Terminal 5: Start a Flower run
flwr run . --stream
```

## Coding guidelines

- Follow Flower's existing code style (ruff formatting, type hints everywhere)
- Match the patterns used in grpc-rere transport — don't reinvent abstractions
- The Message/RecordDict layer is sacred — never modify it, only serialize/deserialize it
- Add MQTT as an OPTION, don't break existing transports
- Write tests that mirror the existing transport tests

## What success looks like

1. `flower-superlink --fleet-api-type mqtt` starts an MQTT-based Fleet API
2. `flower-supernode --mqtt` connects to SuperLink via MQTT broker
3. Any existing Flower app (quickstart-pytorch, etc.) runs unchanged over MQTT
4. Benchmarks show lower latency than gRPC for IoT-scale deployments (50-500 nodes)
5. All existing Flower tests pass (we broke nothing)

## Reference: prior art to be aware of

- SDFLMQ (arXiv:2503.13624): Standalone MQTT-based FL framework. NOT Flower. Study their topic hierarchy design.
- FedComm (arXiv:2208.08764): Benchmarked MQTT vs gRPC for FL. MQTT was 2.5x faster on IoT devices.
- AWS proxy pattern (2021): Shows the current painful workaround our transport eliminates.
- NVIDIA FLARE integration: Shows how another framework (FLARE) integrated with Flower by routing gRPC. We go further by replacing gRPC entirely.
