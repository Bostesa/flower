#!/bin/bash
# Run this FIRST in your cloned Flower repo to map the relevant code for FlowerMQ
# Usage: bash explore.sh > codebase_map.md

echo "# Flower Codebase Map for FlowerMQ Development"
echo ""
echo "Generated: $(date)"
echo ""

echo "## 1. Repository structure (top 2 levels)"
echo '```'
find . -maxdepth 2 -type d | grep -v node_modules | grep -v __pycache__ | grep -v .git | grep -v .egg | sort | head -80
echo '```'
echo ""

echo "## 2. Fleet API server implementations (THIS IS WHAT WE EXTEND)"
echo '```'
find . -path "*/superlink/fleet*" -type f -name "*.py" | sort
echo '```'
echo ""

echo "## 3. SuperNode / client connection code (THE OTHER SIDE WE EXTEND)"
echo '```'
find . -path "*/supernode*" -type f -name "*.py" | sort
find . -path "*/client*" -type f -name "*.py" | grep -i "connection\|grpc\|rest" | sort
echo '```'
echo ""

echo "## 4. Message and Record abstractions (DO NOT MODIFY, ONLY SERIALIZE)"
echo '```'
find . -path "*/common/message*" -o -path "*/common/record*" -o -path "*/common/serde*" -o -path "*/common/inflatable*" | grep ".py$" | sort
echo '```'
echo ""

echo "## 5. Transport type constants"
echo '```'
grep -rn "TRANSPORT_TYPE\|fleet.api.type\|fleet-api-type\|grpc.rere\|grpc-rere" --include="*.py" -l | sort | head -20
echo '```'
echo ""

echo "## 6. CLI entry points"
echo '```'
find . -path "*/server/app.py" -o -path "*/supernode/app.py" -o -path "*/client/app.py" | grep ".py$" | sort
echo '```'
echo ""

echo "## 7. State layer (TaskIns/TaskRes storage)"
echo '```'
find . -path "*/superlink/state*" -type f -name "*.py" | sort
echo '```'
echo ""

echo "## 8. Existing transport constants and types"
echo '```'
grep -rn "TRANSPORT_TYPE" --include="*.py" src/ 2>/dev/null | head -20
echo '```'
echo ""

echo "## 9. Fleet API type selection logic"
echo '```'
grep -rn "fleet.api.type\|fleet_api_type\|grpc-rere\|--rest\|--grpc" --include="*.py" | grep -v __pycache__ | head -30
echo '```'
echo ""

echo "## 10. How grpc-rere Fleet API starts (entry function)"
echo '```'
grep -rn "def.*start.*fleet\|def.*run.*fleet\|def.*serve.*fleet" --include="*.py" | grep -v __pycache__ | head -20
echo '```'
echo ""

echo "## 11. gRPC-rere Fleet API server — FULL FILE"
echo "### (This is the primary file to study and mirror for MQTT)"
GRPC_FLEET=$(find . -path "*/fleet/grpc_rere*" -name "*.py" -not -name "__init__*" -not -path "*__pycache__*" | head -1)
if [ -n "$GRPC_FLEET" ]; then
    echo "File: $GRPC_FLEET"
    echo '```python'
    cat "$GRPC_FLEET"
    echo '```'
else
    echo "Could not find grpc_rere fleet file. Check manually: find . -path '*/fleet/*' -name '*.py'"
fi
echo ""

echo "## 12. SuperNode connection loop — FULL FILE"
echo "### (This is what we replace with MQTT subscription)"
SN_CONN=$(find . -path "*/supernode*" -name "*.py" | xargs grep -l "def connect\|def run\|pull.*task\|recv.*message" 2>/dev/null | head -1)
if [ -n "$SN_CONN" ]; then
    echo "File: $SN_CONN"
    echo '```python'
    cat "$SN_CONN"
    echo '```'
else
    echo "Could not auto-detect. Look in src/py/flwr/supernode/ for the main connection loop."
fi
echo ""

echo "## 13. Message class — FULL FILE"
MSG_FILE=$(find . -path "*/common/message.py" -not -path "*__pycache__*" | head -1)
if [ -n "$MSG_FILE" ]; then
    echo "File: $MSG_FILE"
    echo '```python'
    cat "$MSG_FILE"
    echo '```'
else
    echo "Could not find message.py. Check: find . -path '*/common/message.py'"
fi
echo ""

echo "## 14. Proto/protobuf definitions (if gRPC proto files exist)"
echo '```'
find . -name "*.proto" | sort | head -10
echo '```'
echo ""

echo "## 15. Existing tests for transports (mirror these for MQTT)"
echo '```'
find . -path "*test*" -name "*.py" | grep -i "fleet\|transport\|grpc\|connection" | sort | head -20
echo '```'
echo ""

echo "## Summary: files to create for FlowerMQ"
echo ""
echo "Based on the grpc-rere structure, create parallel MQTT files:"
echo ""
echo '```'
echo "src/py/flwr/server/superlink/fleet/mqtt/"
echo "├── __init__.py"
echo "├── mqtt_fleet_api.py        # Mirror of grpc-rere fleet API server"
echo "└── mqtt_config.py           # Broker address, QoS settings, topic patterns"
echo ""
echo "src/py/flwr/supernode/"
echo "└── mqtt_connection.py       # Mirror of grpc-rere connection, but subscribe instead of poll"
echo '```'

echo ""
echo "## Next step"
echo ""
echo "Feed this file AND the CLAUDE.md to Claude Code, then say:"
echo "  'Read CLAUDE.md and codebase_map.md, then implement the MQTT Fleet API transport starting with the server side.'"
