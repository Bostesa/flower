# Copyright 2025 Flower Labs GmbH. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Context manager for an MQTT connection to the Flower SuperLink."""


import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from logging import ERROR

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from flwr.common.constant import HEARTBEAT_CALL_TIMEOUT, HEARTBEAT_DEFAULT_INTERVAL
from flwr.common.logger import log
from flwr.common.message import Message, remove_content_from_message
from flwr.common.retry_invoker import RetryInvoker
from flwr.common.serde import (
    fab_from_proto,
    message_from_proto,
    message_to_proto,
    run_from_proto,
)
from flwr.common.typing import Fab, Run
from flwr.proto.fab_pb2 import GetFabRequest, GetFabResponse  # pylint: disable=E0611
from flwr.proto.fleet_pb2 import (  # pylint: disable=E0611
    ActivateNodeRequest,
    ActivateNodeResponse,
    DeactivateNodeRequest,
    PullMessagesRequest,
    PullMessagesResponse,
    PushMessagesRequest,
    PushMessagesResponse,
    RegisterNodeFleetRequest,
    UnregisterNodeFleetRequest,
)
from flwr.proto.heartbeat_pb2 import (  # pylint: disable=E0611
    SendNodeHeartbeatRequest,
    SendNodeHeartbeatResponse,
)
from flwr.proto.message_pb2 import (  # pylint: disable=E0611
    ConfirmMessageReceivedRequest,
    ObjectTree,
    PullObjectRequest,
    PullObjectResponse,
    PushObjectRequest,
)
from flwr.proto.node_pb2 import Node  # pylint: disable=E0611
from flwr.proto.run_pb2 import GetRunRequest, GetRunResponse  # pylint: disable=E0611
from flwr.supercore.heartbeat import HeartbeatSender
from flwr.supercore.primitives.asymmetric import generate_key_pairs, public_key_to_bytes

# MQTT constants
TOPIC_PREFIX = "flower/fleet"
STATUS_OK = b"\x00"
STATUS_ERROR = b"\x01"
DEFAULT_RPC_TIMEOUT = 60.0

# QoS levels — mirrors the server-side mapping.
# QoS 2 for model data (weight messages, objects) to prevent duplicates that
# would corrupt aggregation.  QoS 1 for control messages (heartbeat,
# lifecycle, metadata) where duplicates are harmless.
QOS_MODEL_DATA = 2
QOS_CONTROL = 1

METHOD_QOS: dict[str, int] = {
    "register_node": QOS_CONTROL,
    "activate_node": QOS_CONTROL,
    "deactivate_node": QOS_CONTROL,
    "unregister_node": QOS_CONTROL,
    "heartbeat": QOS_CONTROL,
    "pull_messages": QOS_MODEL_DATA,
    "push_messages": QOS_MODEL_DATA,
    "get_run": QOS_CONTROL,
    "get_fab": QOS_CONTROL,
    "push_object": QOS_MODEL_DATA,
    "pull_object": QOS_MODEL_DATA,
    "confirm_message": QOS_CONTROL,
}


class MqttRpcError(Exception):
    """Error raised when an MQTT RPC call fails."""


class _MqttConnection:
    """Low-level MQTT connection with request/response support.

    Manages an MQTT 5.0 client that publishes requests with ResponseTopic and
    CorrelationData properties, then waits for the correlated response on a
    client-specific response topic.
    """

    def __init__(
        self,
        broker_address: str,
        broker_port: int,
        mqtt_tls: tuple[str, str | None, str | None] | None = None,
    ) -> None:
        self._broker_address = broker_address
        self._broker_port = broker_port
        self._mqtt_tls = mqtt_tls  # (ca_certfile, certfile, keyfile)
        self._client_id = f"flower-supernode-{uuid.uuid4().hex[:8]}"
        self._response_topic = f"{TOPIC_PREFIX}/response/{self._client_id}"
        self._client: mqtt.Client | None = None
        self._pending: dict[bytes, threading.Event] = {}
        self._responses: dict[bytes, bytes] = {}
        self._connected = threading.Event()

    def connect(self) -> None:
        """Connect to the MQTT broker and subscribe to the response topic."""
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
            client_id=self._client_id,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Configure TLS if certificates are provided
        if self._mqtt_tls is not None:
            ca, cert, key = self._mqtt_tls
            self._client.tls_set(ca_certs=ca, certfile=cert, keyfile=key)

        try:
            self._client.connect(
                self._broker_address, self._broker_port, keepalive=300
            )
        except (ConnectionRefusedError, OSError) as e:
            raise MqttRpcError(
                f"Cannot connect to MQTT broker at "
                f"{self._broker_address}:{self._broker_port} — {e}. "
                f"Is Mosquitto running? Start it with: "
                f"mosquitto -v -p {self._broker_port}"
            ) from e
        self._client.loop_start()
        if not self._connected.wait(timeout=10.0):
            raise MqttRpcError(
                f"MQTT broker at {self._broker_address}:{self._broker_port} "
                f"accepted TCP connection but MQTT handshake timed out. "
                f"Ensure the broker supports MQTT 5.0."
            )

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None

    # pylint: disable=unused-argument
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        """Subscribe to the response topic on (re)connect."""
        if reason_code.is_failure:
            log(ERROR, "[MQTT] Connection failed: %s", reason_code)
            return
        # Subscribe at QoS 2 so the broker can deliver responses for any
        # method, including model-data RPCs that require exactly-once.
        client.subscribe(self._response_topic, qos=QOS_MODEL_DATA)
        self._connected.set()

    # pylint: disable=unused-argument
    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        """Log broker disconnection."""
        if reason_code.is_failure:
            log(
                ERROR,
                "[MQTT] Disconnected from broker (reason: %s)",
                reason_code,
            )

    def _on_message(
        self,
        client: mqtt.Client,  # pylint: disable=unused-argument
        userdata: object,  # pylint: disable=unused-argument
        msg: mqtt.MQTTMessage,
    ) -> None:
        """Match incoming responses to pending requests by CorrelationData."""
        correlation_data = getattr(msg.properties, "CorrelationData", None)
        if correlation_data is None:
            return
        self._responses[correlation_data] = msg.payload
        event = self._pending.get(correlation_data)
        if event is not None:
            event.set()

    def call(
        self,
        method: str,
        request_bytes: bytes,
        timeout: float = DEFAULT_RPC_TIMEOUT,
    ) -> bytes:
        """Make an RPC call using MQTT 5.0 request/response.

        Publishes the serialized request to ``flower/fleet/{method}`` with a
        ResponseTopic and CorrelationData, then blocks until the correlated
        response arrives or the timeout expires.

        Returns the deserialized response payload (status envelope stripped).
        Raises ``MqttRpcError`` on timeout or server-side error.
        """
        if self._client is None:
            raise MqttRpcError("Not connected to MQTT broker")

        correlation_id = uuid.uuid4().bytes
        event = threading.Event()
        self._pending[correlation_id] = event

        # Set MQTT 5.0 request-response properties
        props = Properties(PacketTypes.PUBLISH)
        props.ResponseTopic = self._response_topic
        props.CorrelationData = correlation_id

        qos = METHOD_QOS.get(method, QOS_CONTROL)
        self._client.publish(
            f"{TOPIC_PREFIX}/{method}",
            request_bytes,
            qos=qos,
            properties=props,
        )

        # Wait for correlated response
        if not event.wait(timeout=timeout):
            self._pending.pop(correlation_id, None)
            raise MqttRpcError(f"Timeout waiting for response on {method}")

        self._pending.pop(correlation_id, None)
        response_payload = self._responses.pop(correlation_id)

        # Parse status envelope: [status_byte][payload]
        if len(response_payload) < 1:
            raise MqttRpcError("Empty response received")

        status = response_payload[0:1]
        body = response_payload[1:]

        if status == STATUS_ERROR:
            raise MqttRpcError(body.decode("utf-8", errors="replace"))

        return body


@contextmanager
def mqtt_request_response(  # pylint: disable=R0914,R0915
    server_address: str,
    insecure: bool,  # pylint: disable=W0613
    retry_invoker: RetryInvoker,  # pylint: disable=W0613
    max_message_length: int = 0,  # pylint: disable=W0613
    root_certificates: bytes | str | None = None,  # pylint: disable=W0613
    authentication_keys: tuple | None = None,
    mqtt_tls: tuple[str, str | None, str | None] | None = None,
) -> Iterator[
    tuple[
        int,
        Callable[[], tuple[Message, ObjectTree] | None],
        Callable[[Message, ObjectTree, float], set[str]],
        Callable[[int], Run],
        Callable[[str, int], Fab],
        Callable[[int, str], bytes],
        Callable[[int, str, bytes], None],
        Callable[[int, str], None],
    ]
]:
    """Primitives for request/response-based interaction via MQTT.

    Drop-in replacement for ``grpc_request_response`` that communicates with
    the SuperLink's MQTT Fleet API instead of gRPC. The interface (8-tuple of
    callables) is identical so the SuperNode main loop works unchanged.

    Parameters
    ----------
    server_address : str
        MQTT broker address in ``host:port`` format (e.g. ``localhost:1883``).
    insecure : bool
        Accepted for API compatibility; not used (MQTT TLS is not yet
        implemented).
    retry_invoker : RetryInvoker
        Accepted for API compatibility; MQTT has built-in reconnection.
    max_message_length : int
        Accepted for API compatibility; not used.
    root_certificates : Optional[Union[bytes, str]]
        Accepted for API compatibility; not used.
    authentication_keys : Optional[tuple]
        EC key pair for node identity. If ``None``, a random pair is generated.
    """
    # Parse broker address
    host, port = _parse_mqtt_address(server_address)

    # Generate authentication keys if not provided (mirrors gRPC behavior)
    if authentication_keys is None:
        authentication_keys = generate_key_pairs()
    node_pk = public_key_to_bytes(authentication_keys[1])

    # Create MQTT connection
    conn = _MqttConnection(host, port, mqtt_tls=mqtt_tls)
    try:
        conn.connect()
    except MqttRpcError as e:
        log(ERROR, str(e))
        raise SystemExit(1) from None

    node: Node | None = None

    ###########################################################################
    # SuperNode functions
    ###########################################################################

    def send_node_heartbeat() -> bool:
        if node is None:
            log(ERROR, "Node instance missing")
            return False
        req = SendNodeHeartbeatRequest(
            node=node, heartbeat_interval=HEARTBEAT_DEFAULT_INTERVAL
        )
        try:
            res_bytes = conn.call(
                "heartbeat",
                req.SerializeToString(),
                timeout=HEARTBEAT_CALL_TIMEOUT,
            )
            res = SendNodeHeartbeatResponse()
            res.ParseFromString(res_bytes)
            if not res.success:
                raise RuntimeError(
                    "Heartbeat failed unexpectedly. The SuperLink does not "
                    "recognize this SuperNode."
                )
            return True
        except MqttRpcError:
            return False

    heartbeat_sender = HeartbeatSender(send_node_heartbeat)

    def register_node() -> None:
        """Register node with SuperLink."""
        conn.call(
            "register_node",
            RegisterNodeFleetRequest(public_key=node_pk).SerializeToString(),
        )

    def activate_node() -> int:
        """Activate node and start heartbeat."""
        req = ActivateNodeRequest(
            public_key=node_pk,
            heartbeat_interval=HEARTBEAT_DEFAULT_INTERVAL,
        )
        res_bytes = conn.call("activate_node", req.SerializeToString())
        res = ActivateNodeResponse()
        res.ParseFromString(res_bytes)

        nonlocal node
        node = Node(node_id=res.node_id)
        heartbeat_sender.start()
        return node.node_id

    def deactivate_node() -> None:
        """Deactivate node and stop heartbeat."""
        nonlocal node
        if node is None:
            return
        heartbeat_sender.stop()
        req = DeactivateNodeRequest(node_id=node.node_id)
        try:
            conn.call("deactivate_node", req.SerializeToString())
        except MqttRpcError:
            pass

    def unregister_node() -> None:
        """Unregister node from SuperLink."""
        nonlocal node
        if node is None:
            return
        req = UnregisterNodeFleetRequest(node_id=node.node_id)
        try:
            conn.call("unregister_node", req.SerializeToString())
        except MqttRpcError:
            pass
        node = None

    def receive() -> tuple[Message, ObjectTree] | None:
        """Pull a message with its ObjectTree from SuperLink."""
        if node is None:
            log(ERROR, "Node instance missing")
            return None
        req = PullMessagesRequest(node=node)
        try:
            res_bytes = conn.call("pull_messages", req.SerializeToString())
        except MqttRpcError:
            return None
        res = PullMessagesResponse()
        res.ParseFromString(res_bytes)
        if len(res.messages_list) == 0:
            return None
        in_message = message_from_proto(res.messages_list[0])
        object_tree = res.message_object_trees[0]
        return in_message, object_tree

    def send(
        message: Message, object_tree: ObjectTree, clientapp_runtime: float
    ) -> set[str]:
        """Send the message with its ObjectTree to SuperLink."""
        if node is None:
            log(ERROR, "Node instance missing")
            return set()
        if message.has_content():
            message = remove_content_from_message(message)
        req = PushMessagesRequest(
            node=node,
            messages_list=[message_to_proto(message)],
            message_object_trees=[object_tree],
            clientapp_runtime_list=[clientapp_runtime],
        )
        res_bytes = conn.call("push_messages", req.SerializeToString())
        res = PushMessagesResponse()
        res.ParseFromString(res_bytes)
        return set(res.objects_to_push)

    def get_run(run_id: int) -> Run:
        req = GetRunRequest(node=node, run_id=run_id)
        res_bytes = conn.call("get_run", req.SerializeToString())
        res = GetRunResponse()
        res.ParseFromString(res_bytes)
        return run_from_proto(res.run)

    def get_fab(fab_hash: str, run_id: int) -> Fab:
        req = GetFabRequest(node=node, hash_str=fab_hash, run_id=run_id)
        res_bytes = conn.call("get_fab", req.SerializeToString())
        res = GetFabResponse()
        res.ParseFromString(res_bytes)
        return fab_from_proto(res.fab)

    def pull_object(run_id: int, object_id: str) -> bytes:
        """Pull an object from the SuperLink."""
        if node is None:
            raise RuntimeError("Node instance missing")
        req = PullObjectRequest(node=node, run_id=run_id, object_id=object_id)
        res_bytes = conn.call("pull_object", req.SerializeToString())
        res = PullObjectResponse()
        res.ParseFromString(res_bytes)
        return res.object_content

    def push_object(run_id: int, object_id: str, contents: bytes) -> None:
        """Push an object to the SuperLink."""
        if node is None:
            raise RuntimeError("Node instance missing")
        req = PushObjectRequest(
            node=node,
            run_id=run_id,
            object_id=object_id,
            object_content=contents,
        )
        conn.call("push_object", req.SerializeToString())

    def confirm_message_received(run_id: int, object_id: str) -> None:
        """Confirm that the message has been received."""
        if node is None:
            raise RuntimeError("Node instance missing")
        req = ConfirmMessageReceivedRequest(
            node=node, run_id=run_id, message_object_id=object_id
        )
        conn.call("confirm_message", req.SerializeToString())

    try:
        register_node()
        node_id = activate_node()
        yield (
            node_id,
            receive,
            send,
            get_run,
            get_fab,
            pull_object,
            push_object,
            confirm_message_received,
        )
    except Exception as exc:  # pylint: disable=broad-except
        log(ERROR, exc)
    finally:
        try:
            if node is not None:
                deactivate_node()
                unregister_node()
        except MqttRpcError:
            pass
        conn.disconnect()


def _parse_mqtt_address(address: str) -> tuple[str, int]:
    """Parse an MQTT broker address into (host, port).

    Handles formats: ``host:port``, ``[::1]:port`` (IPv6), ``host`` (default
    port 1883).
    """
    # IPv6: [host]:port
    if address.startswith("["):
        bracket_end = address.index("]")
        host = address[1:bracket_end]
        if bracket_end + 1 < len(address) and address[bracket_end + 1] == ":":
            port_str = address[bracket_end + 2 :]
        else:
            port_str = "1883"
        return host, int(port_str)
    # host:port
    if ":" in address:
        parts = address.rsplit(":", 1)
        return parts[0], int(parts[1])
    # host only
    return address, 1883
