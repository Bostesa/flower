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
"""Fleet API server using MQTT 5.0 request-response pattern."""


import uuid
from collections.abc import Callable
from logging import DEBUG, ERROR, INFO, WARN

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from flwr.common.constant import PUBLIC_KEY_ALREADY_IN_USE_MESSAGE
from flwr.common.logger import log
from flwr.common.typing import InvalidRunStatusException
from flwr.proto.fab_pb2 import GetFabRequest  # pylint: disable=E0611
from flwr.proto.fleet_pb2 import (  # pylint: disable=E0611
    ActivateNodeRequest,
    DeactivateNodeRequest,
    PullMessagesRequest,
    PushMessagesRequest,
    RegisterNodeFleetRequest,
    UnregisterNodeFleetRequest,
)
from flwr.proto.heartbeat_pb2 import (  # pylint: disable=E0611
    SendNodeHeartbeatRequest,
)
from flwr.proto.message_pb2 import (  # pylint: disable=E0611
    ConfirmMessageReceivedRequest,
    PullObjectRequest,
    PushObjectRequest,
)
from flwr.proto.run_pb2 import GetRunRequest  # pylint: disable=E0611
from flwr.server.superlink.fleet.message_handler import message_handler
from flwr.server.superlink.linkstate import LinkStateFactory
from flwr.supercore.ffs import FfsFactory
from flwr.supercore.inflatable.inflatable_object import UnexpectedObjectContentError
from flwr.supercore.object_store import ObjectStoreFactory

# MQTT constants
TOPIC_PREFIX = "flower/fleet"
STATUS_OK = b"\x00"
STATUS_ERROR = b"\x01"

# Payload size threshold for logging a warning.  Mosquitto's default
# max_packet_size is 256 MB but many production brokers are configured lower.
# Flower's ObjectStore already chunks model weights into 5 MB ArrayChunks
# (see FLWR_PRIVATE_MAX_ARRAY_CHUNK_SIZE), so individual push_object /
# pull_object payloads should stay well below this limit.
MQTT_PAYLOAD_WARN_BYTES = 10 * 1024 * 1024  # 10 MB

# QoS levels per MQTT 5.0 spec:
#   QoS 0 = at most once (fire and forget)
#   QoS 1 = at least once (may duplicate)
#   QoS 2 = exactly once (highest overhead)
#
# Mapping rationale:
#   - Model weight messages (push/pull objects, push/pull messages) use QoS 2
#     because duplicated or lost weight updates corrupt aggregation.
#   - Heartbeats use QoS 1 — duplicates are harmless, but we need delivery.
#   - Node lifecycle (register, activate, deactivate, unregister) uses QoS 1
#     because the operations are idempotent or retried at a higher level.
#   - Run/FAB metadata uses QoS 1 — retried by the SuperNode if missing.
QOS_MODEL_DATA = 2  # pull_messages, push_messages, push_object, pull_object
QOS_CONTROL = 1  # heartbeat, register, activate, deactivate, unregister, etc.

# Map each RPC method to its QoS level
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


class MqttFleetApiServer:
    """Fleet API server that communicates over MQTT 5.0.

    Uses the MQTT 5.0 request-response pattern: clients publish requests to
    method-specific topics with a ResponseTopic and CorrelationData. The server
    processes each request using the shared message_handler module and publishes
    the serialized protobuf response back to the client's ResponseTopic.
    """

    def __init__(
        self,
        state_factory: LinkStateFactory,
        ffs_factory: FfsFactory,
        objectstore_factory: ObjectStoreFactory,
        broker_address: str,
        broker_port: int,
        mqtt_tls: tuple[str, str | None, str | None] | None = None,
        shared_group: str | None = None,
    ) -> None:
        self.state_factory = state_factory
        self.ffs_factory = ffs_factory
        self.objectstore_factory = objectstore_factory
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.mqtt_tls = mqtt_tls  # (ca_certfile, certfile, keyfile)
        self.shared_group = shared_group  # MQTT 5.0 shared subscription group
        self._client: mqtt.Client | None = None

        # Map method names to handler functions
        self._handlers: dict[str, Callable[[bytes], bytes]] = {
            "register_node": self._handle_register_node,
            "activate_node": self._handle_activate_node,
            "deactivate_node": self._handle_deactivate_node,
            "unregister_node": self._handle_unregister_node,
            "heartbeat": self._handle_heartbeat,
            "pull_messages": self._handle_pull_messages,
            "push_messages": self._handle_push_messages,
            "get_run": self._handle_get_run,
            "get_fab": self._handle_get_fab,
            "push_object": self._handle_push_object,
            "pull_object": self._handle_pull_object,
            "confirm_message": self._handle_confirm_message,
        }

    def start(self) -> None:
        """Start the MQTT Fleet API server (blocking).

        Connects to the MQTT broker, subscribes to all Fleet API request topics,
        and blocks in the MQTT network loop until ``stop()`` is called.
        """
        client_id = f"flower-superlink-{uuid.uuid4().hex[:8]}"
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
            client_id=client_id,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Configure TLS if certificates are provided
        if self.mqtt_tls is not None:
            ca, cert, key = self.mqtt_tls
            self._client.tls_set(ca_certs=ca, certfile=cert, keyfile=key)
            log(INFO, "Flower Fleet API (MQTT): TLS enabled (ca=%s)", ca)

        log(
            INFO,
            "Flower Fleet API (MQTT): Connecting to broker at %s:%s",
            self.broker_address,
            self.broker_port,
        )
        try:
            self._client.connect(
                self.broker_address, self.broker_port, keepalive=300
            )
        except (ConnectionRefusedError, OSError) as e:
            log(
                ERROR,
                "Flower Fleet API (MQTT): Cannot connect to MQTT broker at "
                "%s:%s — %s. Is Mosquitto running? Start it with: "
                "mosquitto -v -p %s",
                self.broker_address,
                self.broker_port,
                e,
                self.broker_port,
            )
            return
        self._client.loop_forever()

    def stop(self) -> None:
        """Stop the MQTT Fleet API server."""
        if self._client is not None:
            self._client.disconnect()

    # pylint: disable=unused-argument
    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        """Subscribe to all Fleet API request topics on connect."""
        if reason_code.is_failure:
            log(ERROR, "[MQTT] Connection failed: %s", reason_code)
            return

        for method_name in self._handlers:
            topic = f"{TOPIC_PREFIX}/{method_name}"
            # MQTT 5.0 shared subscriptions: $share/{group}/{topic}
            # The broker distributes messages across group members,
            # enabling horizontal scaling of SuperLink instances.
            sub_topic = topic
            if self.shared_group:
                sub_topic = f"$share/{self.shared_group}/{topic}"
            qos = METHOD_QOS.get(method_name, QOS_CONTROL)
            client.subscribe(sub_topic, qos=qos)
            log(DEBUG, "[MQTT] Subscribed to %s (QoS %d)", sub_topic, qos)

        log(
            INFO,
            "Flower Fleet API (MQTT): Ready on %s:%s",
            self.broker_address,
            self.broker_port,
        )

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
                WARN,
                "[MQTT] Disconnected from broker (reason: %s). "
                "Will attempt to reconnect.",
                reason_code,
            )
        else:
            log(INFO, "[MQTT] Disconnected from broker")

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: object,  # pylint: disable=unused-argument
        msg: mqtt.MQTTMessage,
    ) -> None:
        """Route incoming messages to the appropriate handler."""
        # Extract method name from topic (flower/fleet/{method_name})
        parts = msg.topic.split("/")
        if len(parts) != 3:
            log(ERROR, "[MQTT] Unexpected topic: %s", msg.topic)
            return

        method_name = parts[2]
        handler = self._handlers.get(method_name)
        if handler is None:
            log(ERROR, "[MQTT] No handler for: %s", method_name)
            return

        # Extract MQTT 5.0 request-response properties
        response_topic = getattr(msg.properties, "ResponseTopic", None)
        correlation_data = getattr(msg.properties, "CorrelationData", None)

        if response_topic is None:
            log(ERROR, "[MQTT] No ResponseTopic in request on %s", msg.topic)
            return

        # Determine response QoS based on the method
        qos = METHOD_QOS.get(method_name, QOS_CONTROL)

        # Dispatch to handler
        try:
            response_bytes = handler(msg.payload)
            self._publish_response(
                response_topic, correlation_data, STATUS_OK, response_bytes, qos
            )
        except (
            InvalidRunStatusException,
            ValueError,
            message_handler.InvalidHeartbeatIntervalError,
            UnexpectedObjectContentError,
        ) as e:
            error_msg = str(e).encode("utf-8")
            self._publish_response(
                response_topic, correlation_data, STATUS_ERROR, error_msg, qos
            )
        except Exception as e:  # pylint: disable=broad-except
            log(ERROR, "[MQTT] Error handling %s: %s", method_name, e)
            error_msg = str(e).encode("utf-8")
            self._publish_response(
                response_topic, correlation_data, STATUS_ERROR, error_msg, qos
            )

    def _publish_response(
        self,
        response_topic: str,
        correlation_data: bytes | None,
        status: bytes,
        payload: bytes,
        qos: int = QOS_CONTROL,
    ) -> None:
        """Publish a response with status envelope to the client."""
        if self._client is None:
            return
        total = len(status) + len(payload)
        if total > MQTT_PAYLOAD_WARN_BYTES:
            log(
                WARN,
                "[MQTT] Response payload is %d bytes (%.1f MB). "
                "Ensure broker max_packet_size can accommodate this.",
                total,
                total / (1024 * 1024),
            )
        props = Properties(PacketTypes.PUBLISH)
        if correlation_data is not None:
            props.CorrelationData = correlation_data
        self._client.publish(
            response_topic,
            status + payload,
            qos=qos,
            properties=props,
        )

    # ---- RPC Handlers ----
    # Each handler deserializes the protobuf request, calls the shared
    # message_handler function, and returns the serialized response.

    def _handle_register_node(self, payload: bytes) -> bytes:
        request = RegisterNodeFleetRequest()
        request.ParseFromString(payload)
        try:
            response = message_handler.register_node(
                request=request, state=self.state_factory.state()
            )
        except ValueError:
            raise ValueError(PUBLIC_KEY_ALREADY_IN_USE_MESSAGE) from None
        log(DEBUG, "[MQTT.RegisterNode] Registered node_id=%s", response.node_id)
        return response.SerializeToString()

    def _handle_activate_node(self, payload: bytes) -> bytes:
        request = ActivateNodeRequest()
        request.ParseFromString(payload)
        response = message_handler.activate_node(
            request=request, state=self.state_factory.state()
        )
        log(INFO, "[MQTT.ActivateNode] Activated node_id=%s", response.node_id)
        return response.SerializeToString()

    def _handle_deactivate_node(self, payload: bytes) -> bytes:
        request = DeactivateNodeRequest()
        request.ParseFromString(payload)
        response = message_handler.deactivate_node(
            request=request, state=self.state_factory.state()
        )
        log(INFO, "[MQTT.DeactivateNode] Deactivated node_id=%s", request.node_id)
        return response.SerializeToString()

    def _handle_unregister_node(self, payload: bytes) -> bytes:
        request = UnregisterNodeFleetRequest()
        request.ParseFromString(payload)
        response = message_handler.unregister_node(
            request=request, state=self.state_factory.state()
        )
        log(DEBUG, "[MQTT.UnregisterNode] Unregistered node_id=%s", request.node_id)
        return response.SerializeToString()

    def _handle_heartbeat(self, payload: bytes) -> bytes:
        request = SendNodeHeartbeatRequest()
        request.ParseFromString(payload)
        response = message_handler.send_node_heartbeat(
            request=request, state=self.state_factory.state()
        )
        return response.SerializeToString()

    def _handle_pull_messages(self, payload: bytes) -> bytes:
        request = PullMessagesRequest()
        request.ParseFromString(payload)
        log(INFO, "[MQTT.PullMessages] node_id=%s", request.node.node_id)
        response = message_handler.pull_messages(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_push_messages(self, payload: bytes) -> bytes:
        request = PushMessagesRequest()
        request.ParseFromString(payload)
        if request.messages_list:
            log(
                INFO,
                "[MQTT.PushMessages] Push replies from node_id=%s",
                request.messages_list[0].metadata.src_node_id,
            )
        response = message_handler.push_messages(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_get_run(self, payload: bytes) -> bytes:
        request = GetRunRequest()
        request.ParseFromString(payload)
        log(INFO, "[MQTT.GetRun] run_id=%s", request.run_id)
        response = message_handler.get_run(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_get_fab(self, payload: bytes) -> bytes:
        request = GetFabRequest()
        request.ParseFromString(payload)
        log(INFO, "[MQTT.GetFab] fab_hash=%s", request.hash_str)
        response = message_handler.get_fab(
            request=request,
            ffs=self.ffs_factory.ffs(),
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_push_object(self, payload: bytes) -> bytes:
        request = PushObjectRequest()
        request.ParseFromString(payload)
        log(DEBUG, "[MQTT.PushObject] object_id=%s", request.object_id)
        response = message_handler.push_object(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_pull_object(self, payload: bytes) -> bytes:
        request = PullObjectRequest()
        request.ParseFromString(payload)
        log(DEBUG, "[MQTT.PullObject] object_id=%s", request.object_id)
        response = message_handler.pull_object(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()

    def _handle_confirm_message(self, payload: bytes) -> bytes:
        request = ConfirmMessageReceivedRequest()
        request.ParseFromString(payload)
        log(
            DEBUG,
            "[MQTT.ConfirmMessageReceived] message_id=%s",
            request.message_object_id,
        )
        response = message_handler.confirm_message_received(
            request=request,
            state=self.state_factory.state(),
            store=self.objectstore_factory.store(),
        )
        return response.SerializeToString()


def run_fleet_api_mqtt(
    host: str,
    port: int,
    state_factory: LinkStateFactory,
    ffs_factory: FfsFactory,
    objectstore_factory: ObjectStoreFactory,
    mqtt_tls: tuple[str, str | None, str | None] | None = None,
    shared_group: str | None = None,
) -> None:
    """Run the MQTT Fleet API server (blocking).

    This function is intended to be called in a daemon thread, similar to the
    REST Fleet API. It connects to the MQTT broker and blocks in the network
    loop, handling Fleet API requests from SuperNodes.
    """
    server = MqttFleetApiServer(
        state_factory=state_factory,
        ffs_factory=ffs_factory,
        objectstore_factory=objectstore_factory,
        broker_address=host,
        broker_port=port,
        mqtt_tls=mqtt_tls,
        shared_group=shared_group,
    )
    server.start()
