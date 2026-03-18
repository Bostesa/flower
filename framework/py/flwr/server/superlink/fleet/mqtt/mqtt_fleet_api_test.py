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
"""Tests for MQTT Fleet API server."""


import unittest
from unittest.mock import MagicMock, patch

from flwr.proto.fleet_pb2 import (  # pylint: disable=E0611
    ActivateNodeResponse,
    RegisterNodeFleetRequest,
    RegisterNodeFleetResponse,
)
from flwr.proto.heartbeat_pb2 import (  # pylint: disable=E0611
    SendNodeHeartbeatResponse,
)

from .mqtt_fleet_api import (
    METHOD_QOS,
    QOS_CONTROL,
    QOS_MODEL_DATA,
    STATUS_ERROR,
    STATUS_OK,
    MqttFleetApiServer,
)


class TestQoSMapping(unittest.TestCase):
    """Verify QoS levels match the CLAUDE.md specification."""

    def test_model_data_uses_qos2(self) -> None:
        """Model weight methods must use QoS 2 (exactly-once)."""
        for method in ("pull_messages", "push_messages", "push_object", "pull_object"):
            self.assertEqual(
                METHOD_QOS[method],
                QOS_MODEL_DATA,
                f"{method} should use QoS {QOS_MODEL_DATA}",
            )

    def test_control_uses_qos1(self) -> None:
        """Control methods must use QoS 1 (at-least-once)."""
        for method in (
            "heartbeat",
            "register_node",
            "activate_node",
            "deactivate_node",
            "unregister_node",
            "get_run",
            "get_fab",
            "confirm_message",
        ):
            self.assertEqual(
                METHOD_QOS[method],
                QOS_CONTROL,
                f"{method} should use QoS {QOS_CONTROL}",
            )

    def test_all_handlers_have_qos(self) -> None:
        """Every handler method must have a QoS mapping."""
        server = _make_server()
        for method in server._handlers:
            self.assertIn(method, METHOD_QOS, f"No QoS mapping for {method}")


class TestHandlers(unittest.TestCase):
    """Verify RPC handlers serialize/deserialize correctly."""

    def test_register_node_roundtrip(self) -> None:
        """RegisterNode handler returns a valid protobuf response."""
        server = _make_server()
        server.state_factory.state().create_node.return_value = 42

        req = RegisterNodeFleetRequest(public_key=b"test-key")
        result = server._handle_register_node(req.SerializeToString())

        res = RegisterNodeFleetResponse()
        res.ParseFromString(result)
        self.assertEqual(res.node_id, 42)

    def test_activate_node_roundtrip(self) -> None:
        """ActivateNode handler returns the assigned node_id."""
        server = _make_server()
        server.state_factory.state().get_node_id_by_public_key.return_value = 99
        server.state_factory.state().activate_node.return_value = True

        from flwr.proto.fleet_pb2 import ActivateNodeRequest  # pylint: disable=E0611

        req = ActivateNodeRequest(public_key=b"key", heartbeat_interval=30)
        result = server._handle_activate_node(req.SerializeToString())

        res = ActivateNodeResponse()
        res.ParseFromString(result)
        self.assertEqual(res.node_id, 99)

    def test_heartbeat_roundtrip(self) -> None:
        """Heartbeat handler returns success=True."""
        server = _make_server()
        server.state_factory.state().acknowledge_node_heartbeat.return_value = True

        from flwr.proto.heartbeat_pb2 import (  # pylint: disable=E0611
            SendNodeHeartbeatRequest,
        )
        from flwr.proto.node_pb2 import Node  # pylint: disable=E0611

        req = SendNodeHeartbeatRequest(
            node=Node(node_id=1), heartbeat_interval=30
        )
        result = server._handle_heartbeat(req.SerializeToString())

        res = SendNodeHeartbeatResponse()
        res.ParseFromString(result)
        self.assertTrue(res.success)


class TestStatusEnvelope(unittest.TestCase):
    """Verify the status byte envelope format."""

    def test_status_ok_prefix(self) -> None:
        self.assertEqual(STATUS_OK, b"\x00")

    def test_status_error_prefix(self) -> None:
        self.assertEqual(STATUS_ERROR, b"\x01")

    @patch("paho.mqtt.client.Client")
    def test_publish_response_prepends_status(self, mock_client_cls: MagicMock) -> None:
        """_publish_response prepends the status byte to the payload."""
        server = _make_server()
        mock_client = MagicMock()
        server._client = mock_client

        server._publish_response("resp/topic", b"corr", STATUS_OK, b"payload")

        call_args = mock_client.publish.call_args
        published_payload = call_args[0][1]
        self.assertTrue(published_payload.startswith(STATUS_OK))
        self.assertEqual(published_payload, STATUS_OK + b"payload")


class TestBrokerUnreachable(unittest.TestCase):
    """Verify clean error when broker is unavailable."""

    @patch("paho.mqtt.client.Client")
    def test_connection_refused_logs_clear_error(
        self, mock_client_cls: MagicMock
    ) -> None:
        """start() should log a clear error, not raise a raw exception."""
        mock_instance = mock_client_cls.return_value
        mock_instance.connect.side_effect = ConnectionRefusedError("refused")

        server = _make_server()

        # start() should catch the error and return, not raise
        with self.assertLogs("flwr", level="ERROR") as cm:
            server.start()

        error_msgs = [m for m in cm.output if "Mosquitto" in m]
        self.assertTrue(
            len(error_msgs) > 0,
            "Expected a log message mentioning Mosquitto",
        )


class TestSharedSubscriptions(unittest.TestCase):
    """Verify shared subscription topic prefix."""

    @patch("paho.mqtt.client.Client")
    def test_shared_group_prepends_prefix(self, mock_client_cls: MagicMock) -> None:
        """When shared_group is set, subscriptions use $share/{group}/."""
        server = _make_server(shared_group="superlinks")
        mock_client = mock_client_cls.return_value

        # Simulate on_connect
        mock_rc = MagicMock()
        mock_rc.is_failure = False
        server._client = mock_client
        server._on_connect(mock_client, None, MagicMock(), mock_rc)

        # Check that subscribe was called with shared prefix
        subscribe_calls = mock_client.subscribe.call_args_list
        topics = [call[0][0] for call in subscribe_calls]
        for topic in topics:
            self.assertTrue(
                topic.startswith("$share/superlinks/"),
                f"Expected $share/superlinks/ prefix, got: {topic}",
            )

    @patch("paho.mqtt.client.Client")
    def test_no_shared_group_uses_plain_topics(
        self, mock_client_cls: MagicMock
    ) -> None:
        """When shared_group is None, subscriptions use plain topics."""
        server = _make_server(shared_group=None)
        mock_client = mock_client_cls.return_value

        mock_rc = MagicMock()
        mock_rc.is_failure = False
        server._client = mock_client
        server._on_connect(mock_client, None, MagicMock(), mock_rc)

        subscribe_calls = mock_client.subscribe.call_args_list
        topics = [call[0][0] for call in subscribe_calls]
        for topic in topics:
            self.assertFalse(
                topic.startswith("$share/"),
                f"Unexpected shared prefix in: {topic}",
            )


class TestTLSConfig(unittest.TestCase):
    """Verify TLS configuration is applied."""

    @patch("paho.mqtt.client.Client")
    def test_tls_set_called_when_config_provided(
        self, mock_client_cls: MagicMock
    ) -> None:
        """tls_set() is called before connect() when mqtt_tls is set."""
        mock_instance = mock_client_cls.return_value
        # Prevent loop_forever from blocking
        mock_instance.connect.side_effect = ConnectionRefusedError("test")

        server = _make_server(
            mqtt_tls=("/path/ca.crt", "/path/client.crt", "/path/client.key")
        )

        with self.assertLogs("flwr"):
            server.start()

        mock_instance.tls_set.assert_called_once_with(
            ca_certs="/path/ca.crt",
            certfile="/path/client.crt",
            keyfile="/path/client.key",
        )

    @patch("paho.mqtt.client.Client")
    def test_tls_not_called_when_no_config(
        self, mock_client_cls: MagicMock
    ) -> None:
        """tls_set() is NOT called when mqtt_tls is None."""
        mock_instance = mock_client_cls.return_value
        mock_instance.connect.side_effect = ConnectionRefusedError("test")

        server = _make_server(mqtt_tls=None)

        with self.assertLogs("flwr"):
            server.start()

        mock_instance.tls_set.assert_not_called()


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_server(
    mqtt_tls: tuple[str, str | None, str | None] | None = None,
    shared_group: str | None = None,
) -> MqttFleetApiServer:
    """Create a MqttFleetApiServer with mocked factories."""
    state_factory = MagicMock()
    ffs_factory = MagicMock()
    objectstore_factory = MagicMock()
    return MqttFleetApiServer(
        state_factory=state_factory,
        ffs_factory=ffs_factory,
        objectstore_factory=objectstore_factory,
        broker_address="localhost",
        broker_port=1883,
        mqtt_tls=mqtt_tls,
        shared_group=shared_group,
    )


if __name__ == "__main__":
    unittest.main()
