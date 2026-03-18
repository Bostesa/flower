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
"""Tests for MQTT client connection."""


import threading
import unittest
from unittest.mock import MagicMock, patch

from .connection import (
    METHOD_QOS,
    QOS_CONTROL,
    QOS_MODEL_DATA,
    STATUS_ERROR,
    STATUS_OK,
    MqttRpcError,
    _MqttConnection,
    _parse_mqtt_address,
)


class TestParseAddress(unittest.TestCase):
    """Verify MQTT address parsing."""

    def test_host_port(self) -> None:
        self.assertEqual(_parse_mqtt_address("localhost:1883"), ("localhost", 1883))

    def test_ip_port(self) -> None:
        self.assertEqual(_parse_mqtt_address("192.168.1.1:8883"), ("192.168.1.1", 8883))

    def test_ipv6(self) -> None:
        self.assertEqual(_parse_mqtt_address("[::1]:1883"), ("::1", 1883))

    def test_ipv6_default_port(self) -> None:
        self.assertEqual(_parse_mqtt_address("[::1]"), ("::1", 1883))

    def test_host_only_defaults_to_1883(self) -> None:
        self.assertEqual(_parse_mqtt_address("broker.local"), ("broker.local", 1883))


class TestQoSMapping(unittest.TestCase):
    """Client-side QoS must mirror server-side."""

    def test_model_data_qos2(self) -> None:
        for method in ("pull_messages", "push_messages", "push_object", "pull_object"):
            self.assertEqual(METHOD_QOS[method], QOS_MODEL_DATA)

    def test_control_qos1(self) -> None:
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
            self.assertEqual(METHOD_QOS[method], QOS_CONTROL)


class TestCallTimeout(unittest.TestCase):
    """Verify call() raises MqttRpcError on timeout."""

    def test_timeout_raises_error(self) -> None:
        """call() must raise MqttRpcError when no response arrives."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        with self.assertRaises(MqttRpcError) as ctx:
            conn.call("test_method", b"payload", timeout=0.1)

        self.assertIn("Timeout", str(ctx.exception))

    def test_timeout_cleans_up_pending(self) -> None:
        """After timeout, the correlation ID is removed from pending."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        try:
            conn.call("test", b"data", timeout=0.1)
        except MqttRpcError:
            pass

        self.assertEqual(len(conn._pending), 0)


class TestCallErrorResponse(unittest.TestCase):
    """Verify call() raises on STATUS_ERROR responses."""

    def test_error_status_raises(self) -> None:
        """A STATUS_ERROR response must raise MqttRpcError with the message."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        error_msg = b"Run not found"

        def fake_publish(topic, payload, qos, properties):
            # Simulate server response arriving immediately
            corr_data = properties.CorrelationData
            conn._responses[corr_data] = STATUS_ERROR + error_msg
            event = conn._pending.get(corr_data)
            if event:
                event.set()

        conn._client.publish.side_effect = fake_publish

        with self.assertRaises(MqttRpcError) as ctx:
            conn.call("get_run", b"request", timeout=1.0)

        self.assertIn("Run not found", str(ctx.exception))

    def test_ok_status_returns_payload(self) -> None:
        """A STATUS_OK response returns the body bytes."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        expected = b"response-payload"

        def fake_publish(topic, payload, qos, properties):
            corr_data = properties.CorrelationData
            conn._responses[corr_data] = STATUS_OK + expected
            event = conn._pending.get(corr_data)
            if event:
                event.set()

        conn._client.publish.side_effect = fake_publish

        result = conn.call("pull_messages", b"request", timeout=1.0)
        self.assertEqual(result, expected)


class TestBrokerUnreachable(unittest.TestCase):
    """Verify clean error when broker is unavailable."""

    @patch("paho.mqtt.client.Client")
    def test_connection_refused_gives_clear_message(
        self, mock_client_cls: MagicMock
    ) -> None:
        """MqttRpcError message must mention Mosquitto and the port."""
        mock_instance = mock_client_cls.return_value
        mock_instance.connect.side_effect = ConnectionRefusedError("refused")

        conn = _MqttConnection("localhost", 1883)

        with self.assertRaises(MqttRpcError) as ctx:
            conn.connect()

        msg = str(ctx.exception)
        self.assertIn("Mosquitto", msg)
        self.assertIn("1883", msg)

    def test_not_connected_raises_on_call(self) -> None:
        """call() raises if connect() was never called."""
        conn = _MqttConnection("localhost", 1883)

        with self.assertRaises(MqttRpcError) as ctx:
            conn.call("test", b"data")

        self.assertIn("Not connected", str(ctx.exception))


class TestTLSConfig(unittest.TestCase):
    """Verify TLS configuration is applied to the client."""

    @patch("paho.mqtt.client.Client")
    def test_tls_set_called(self, mock_client_cls: MagicMock) -> None:
        """tls_set() is called before connect() when mqtt_tls is set."""
        mock_instance = mock_client_cls.return_value
        # Prevent actual connection
        mock_instance.connect.side_effect = ConnectionRefusedError("test")

        conn = _MqttConnection(
            "localhost", 8883, mqtt_tls=("/ca.crt", "/client.crt", "/client.key")
        )

        with self.assertRaises(MqttRpcError):
            conn.connect()

        mock_instance.tls_set.assert_called_once_with(
            ca_certs="/ca.crt", certfile="/client.crt", keyfile="/client.key"
        )

    @patch("paho.mqtt.client.Client")
    def test_tls_not_called_without_config(self, mock_client_cls: MagicMock) -> None:
        """tls_set() is NOT called when mqtt_tls is None."""
        mock_instance = mock_client_cls.return_value
        mock_instance.connect.side_effect = ConnectionRefusedError("test")

        conn = _MqttConnection("localhost", 1883)

        with self.assertRaises(MqttRpcError):
            conn.connect()

        mock_instance.tls_set.assert_not_called()

    @patch("paho.mqtt.client.Client")
    def test_tls_ca_only(self, mock_client_cls: MagicMock) -> None:
        """TLS works with only a CA cert (no client cert)."""
        mock_instance = mock_client_cls.return_value
        mock_instance.connect.side_effect = ConnectionRefusedError("test")

        conn = _MqttConnection("localhost", 8883, mqtt_tls=("/ca.crt", None, None))

        with self.assertRaises(MqttRpcError):
            conn.connect()

        mock_instance.tls_set.assert_called_once_with(
            ca_certs="/ca.crt", certfile=None, keyfile=None
        )


class TestCallQoSSelection(unittest.TestCase):
    """Verify call() uses per-method QoS."""

    def test_pull_messages_uses_qos2(self) -> None:
        """pull_messages must publish at QoS 2."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        # Simulate immediate response
        def fake_publish(topic, payload, qos, properties):
            corr_data = properties.CorrelationData
            conn._responses[corr_data] = STATUS_OK + b"resp"
            conn._pending[corr_data].set()

        conn._client.publish.side_effect = fake_publish
        conn.call("pull_messages", b"req", timeout=1.0)

        qos_used = conn._client.publish.call_args[1].get(
            "qos", conn._client.publish.call_args[0][2] if len(conn._client.publish.call_args[0]) > 2 else None
        )
        self.assertEqual(qos_used, QOS_MODEL_DATA)

    def test_heartbeat_uses_qos1(self) -> None:
        """heartbeat must publish at QoS 1."""
        conn = _MqttConnection("localhost", 1883)
        conn._client = MagicMock()
        conn._connected.set()

        def fake_publish(topic, payload, qos, properties):
            corr_data = properties.CorrelationData
            conn._responses[corr_data] = STATUS_OK + b"resp"
            conn._pending[corr_data].set()

        conn._client.publish.side_effect = fake_publish
        conn.call("heartbeat", b"req", timeout=1.0)

        qos_used = conn._client.publish.call_args[1].get(
            "qos", conn._client.publish.call_args[0][2] if len(conn._client.publish.call_args[0]) > 2 else None
        )
        self.assertEqual(qos_used, QOS_CONTROL)


if __name__ == "__main__":
    unittest.main()
