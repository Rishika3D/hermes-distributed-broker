"""Tests for HermesClient retry / failover behaviour.

Uses a scripted stub HTTP server on localhost (no real Hermes cluster, no external network).
``time.sleep`` is patched so backoff delays are recorded, not waited for.

Run:  python3 -m unittest discover -s clients/python -v
"""

import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import hermes_client
from hermes_client import HermesClient, HermesError


class ScriptedServer:
    """Replies to each request with the next scripted (status, body); repeats the last one."""

    def __init__(self, script):
        self.script = list(script)
        self.requests = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 (stdlib naming)
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                index = min(outer.requests, len(outer.script) - 1)
                outer.requests += 1
                status, body = outer.script[index]
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):  # keep test output quiet
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        self.thread = threading.Thread(target=lambda: self.httpd.serve_forever(poll_interval=0.01), daemon=True)
        self.thread.start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def closed_port_url():
    """A URL nothing is listening on (connection refused)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{s.getsockname()[1]}"


class RetryBehaviour(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(hermes_client.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)
        self.servers = []
        self.addCleanup(lambda: [s.close() for s in self.servers])

    def server(self, script):
        s = ScriptedServer(script)
        self.servers.append(s)
        return s

    def test_transient_503_is_retried_with_exponential_backoff_until_success(self):
        srv = self.server([(503, {"error": "electing"}), (503, {"error": "electing"}),
                           (200, {"partition": 1, "offset": 7})])
        client = HermesClient(brokers=[srv.url])

        result = client.produce("orders", "hello")

        self.assertEqual(result, {"partition": 1, "offset": 7})
        self.assertEqual(srv.requests, 3)
        self.assertEqual([c.args[0] for c in self.sleep.call_args_list], [0.2, 0.4])

    def test_client_errors_are_never_retried(self):
        srv = self.server([(400, {"error": "bad topic name"})])
        client = HermesClient(brokers=[srv.url])

        with self.assertRaises(HermesError) as ctx:
            client.produce("bad,name", "v")

        self.assertIn("400", str(ctx.exception))
        self.assertEqual(srv.requests, 1)
        self.sleep.assert_not_called()

    def test_fails_over_to_next_broker_immediately_without_sleeping(self):
        good = self.server([(200, {"partition": 0, "offset": 1})])
        client = HermesClient(brokers=[closed_port_url(), good.url])

        self.assertEqual(client.produce("orders", "v")["offset"], 1)
        self.sleep.assert_not_called()

    def test_gives_up_after_bounded_retries_and_raises(self):
        srv = self.server([(503, {"error": "down"})])
        client = HermesClient(brokers=[srv.url])

        with self.assertRaises(HermesError):
            client.produce("orders", "v")

        self.assertEqual(srv.requests, 4, "initial attempt + 3 default retries")
        self.assertEqual(self.sleep.call_count, 3)

    def test_backoff_delay_is_capped(self):
        srv = self.server([(503, {"error": "down"})])
        client = HermesClient(brokers=[srv.url], retries=10)

        with self.assertRaises(HermesError):
            client.produce("orders", "v")

        delays = [c.args[0] for c in self.sleep.call_args_list]
        self.assertEqual(len(delays), 10)
        self.assertLessEqual(max(delays), 2.0)


if __name__ == "__main__":
    unittest.main()
