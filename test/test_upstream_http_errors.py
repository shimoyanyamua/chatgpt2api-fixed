from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from curl_cffi import requests
from utils.helper import UpstreamHTTPError, _UPSTREAM_ERROR_BODY_LIMIT, ensure_ok


class UpstreamHTTPErrorTests(unittest.TestCase):
    def test_real_streamed_error_preserves_json_and_retry_after(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"detail": "Unusual activity has been detected from your device."}).encode()
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Retry-After", "120")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            with requests.Session(trust_env=False) as session:
                response = session.get(f"http://127.0.0.1:{server.server_port}/", stream=True)
                # Accessing .text early also must not hide the streamed body.
                self.assertEqual(response.text, "")
                with self.assertRaises(UpstreamHTTPError) as error:
                    ensure_ok(response, "conversation")
                self.assertEqual(error.exception.body["detail"], "Unusual activity has been detected from your device.")
                self.assertEqual(error.exception.status_code, 403)
                self.assertEqual(error.exception.retry_after, 120)
                self.assertIn("Unusual activity", str(error.exception))
                self.assertTrue(response._stream_closed)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_large_error_is_bounded_and_stream_is_closed(self):
        response = mock.Mock(status_code=502, headers={}, queue=object())
        response.iter_content.return_value = iter([b"x" * (_UPSTREAM_ERROR_BODY_LIMIT + 10)])
        with self.assertRaises(UpstreamHTTPError) as error:
            ensure_ok(response, "upstream")
        self.assertEqual(len(error.exception.body), _UPSTREAM_ERROR_BODY_LIMIT)
        response.close.assert_called_once()

    def test_read_failure_preserves_http_status_and_closes_stream(self):
        response = mock.Mock(status_code=429, headers={"Retry-After": "10"}, queue=object())
        response.iter_content.side_effect = RuntimeError("connection reset")
        with self.assertRaises(UpstreamHTTPError) as error:
            ensure_ok(response, "upstream")
        self.assertEqual(error.exception.status_code, 429)
        self.assertEqual(error.exception.retry_after, 10)
        self.assertIn("Unable to read", error.exception.body)
        response.close.assert_called_once()

    def test_success_stream_is_not_consumed_or_closed(self):
        response = mock.Mock(status_code=200, queue=object())
        ensure_ok(response, "upstream")
        response.iter_content.assert_not_called()
        response.close.assert_not_called()

    def test_nonstream_error_keeps_structured_body(self):
        response = mock.Mock(status_code=404, queue=None, headers={})
        response.json.return_value = {"detail": "Model not found"}
        with self.assertRaises(UpstreamHTTPError) as error:
            ensure_ok(response, "upstream")
        self.assertEqual(error.exception.body, {"detail": "Model not found"})


if __name__ == "__main__":
    unittest.main()
