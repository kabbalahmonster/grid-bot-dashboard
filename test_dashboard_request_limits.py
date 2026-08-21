import unittest

import dashboard_server


class TestDashboardRequestLimits(unittest.TestCase):
    def setUp(self):
        self.client = dashboard_server.app.test_client()

    def test_status_body_limit_returns_json_413(self):
        original_api_key = dashboard_server.API_KEY
        dashboard_server.API_KEY = "test-key"
        try:
            response = self.client.post(
                "/api/status",
                data=b"x" * (dashboard_server.MAX_STATUS_REQUEST_BYTES + 1),
                content_type="application/json",
                headers={"X-API-Key": "test-key"},
            )
        finally:
            dashboard_server.API_KEY = original_api_key
        self.assertEqual(response.status_code, 413)
        self.assertIn("byte limit", response.get_json()["error"])


if __name__ == "__main__":
    unittest.main()
