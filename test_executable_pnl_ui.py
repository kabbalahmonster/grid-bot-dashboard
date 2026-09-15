import unittest

import dashboard_test_env  # noqa: F401
from dashboard_server import app


class TestExecutablePnlUi(unittest.TestCase):
    def test_dashboard_labels_spot_and_estimated_exit_pnl(self):
        response = app.test_client().get("/")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("AVG spot P&amp;L", html)
        self.assertIn("Spot &#39;", html.replace("'", "&#39;"))
        self.assertIn("Est. exit:", html)
        self.assertIn("sampled reverse quote", html)
        self.assertIn("quoteAgeSeconds > 180", html)


if __name__ == "__main__":
    unittest.main()
