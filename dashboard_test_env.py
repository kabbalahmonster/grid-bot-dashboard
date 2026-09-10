"""Isolate persistence before tests import the dashboard application."""

import atexit
import os
import shutil
import tempfile


_state_dir = tempfile.mkdtemp(prefix="doomdash-tests-")
atexit.register(shutil.rmtree, _state_dir, ignore_errors=True)

os.environ["STATE_FILE"] = os.path.join(_state_dir, "dashboard_state.json")
os.environ["TELEGRAM_ALERT_STATE_FILE"] = os.path.join(_state_dir, "telegram_alert_state.json")
os.environ["DOOM_SCOUT_STATE_FILE"] = os.path.join(_state_dir, "doom_scout.json")
