import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cpu_drill


class PreflightTest(unittest.TestCase):
    def test_dead_docker_stops_before_creating_output_or_cluster(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "run"
            stderr = io.StringIO()
            with (patch.object(sys, "argv", ["cpu_drill.py", "--out", str(out)]),
                  patch.object(cpu_drill, "MIN_TEMP_FREE", 0),
                  patch.object(cpu_drill, "command", side_effect=subprocess.TimeoutExpired("docker info", 15)) as cli,
                  contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as stopped):
                cpu_drill.main()
            self.assertEqual(2, stopped.exception.code)
            self.assertIn("Docker is not responding", stderr.getvalue())
            cli.assert_called_once_with("docker", "info", "--format", "{{.OSType}}", timeout=15)
            self.assertFalse(out.exists())
