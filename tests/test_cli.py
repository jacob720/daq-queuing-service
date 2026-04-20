import subprocess
import sys

from daq_queuing_service import __version__


def test_cli_version():
    cmd = [sys.executable, "-m", "daq_queuing_service", "--version"]
    assert subprocess.check_output(cmd).decode().strip() == __version__
