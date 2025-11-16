""" Cross-platform launcher for backend, frontend and core.
    Linux  -> uses nohup to run processes in background (.out logs)
    Windows -> uses subprocess.Popen with log files
"""

from logger import logger
from pathlib import Path
import subprocess
import sys
import os
import platform

PYTHON = sys.executable


def run_linux():
    """ Starts all systems using nohup (Linux only) """

    logger.info("Detected Linux. Starting services via nohup...")

    scripts = [
        ("backend_main.py", "backend_main.out"),
        ("frontend_main.py", "frontend_main.out"),
        ("core_main.py", "core_main.out"),
    ]

    for script, outfile in scripts:
        cmd = f"nohup {PYTHON} -u {script} > {outfile} 2>&1 &"
        logger.info(f"Running: {cmd}")
        os.system(cmd)

    logger.info("All services started in background (nohup).")
    logger.info("Logs: *.out files in current directory.")


def ensure_log_dir():
    """ Logs """

    logs_path = Path("logs")
    logs_path.mkdir(exist_ok=True)
    return logs_path


def start_windows(script, logfile):
    """ Starts a process in background and redirects logs to a file """

    logs_dir = ensure_log_dir()
    log_path = logs_dir / logfile
    log_file = open(log_path, "a", encoding="utf-8")

    logger.info(f"Starting {script} -> logs/{logfile}")

    subprocess.Popen(
        [PYTHON, script],
        stdout=log_file,
        stderr=log_file,
        stdin=subprocess.DEVNULL,
        close_fds=True,
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )


def run_windows():
    """ Starts backend, frontend, core in separate consoles (Windows) """

    logger.info("Detected Windows. Starting services in new consoles...")

    start_windows("backend_main.py", "backend.log")
    start_windows("frontend_main.py", "frontend.log")
    start_windows("core_main.py", "core.log")

    logger.info("All Windows services launched.")


def run():
    """ Starts all systems """

    system = platform.system().lower()

    logger.info(f"=== Launcher started on {system.upper()} ===")

    if system == "linux":
        run_linux()
    elif system == "windows":
        run_windows()
    else:
        logger.error(f"Unsupported OS: {system}")
        raise SystemExit(1)


if __name__ == "__main__":
    run()
