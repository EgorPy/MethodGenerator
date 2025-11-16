""" Cross-platform launcher for backend, frontend and core.
    Linux  -> uses nohup to run processes in background (.out logs)
    Windows -> opens separate console windows, no log files
"""

from logger import logger
import subprocess
import platform
import sys
import os

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


def start_windows(script):
    """ Starts a process in a new console and shows output in real time """

    logger.info(f"Starting {script} in new console...")

    subprocess.Popen(
        [PYTHON, "-u", script],
        creationflags=subprocess.CREATE_NEW_CONSOLE
    )


def run_windows():
    """ Starts backend, frontend, core in separate consoles (Windows) """

    logger.info("Detected Windows. Starting services in new consoles...")

    start_windows("backend_main.py")
    start_windows("frontend_main.py")
    start_windows("core_main.py")

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
