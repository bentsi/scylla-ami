#!/usr/bin/env python3
import sys
import logging
import pathlib

LOG_DIR = pathlib.Path("/var/lib/scylla/logs")
AMI_LOG_PATH = LOG_DIR / "ami.log"


class ExitOnExceptionHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        # exit the application when logged message is above error logging level
        if record.levelno >= logging.ERROR:
            sys.exit(1)


def setup_logging(log_level=logging.INFO):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s - [%(module)s] - %(levelname)s - %(message)s")

    file_handler = logging.FileHandler(str(AMI_LOG_PATH))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(ExitOnExceptionHandler())
    root_logger.setLevel(log_level)
