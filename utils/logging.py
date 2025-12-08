import logging
from pathlib import Path

def get_logger(name: str = "gdelt_pipeline", log_to_file: bool = False):
    logger = logging.getLogger(name)

    if not logger.handlers:  # Prevent duplicate handlers during imports
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        # ---- console handler ----
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # ---- optional file handler ----
        if log_to_file:
            log_path = Path("logs")
            log_path.mkdir(exist_ok=True)
            file_handler = logging.FileHandler(log_path / "pipeline.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger