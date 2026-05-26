import logging
from pathlib import Path

def get_logger(name: str = "gdelt_pipeline", log_to_file: bool = False):
    logger = logging.getLogger(name)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if log_to_file and not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        log_path = Path("logs")
        log_path.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_path / "pipeline.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger