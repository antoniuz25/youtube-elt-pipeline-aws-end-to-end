import logging
from pathlib import Path

logger_name="pipeline"

def get_logger(name: str=logger_name) -> logging.Logger:
    Path("logs").mkdir(exist_ok=True)

    logger=logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger
    
    fmt=logging.Formatter("%(asctime)s |%(levelname)s | %(name)s | %(message)s")

    fh=logging.FileHandler(f"logs/{logger_name}.log",encoding="utf-8")

    sh=logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger