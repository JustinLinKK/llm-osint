from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

_LOGGER_INITIALIZED = False


def get_logger(name: Optional[str] = None) -> logging.Logger:
    global _LOGGER_INITIALIZED
    if not _LOGGER_INITIALIZED:
        _LOGGER_INITIALIZED = True
        log_dir = Path(__file__).resolve().parents[1] / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "agent-langgraph.log"

        handlers = [
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
            handlers=handlers,
        )

    return logging.getLogger(name or "agent-langgraph")
