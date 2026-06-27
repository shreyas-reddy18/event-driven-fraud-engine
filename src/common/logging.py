from __future__ import annotations

import json
import logging
import sys
from typing import Any


class StructuredLogger(logging.Logger):
    def _log_structured(self, level: int, msg: str, **kwargs: Any) -> None:
        payload = {"message": msg, **kwargs}
        self.log(level, json.dumps(payload))

    def info(self, msg: str, **kwargs: Any) -> None:  # type: ignore[override]
        self._log_structured(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:  # type: ignore[override]
        self._log_structured(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:  # type: ignore[override]
        self._log_structured(logging.ERROR, msg, **kwargs)


def get_logger(name: str) -> StructuredLogger:
    logging.setLoggerClass(StructuredLogger)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger  # type: ignore[return-value]
