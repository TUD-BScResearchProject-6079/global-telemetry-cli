from __future__ import annotations

from datetime import datetime, timezone
import functools
import logging
import os
import time
from typing import Any, Callable


class LogUtils:
    _logger = None

    @staticmethod
    def init_logger(log_dir: str = 'logs') -> logging.Logger:
        if LogUtils._logger:
            return LogUtils._logger

        current_dir = os.path.dirname(os.path.abspath(__file__))
        logs_dir_path = os.path.join(os.path.abspath(os.path.join(current_dir, '..')), log_dir)
        os.makedirs(logs_dir_path, exist_ok=True)
        utc_now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')
        log_path = os.path.join(log_dir, f'{utc_now}.log')

        logging.Formatter.converter = time.gmtime

        LogUtils._logger = logging.getLogger("daily_logger")
        LogUtils._logger.setLevel(logging.INFO)

        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        handler.setFormatter(formatter)

        LogUtils._logger.addHandler(handler)
        LogUtils._logger.addHandler(logging.StreamHandler())

        return LogUtils._logger

    @staticmethod
    def log_function(func: Callable[..., Any]) -> Callable[..., Any]:
        logger = LogUtils.init_logger()

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger.info(f'Starting: {func.__name__}')
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f'Finished: {func.__name__} (Duration: {duration:.2f}s)')
                return result
            except Exception as e:
                logger.exception(f'Exception in {func.__name__}: {e}')
                raise

        return wrapper
