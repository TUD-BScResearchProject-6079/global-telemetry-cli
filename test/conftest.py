import os
import sys
from typing import Any, Iterator
from unittest.mock import MagicMock, patch

import pytest

file_handler_patch = patch("logging.FileHandler", MagicMock())
file_handler_patch.start()


@pytest.fixture(autouse=True)
def patch_logger() -> Iterator[MagicMock]:
    with patch("src.config.logger", MagicMock()) as mock_logger:
        yield mock_logger


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
    _ = session
    _ = exitstatus
    file_handler_patch.stop()
