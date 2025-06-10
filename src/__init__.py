from pathlib import Path

from src.logger import LogUtils

logger = LogUtils.init_logger()
data_dir = (Path(__file__).parent / '..' / "data").resolve()
