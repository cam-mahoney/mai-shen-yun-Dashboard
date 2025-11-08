import logging
from pathlib import Path
import pandas as pd
from typing import Union

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def resolve_path(relative_path: str) -> Path:
    return Path(__file__).resolve().parents[2] / relative_path

def read_file(file_path: Union[str, Path]) -> pd.DataFrame:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")
    if file_path.suffix == ".csv":
        return pd.read_csv(file_path)
    if file_path.suffix in [".xlsx", ".xls"]:
        return pd.read_excel(file_path)
    raise ValueError(f"Unsupported file type: {file_path.suffix}")
