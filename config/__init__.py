"""
Banking Data Engineering Platform — Configuration Loader
=========================================================
Loads config.yaml, merges with environment variables, and
exposes a typed Config object used throughout the project.

Why a central config module?
- Single source of truth for all settings
- Environment variables override YAML for Docker/CI/CD
- No hardcoded credentials anywhere
- Easy to swap dev → production settings
"""

import os
import yaml
import logging
import logging.config
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv

# Load .env file at import time (harmless if file doesn't exist)
load_dotenv()

# Project root = parent of this file's parent (config/)
PROJECT_ROOT = Path(__file__).parent.parent


def _resolve_env_vars(value: Any) -> Any:
    """
    Recursively resolve ${ENV_VAR} placeholders in YAML values.
    
    Example:
        host: "${DATABASE_HOST}"  →  host: "localhost"
    """
    if isinstance(value, str):
        if value.startswith("${") and value.endswith("}"):
            env_key = value[2:-1]
            resolved = os.getenv(env_key, "")
            return resolved
        return value
    elif isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(config_path: Optional[Path] = None) -> dict:
    """
    Load the main config.yaml file and resolve environment variables.
    
    Args:
        config_path: Path to config.yaml. Defaults to config/config.yaml.
    
    Returns:
        Dictionary with all configuration values.
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    return _resolve_env_vars(raw_config)


def setup_logging(logging_config_path: Optional[Path] = None) -> None:
    """
    Configure logging from logging.yaml.
    Creates logs/ directory if it doesn't exist.
    
    Args:
        logging_config_path: Path to logging.yaml.
    """
    if logging_config_path is None:
        logging_config_path = PROJECT_ROOT / "config" / "logging.yaml"

    # Ensure logs directory exists
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(exist_ok=True)

    if logging_config_path.exists():
        with open(logging_config_path, "r", encoding="utf-8") as f:
            log_config = yaml.safe_load(f)
        logging.config.dictConfig(log_config)
    else:
        # Fallback to basic config if logging.yaml is missing
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        )


def get_db_url(config: dict) -> str:
    """
    Build a SQLAlchemy-compatible PostgreSQL connection URL.
    
    Args:
        config: Loaded configuration dictionary.
    
    Returns:
        PostgreSQL connection string.
    
    Example:
        postgresql://banking_user:password@localhost:5432/banking_dw
    """
    db = config["database"]
    return (
        f"postgresql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )


def get_data_path(config: dict, layer: str, dataset: str = "") -> Path:
    """
    Get the local filesystem path for a data lake layer.
    
    Args:
        config: Loaded configuration dictionary.
        layer: One of 'raw', 'processed', 'curated', 'rejected', 'sample'
        dataset: Optional sub-directory name (e.g., 'transactions')
    
    Returns:
        Path object for the requested location.
    
    Why:
        Abstracts the data lake structure so code doesn't use hardcoded paths.
        In production, this would route to S3 instead.
    """
    storage_mode = config["storage"]["mode"]
    
    if storage_mode == "local":
        base = Path(config["storage"]["local_root"])
    else:
        # For S3, return a string prefix (handled by storage abstraction)
        raise NotImplementedError("S3 mode implemented in storage/s3_client.py")

    layer_path_key = config["storage"]["paths"].get(layer, layer)
    full_path = base / layer_path_key
    
    if dataset:
        full_path = full_path / dataset

    full_path.mkdir(parents=True, exist_ok=True)
    return full_path


def get_generation_params(config: dict) -> dict:
    """
    Get data generation parameters based on the current mode.
    
    Args:
        config: Loaded configuration dictionary.
    
    Returns:
        Dictionary with record counts and date range.
    """
    mode = os.getenv("DATA_GENERATION_MODE", config["data_generation"]["mode"])
    params = config["data_generation"][mode].copy()
    params["bad_data_percentage"] = config["data_generation"]["bad_data_percentage"]
    params["batch_size"] = config["data_generation"]["batch_size"]
    params["mode"] = mode
    return params
