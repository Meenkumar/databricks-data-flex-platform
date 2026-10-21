"""
Configuration loader for reading YAML/JSON configuration files.
"""
import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional
from common.exceptions import ConfigurationException
from common.logging import get_logger


logger = get_logger(__name__)


class ConfigLoader:
    """Loads and manages configuration files."""

    def __init__(self, config_base_path: str = "/Workspace/config"):
        """
        Initialize config loader.

        Args:
            config_base_path: Base path for configuration files
        """
        self.config_base_path = Path(config_base_path)
        self._cache = {}

    def load_config(self, config_name: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Load configuration file by name.

        Args:
            config_name: Name of config file (without extension)
            use_cache: Whether to use cached config

        Returns:
            Configuration dictionary

        Raises:
            ConfigurationException: If config file not found or invalid
        """
        if use_cache and config_name in self._cache:
            logger.debug(f"Loading cached config: {config_name}")
            return self._cache[config_name]

        # Try YAML first, then JSON
        for ext in ['.yml', '.yaml', '.json']:
            config_path = self.config_base_path / f"{config_name}{ext}"

            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        if ext == '.json':
                            config = json.load(f)
                        else:
                            config = yaml.safe_load(f)

                    logger.info(f"Loaded config: {config_path}")
                    self._cache[config_name] = config
                    return config

                except Exception as e:
                    raise ConfigurationException(
                        f"Failed to parse config file: {config_path}",
                        error_code="CONFIG_PARSE_ERROR"
                    ) from e

        raise ConfigurationException(
            f"Configuration file not found: {config_name}",
            error_code="CONFIG_NOT_FOUND"
        )

    def load_schema(self, schema_name: str) -> Dict[str, Any]:
        """Load schema configuration from config/schemas directory."""
        schema_path = self.config_base_path / "schemas" / f"{schema_name}.yml"

        try:
            with open(schema_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationException(
                f"Schema file not found: {schema_name}",
                error_code="SCHEMA_NOT_FOUND"
            )

    def load_mapping(self, mapping_name: str) -> Dict[str, Any]:
        """Load field mapping configuration."""
        mapping_path = self.config_base_path / "mappings" / f"{mapping_name}.yml"

        try:
            with open(mapping_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationException(
                f"Mapping file not found: {mapping_name}",
                error_code="MAPPING_NOT_FOUND"
            )

    def get_client_config(self, client_name: str) -> Dict[str, Any]:
        """Load client-specific configuration."""
        client_path = self.config_base_path / "clients" / f"{client_name}.yml"

        try:
            with open(client_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationException(
                f"Client config not found: {client_name}",
                error_code="CLIENT_CONFIG_NOT_FOUND"
            )
