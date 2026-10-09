"""
Centralized Configuration Module using Pydantic Settings.

Parses environment variables and falls back to default values for local development.
"""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application Settings configuration.

    Reads environment variables or defaults to local development values.
    """
    # Environment & General Configurations
    ENV: str = "dev"
    DEBUG: bool = True
    PROJECT_NAME: str = "fin-risk-mlops"

    # Databricks Cloud Workspace Configuration
    DATABRICKS_HOST: Optional[str] = "https://localhost-mock.cloud.databricks.com"
    DATABRICKS_TOKEN: Optional[str] = "mock_local_token"
    DATABRICKS_SQL_HTTP_PATH: Optional[str] = "/sql/1.0/endpoints/mock_path"

    # Storage & Data Paths (Local Fallback Paths)
    BRONZE_DELTA_PATH: str = "data/delta/bronze"
    SILVER_DELTA_PATH: str = "data/delta/silver"
    GOLD_DELTA_PATH: str = "data/delta/gold"

    # MLflow Configurations
    MLFLOW_TRACKING_URI: str = "http://localhost:5000"
    MLFLOW_EXPERIMENT_NAME: str = "/Shared/fin-risk-mlops-experiment"

    # Pydantic Settings Configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Instantiate centralized settings singleton
settings = Settings()