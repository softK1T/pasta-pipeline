"""
Configuration management for the Pasta Pipeline.
Centralizes all configuration settings and provides validation.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import timedelta

logger = logging.getLogger(__name__)


@dataclass
class TelegramConfig:
    """Telegram API configuration."""
    api_id: str
    api_hash: str
    phone_number: str
    session_string: str
    channel_name: str = "@mrakopedia"
    time_limit_days: int = 7
    max_messages_per_batch: int = 1000
    retry_attempts: int = 3
    retry_delay: int = 5

    def validate(self) -> bool:
        """Validate Telegram configuration."""
        required_fields = ['api_id', 'api_hash', 'phone_number', 'session_string']
        for field in required_fields:
            if not getattr(self, field):
                logger.error(f"Missing required Telegram config: {field}")
                return False
        return True


@dataclass
class DatabaseConfig:
    """Database configuration."""
    connection_string: str
    batch_size: int = 100
    max_retries: int = 3
    pool_size: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600

    def validate(self) -> bool:
        """Validate database configuration."""
        if not self.connection_string:
            logger.error("Missing database connection string")
            return False
        return True


@dataclass
class TelegraphConfig:
    """Telegraph processing configuration."""
    max_retries: int = 3
    retry_delay: int = 2
    request_timeout: int = 30
    rate_limit_delay: int = 1
    max_content_length: int = 1000000  # 1MB
    max_links_per_batch: int = 1000


@dataclass
class AirflowConfig:
    """Airflow configuration."""
    admin_password: str
    secret_key: str
    executor: str = "LocalExecutor"
    load_examples: bool = False
    webserver_port: int = 8080
    scheduler_heartbeat: int = 5
    dagbag_import_timeout: int = 30


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    telegram: TelegramConfig
    database: DatabaseConfig
    telegraph: TelegraphConfig
    airflow: AirflowConfig
    
    # Processing modes
    processing_mode: str = "incremental"  # incremental, full, refresh_old, daily
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Data retention
    data_retention_days: int = 90
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090

    def validate(self) -> bool:
        """Validate all configuration sections."""
        validations = [
            self.telegram.validate(),
            self.database.validate(),
        ]
        return all(validations)


class ConfigManager:
    """Configuration manager for the pipeline."""
    
    def __init__(self, env_file: str = ".env"):
        self.env_file = env_file
        self.config = self._load_config()
    
    def _load_config(self) -> PipelineConfig:
        """Load configuration from environment variables."""
        try:
            # Load environment variables
            if os.path.exists(self.env_file):
                from dotenv import load_dotenv
                load_dotenv(self.env_file)
            
            # Telegram config
            telegram_config = TelegramConfig(
                api_id=os.getenv("API_ID", ""),
                api_hash=os.getenv("API_HASH", ""),
                phone_number=os.getenv("PHONE_NUMBER", ""),
                session_string=os.getenv("SESSION_STRING", ""),
                channel_name=os.getenv("CHANNEL_NAME", "@mrakopedia"),
                time_limit_days=int(os.getenv("TIME_LIMIT_DAYS", "7")),
                max_messages_per_batch=int(os.getenv("MAX_MESSAGES_PER_BATCH", "1000")),
                retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
                retry_delay=int(os.getenv("RETRY_DELAY", "5"))
            )
            
            # Database config
            database_config = DatabaseConfig(
                connection_string=os.getenv("AIRFLOW_SQL_CONN", ""),
                batch_size=int(os.getenv("DB_BATCH_SIZE", "100")),
                max_retries=int(os.getenv("DB_MAX_RETRIES", "3")),
                pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
                pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
                pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "3600"))
            )
            
            # Telegraph config
            telegraph_config = TelegraphConfig(
                max_retries=int(os.getenv("TELEGRAPH_MAX_RETRIES", "3")),
                retry_delay=int(os.getenv("TELEGRAPH_RETRY_DELAY", "2")),
                request_timeout=int(os.getenv("TELEGRAPH_TIMEOUT", "30")),
                rate_limit_delay=int(os.getenv("TELEGRAPH_RATE_LIMIT", "1")),
                max_content_length=int(os.getenv("TELEGRAPH_MAX_CONTENT", "1000000")),
                max_links_per_batch=int(os.getenv("TELEGRAPH_MAX_LINKS", "1000"))
            )
            
            # Airflow config
            airflow_config = AirflowConfig(
                admin_password=os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"),
                secret_key=os.getenv("AIRFLOW_SECRET_KEY", "your-secret-key"),
                executor=os.getenv("AIRFLOW_EXECUTOR", "LocalExecutor"),
                load_examples=os.getenv("AIRFLOW_LOAD_EXAMPLES", "false").lower() == "true",
                webserver_port=int(os.getenv("AIRFLOW_WEBSERVER_PORT", "8080")),
                scheduler_heartbeat=int(os.getenv("AIRFLOW_SCHEDULER_HEARTBEAT", "5")),
                dagbag_import_timeout=int(os.getenv("AIRFLOW_DAGBAG_TIMEOUT", "30"))
            )
            
            # Main config
            config = PipelineConfig(
                telegram=telegram_config,
                database=database_config,
                telegraph=telegraph_config,
                airflow=airflow_config,
                processing_mode=os.getenv("PROCESSING_MODE", "incremental"),
                log_level=os.getenv("LOG_LEVEL", "INFO"),
                data_retention_days=int(os.getenv("DATA_RETENTION_DAYS", "90")),
                enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
                metrics_port=int(os.getenv("METRICS_PORT", "9090"))
            )
            
            # Validate configuration
            if not config.validate():
                raise ValueError("Configuration validation failed")
            
            logger.info("Configuration loaded successfully")
            return config
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def get_telegram_config(self) -> TelegramConfig:
        """Get Telegram configuration."""
        return self.config.telegram
    
    def get_database_config(self) -> DatabaseConfig:
        """Get database configuration."""
        return self.config.database
    
    def get_telegraph_config(self) -> TelegraphConfig:
        """Get Telegraph configuration."""
        return self.config.telegraph
    
    def get_airflow_config(self) -> AirflowConfig:
        """Get Airflow configuration."""
        return self.config.airflow
    
    def get_processing_mode(self) -> str:
        """Get current processing mode."""
        return self.config.processing_mode
    
    def get_log_level(self) -> str:
        """Get log level."""
        return self.config.log_level
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "telegram": {
                "api_id": self.config.telegram.api_id,
                "api_hash": self.config.telegram.api_hash,
                "phone_number": self.config.telegram.phone_number,
                "channel_name": self.config.telegram.channel_name,
                "time_limit_days": self.config.telegram.time_limit_days,
                "max_messages_per_batch": self.config.telegram.max_messages_per_batch
            },
            "database": {
                "batch_size": self.config.database.batch_size,
                "max_retries": self.config.database.max_retries,
                "pool_size": self.config.database.pool_size
            },
            "telegraph": {
                "max_retries": self.config.telegraph.max_retries,
                "request_timeout": self.config.telegraph.request_timeout,
                "rate_limit_delay": self.config.telegraph.rate_limit_delay,
                "max_content_length": self.config.telegraph.max_content_length
            },
            "airflow": {
                "executor": self.config.airflow.executor,
                "webserver_port": self.config.airflow.webserver_port,
                "load_examples": self.config.airflow.load_examples
            },
            "pipeline": {
                "processing_mode": self.config.processing_mode,
                "log_level": self.config.log_level,
                "data_retention_days": self.config.data_retention_days,
                "enable_metrics": self.config.enable_metrics
            }
        }


# Global configuration instance
config_manager = ConfigManager()


def get_config() -> PipelineConfig:
    """Get the global configuration instance."""
    return config_manager.config


def get_telegram_config() -> TelegramConfig:
    """Get Telegram configuration."""
    return config_manager.get_telegram_config()


def get_database_config() -> DatabaseConfig:
    """Get database configuration."""
    return config_manager.get_database_config()


def get_telegraph_config() -> TelegraphConfig:
    """Get Telegraph configuration."""
    return config_manager.get_telegraph_config()


def get_airflow_config() -> AirflowConfig:
    """Get Airflow configuration."""
    return config_manager.get_airflow_config()