"""Simple module for maintaining configuration"""

KEDRO_RICH_ENABLED = "KEDRO_RICH_ENABLED"

RICH_LOGGING_HANDLER = {
    "class": "rich.logging.RichHandler",
    "level": "INFO",
    "markup": False,
    "log_time_format": "[%X]",
}
