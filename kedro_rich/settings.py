"""Simple module for maintaining configuration"""

KEDRO_RICH_ENABLED = "KEDRO_RICH_ENABLED"
KEDRO_RICH_SHOW_DATASET_PROGRESS = True

KEDRO_RICH_LOGGING_HANDLER = {
    "class": "rich.logging.RichHandler",
    "level": "INFO",
    "markup": False,
    "log_time_format": "[%X]",
}

KEDRO_RICH_CATALOG_LIST_THRESHOLD = 10
