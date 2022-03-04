"""This module ensures that the rich logging and exceptions handlers are used"""
from typing import Callable

import click
import rich
import rich.logging
from kedro.framework.session import KedroSession


def apply_rich_logging_handler():
    """
    This function does two things:

    (1) It mutates the dictionary provided by `logging.yml` to
    use the `rich.logging.RichHandler` instead of the standard output one
    (2) It enables the rich.Traceback handler so that exceptions are prettier
    """

    def replace_console_handler(func: Callable) -> Callable:
        """This function mutates the dictionary returned by reading logging.yml"""

        def wrapped(*args, **kwargs):
            logging_config = func(*args, **kwargs)
            logging_config["handlers"]["console"] = {
                "class": "rich.logging.RichHandler",
                "level": "INFO",
                "markup": False,
                "log_time_format": "[%X]",
            }
            return logging_config

        return wrapped

    # I hate this - currently the only way to change the handlers
    # provided by the user in their conf/base/logging.yml is to
    # mutate the result of the function call

    # pylint: disable=protected-access
    KedroSession._get_logging_config = replace_console_handler(
        KedroSession._get_logging_config
    )

    # The suppress=[click] command means that exceptions will not
    # show the frames related to
    rich.traceback.install(show_locals=False, suppress=[click])


rich_logging = apply_rich_logging_handler
