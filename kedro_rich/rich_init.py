"""This module ensures that the rich logging and exceptions handlers are used"""
import logging
from typing import Callable

import click
import rich
import rich.logging
from kedro.framework.session import KedroSession

from kedro_rich.settings import RICH_LOGGING_HANDLER


def apply_rich_logging_handler():
    """
    This function does two things:

    (1) It mutates the dictionary provided by `logging.yml` to
    use the `rich.logging.RichHandler` instead of the standard output one
    (2) It enables the rich.Traceback handler so that exceptions are prettier
    """

    # ensure warnings are caught by logger not stout
    logging.captureWarnings(True)

    def _replace_console_handler(func: Callable) -> Callable:
        """This function mutates the dictionary returned by reading logging.yml"""

        def wrapped(*args, **kwargs):
            logging_config = func(*args, **kwargs)
            logging_config["handlers"]["console"] = RICH_LOGGING_HANDLER
            return logging_config

        return wrapped

    # I hate this - currently the only way to change the handlers
    # provided by the user in their conf/base/logging.yml is to
    # mutate the result of the function call

    # pylint: disable=protected-access
    KedroSession._get_logging_config = _replace_console_handler(
        KedroSession._get_logging_config
    )


def apply_rich_tracebacks():
    """
    This method ensures that tracebacks raised by the Kedro project
    go through the rich traceback method

    The `suppress=[click]` argument means that exceptions will not
    show the frames related to the CLI framework and only the actual
    logic the user defines.
    """
    rich.traceback.install(show_locals=False, suppress=[click])


def start_up():
    """This method runs the setup methods needed to override
    certain defaults at start up
    """
    apply_rich_logging_handler()
    apply_rich_tracebacks()
