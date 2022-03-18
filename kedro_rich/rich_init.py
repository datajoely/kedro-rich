"""This module ensures that the rich logging and exceptions handlers are used"""
import click
import rich
import rich.traceback
from kedro.io.data_catalog import DataCatalog
from kedro.pipeline.node import Node

from kedro_rich.utilities.kedro_override_utils import (
    override_catalog_load,
    override_catalog_save,
    override_kedro_cli_get_command,
    override_kedro_proj_logging_handler,
    override_node_str,
)


def override_kedro_lib_logging():
    """This method overrides default Kedro methods to prettify the logging
    output, longer term this could just involve changes to Kedro core.
    """

    Node.__str__ = override_node_str
    DataCatalog.load = override_catalog_load
    DataCatalog.save = override_catalog_save


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
    override_kedro_proj_logging_handler()
    override_kedro_lib_logging()
    override_kedro_cli_get_command()
    apply_rich_tracebacks()
