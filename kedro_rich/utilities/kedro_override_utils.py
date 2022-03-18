"""This module provides methods which we use to override default Kedro methods"""
# pylint: disable=protected-access
import logging
from typing import Any, Callable, Optional, Set

import rich
from click.core import _check_multicommand
from kedro.framework.cli.cli import KedroCLI
from kedro.framework.session import KedroSession
from kedro.io.core import AbstractVersionedDataSet, Version
from rich.panel import Panel

from kedro_rich.constants import KEDRO_RICH_LOGGING_HANDLER


def override_node_str(self) -> str:
    """This method rich-ifies the node.__str__ method"""

    def _drop_namespaces(xset: Set[str]) -> Optional[Set]:
        """This method cleans up the namesapces"""
        split = {x.split(".")[-1] for x in xset}
        if split:
            return split
        return None

    func_name = f"[magenta]𝑓𝑥 {self._func_name}([/]"
    inputs = _drop_namespaces(self.inputs)
    bridge = "[magenta])[/] [cyan]➡[/] "
    outputs = _drop_namespaces(self.outputs)
    return f"{func_name}{inputs}{bridge}{outputs}"


def override_catalog_load(self, name: str, version: str = None) -> Any:
    """Loads a registered data set (Rich-ified output).

    Args:
        name: A data set to be loaded.
        version: Optional argument for concrete data version to be loaded.
            Works only with versioned datasets.

    Returns:
        The loaded data as configured.

    Raises:
        DataSetNotFoundError: When a data set with the given name
            has not yet been registered.

    Example:
    ::

        >>> from kedro.io import DataCatalog
        >>> from kedro.extras.datasets.pandas import CSVDataSet
        >>>
        >>> cars = CSVDataSet(filepath="cars.csv",
        >>>                   load_args=None,
        >>>                   save_args={"index": False})
        >>> io = DataCatalog(data_sets={'cars': cars})
        >>>
        >>> df = io.load("cars")
    """
    load_version = Version(version, None) if version else None
    dataset = self._get_dataset(name, version=load_version)

    self._logger.info(
        "Loading data from [bright_blue]%s[/] ([bright_blue][b]%s[/][/])...",
        name,
        type(dataset).__name__,
    )

    func = self._get_transformed_dataset_function(name, "load", dataset)
    result = func()

    version = (
        dataset.resolve_load_version()
        if isinstance(dataset, AbstractVersionedDataSet)
        else None
    )

    # Log only if versioning is enabled for the data set
    if self._journal and version:
        self._journal.log_catalog(name, "load", version)
    return result


def override_catalog_save(self, name: str, data: Any) -> None:
    """Save data to a registered data set.

    Args:
        name: A data set to be saved to.
        data: A data object to be saved as configured in the registered
            data set.

    Raises:
        DataSetNotFoundError: When a data set with the given name
            has not yet been registered.

    Example:
    ::

        >>> import pandas as pd
        >>>
        >>> from kedro.extras.datasets.pandas import CSVDataSet
        >>>
        >>> cars = CSVDataSet(filepath="cars.csv",
        >>>                   load_args=None,
        >>>                   save_args={"index": False})
        >>> io = DataCatalog(data_sets={'cars': cars})
        >>>
        >>> df = pd.DataFrame({'col1': [1, 2],
        >>>                    'col2': [4, 5],
        >>>                    'col3': [5, 6]})
        >>> io.save("cars", df)
    """
    dataset = self._get_dataset(name)

    self._logger.info(
        "Saving data to [bright_blue]%s[/] ([bright_blue][b]%s[/][/])...",
        name,
        type(dataset).__name__,
    )

    func = self._get_transformed_dataset_function(name, "save", dataset)
    func(data)

    version = (
        dataset.resolve_save_version()
        if isinstance(dataset, AbstractVersionedDataSet)
        else None
    )

    # Log only if versioning is enabled for the data set
    if self._journal and version:
        self._journal.log_catalog(name, "save", version)


def override_kedro_proj_logging_handler():
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
            logging_config["handlers"]["console"] = KEDRO_RICH_LOGGING_HANDLER
            return logging_config

        return wrapped

    # pylint: disable=protected-access
    KedroSession._get_logging_config = _replace_console_handler(
        KedroSession._get_logging_config
    )


def override_kedro_cli_get_command():
    """This method overrides the Click get_command() method
    so that we can give the user a useful message if they try to do a Kedro
    project command outside of a project directory
    """

    # pylint: disable=invalid-name
    # pylint: disable=inconsistent-return-statements
    def _get_command(self, ctx, cmd_name):
        for source in self.sources:
            rv = source.get_command(ctx, cmd_name)
            if rv is not None:
                if self.chain:
                    _check_multicommand(self, cmd_name, rv)
                return rv
        if not self._metadata:

            warn = "[orange1][b]You are not in a Kedro project[/]![/]"
            result = "Project specific commands such as '[bright_cyan]run[/]' or \
'[bright_cyan]jupyter[/]' are only available within a project directory."
            solution = "[bright_black][b]Hint:[/] [i]Kedro is looking for a file called \
'[magenta]pyproject.toml[/]', is one present in your current working directory?[/][/]"
            msg = f"{warn} {result}\n\n{solution}"
            console = rich.console.Console()
            panel = Panel(
                msg,
                title=f"Command '{cmd_name}' not found",
                expand=False,
                border_style="dim",
                title_align="left",
            )
            console.print("\n", panel, "\n")

    KedroCLI.get_command = _get_command
