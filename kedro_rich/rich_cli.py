"""Command line tools for manipulating a Kedro project.
Intended to be invoked via `kedro`."""
import importlib
import json
import os
from typing import Callable, Dict

import click
import rich_click
import yaml
from kedro.framework.cli.catalog import _create_session, create_catalog
from kedro.framework.cli.project import run
from kedro.framework.cli.utils import CONTEXT_SETTINGS, env_option
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline
from rich import box
from rich.panel import Panel

from kedro_rich.constants import KEDRO_RICH_PROGRESS_ENV_VAR_KEY
from kedro_rich.utilities.catalog_utils import (
    get_catalog_datasets,
    get_datasets_by_pipeline,
    report_datasets_as_list,
)
from kedro_rich.utilities.formatting_utils import prepare_rich_table


@click.group(context_settings=CONTEXT_SETTINGS, name="kedro-rich")
def commands():
    """Command line tools for manipulating a Kedro project."""


def handle_parallel_run(func: Callable) -> Callable:
    """
    This method raps the run command callback so that the
    parallel runner is disabled
    """

    def wrapped(*args, **kwargs):
        """
        This method will add an environment variable if the user
        selects a parallel run, kedro-rich will disable the progress bar in
        that situation
        """

        # Only run progress bars if (1) In complex mode (2) NOT ParallelRunner
        if not kwargs["simple"]:
            if not kwargs["parallel"]:
                os.environ[KEDRO_RICH_PROGRESS_ENV_VAR_KEY] = "1"

        # drop 'simple' kwarg as that doesn't exist in the original func
        original_kwargs = {k: v for k, v in kwargs.items() if "simple" != k}
        result = func(*args, **original_kwargs)

        if os.environ.get(KEDRO_RICH_PROGRESS_ENV_VAR_KEY):
            del os.environ[KEDRO_RICH_PROGRESS_ENV_VAR_KEY]
        return result

    return wrapped


run.callback = handle_parallel_run(run.callback)
run.__class__ = rich_click.RichCommand
commands.add_command(
    click.option(
        "--simple",
        "-s",
        default=False,
        is_flag=True,
        help="Disable rich progress bars (simple mode)",
    )(run)
)


@commands.group(cls=rich_click.RichGroup)
def catalog():
    """Commands for working with catalog."""
    pass


catalog.add_command(create_catalog)


@catalog.command(cls=rich_click.RichCommand, name="list")
@env_option
@click.option(
    "--format",
    "-f",
    "fmt",
    default="yaml",
    type=click.Choice(["yaml", "json", "table"], case_sensitive=False),
    help="Output the 'yaml' (default) / 'json' results to stdout or pretty"
    " print 'table' to console",
)
@click.pass_obj
def list_datasets(metadata: ProjectMetadata, fmt: str, env: str):
    """Detail datasets by type."""

    # Needed to avoid circular reference
    from rich.console import Console  # pylint: disable=import-outside-toplevel

    pipelines = _get_pipeline_registry(metadata)
    session = _create_session(metadata.package_name, env=env)
    context = session.load_context()
    catalog_datasets = get_catalog_datasets(context.catalog, drop_params=True)
    pipeline_datasets = get_datasets_by_pipeline(context.catalog, pipelines)
    mapped_datasets = report_datasets_as_list(pipeline_datasets, catalog_datasets)
    console = Console()

    if fmt == "yaml":
        struct = {
            f"{x['namespace']}.{x['key']}" if x["namespace"] else x["key"]: x
            for x in mapped_datasets
        }
        console.out(yaml.safe_dump(struct))
    if fmt == "json":
        console.out(json.dumps(mapped_datasets, indent=2))
    elif fmt == "table":
        table = prepare_rich_table(records=mapped_datasets, pipes=pipelines)
        console.print(
            "\n",
            Panel(
                table,
                expand=False,
                title=f"Catalog contains [b][cyan]{len(mapped_datasets)}[/][/] persisted datasets",
                padding=(1, 1),
                box=box.MINIMAL,
            ),
        )


def _get_pipeline_registry(proj_metadata: ProjectMetadata) -> Dict[str, Pipeline]:
    """
    This method retrieves the pipelines registered in the project where
    the plugin in installed
    """
    # is this the right 0.18.x version of doing this?
    # The object is no longer in the context
    registry = importlib.import_module(
        f"{proj_metadata.package_name}.pipeline_registry"
    )
    return registry.register_pipelines()
