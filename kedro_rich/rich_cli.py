"""Command line tools for manipulating a Kedro project.
Intended to be invoked via `kedro`."""
import importlib
import json
import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import click
import rich_click
import yaml
from kedro.framework.cli.catalog import create_catalog
from kedro.framework.cli.project import (
    ASYNC_ARG_HELP,
    CONFIG_FILE_HELP,
    FROM_INPUTS_HELP,
    FROM_NODES_HELP,
    LOAD_VERSION_HELP,
    NODE_ARG_HELP,
    PARALLEL_ARG_HELP,
    PARAMS_ARG_HELP,
    PIPELINE_ARG_HELP,
    RUNNER_ARG_HELP,
    TAG_ARG_HELP,
    TO_NODES_HELP,
    TO_OUTPUTS_HELP,
)
from kedro.framework.cli.utils import (
    CONTEXT_SETTINGS,
    KedroCliError,
    _config_file_callback,
    _reformat_load_versions,
    _split_params,
    env_option,
    split_string,
)
from kedro.framework.session import KedroSession
from kedro.framework.startup import ProjectMetadata
from kedro.pipeline import Pipeline
from kedro.utils import load_obj
from rich import box
from rich.panel import Panel
from rich.style import Style
from rich.table import Table

from kedro_rich.constants import (
    KEDRO_RICH_CATALOG_LIST_THRESHOLD,
    KEDRO_RICH_PROGRESS_ENV_VAR_KEY,
)
from kedro_rich.utilities.catalog_utils import (
    get_catalog_datasets,
    get_datasets_by_pipeline,
    summarise_datasets_as_list,
)


def _create_session(package_name: str, **kwargs):
    kwargs.setdefault("save_on_close", False)
    try:
        return KedroSession.create(package_name, **kwargs)
    except Exception as exc:
        raise KedroCliError(
            f"Unable to instantiate Kedro session.\nError: {exc}"
        ) from exc


def _get_values_as_tuple(values: Iterable[str]) -> Tuple[str, ...]:
    return tuple(chain.from_iterable(value.split(",") for value in values))


@click.group(context_settings=CONTEXT_SETTINGS, name="kedro-rich")
def commands():
    """Command line tools for manipulating a Kedro project."""


@commands.command(cls=rich_click.RichCommand)
@click.option(
    "--from-inputs", type=str, default="", help=FROM_INPUTS_HELP, callback=split_string
)
@click.option(
    "--to-outputs", type=str, default="", help=TO_OUTPUTS_HELP, callback=split_string
)
@click.option(
    "--from-nodes", type=str, default="", help=FROM_NODES_HELP, callback=split_string
)
@click.option(
    "--to-nodes", type=str, default="", help=TO_NODES_HELP, callback=split_string
)
@click.option("--node", "-n", "node_names", type=str, multiple=True, help=NODE_ARG_HELP)
@click.option(
    "--runner", "-r", type=str, default=None, multiple=False, help=RUNNER_ARG_HELP
)
@click.option("--parallel", "-p", is_flag=True, multiple=False, help=PARALLEL_ARG_HELP)
@click.option("--async", "is_async", is_flag=True, multiple=False, help=ASYNC_ARG_HELP)
@env_option
@click.option("--tag", "-t", type=str, multiple=True, help=TAG_ARG_HELP)
@click.option(
    "--load-version",
    "-lv",
    type=str,
    multiple=True,
    help=LOAD_VERSION_HELP,
    callback=_reformat_load_versions,
)
@click.option("--pipeline", type=str, default=None, help=PIPELINE_ARG_HELP)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help=CONFIG_FILE_HELP,
    callback=_config_file_callback,
)
@click.option(
    "--params", type=str, default="", help=PARAMS_ARG_HELP, callback=_split_params
)
def run(
    tag,
    env,
    parallel,
    runner,
    is_async,
    node_names,
    to_nodes,
    from_nodes,
    from_inputs,
    to_outputs,
    load_version,
    pipeline,
    config,  # pylint: disable=unused-argument
    params,
):  # pylint: disable=too-many-arguments, too-many-locals
    """Run the pipeline."""
    if parallel and runner:
        raise KedroCliError(
            "Both --parallel and --runner options cannot be used together. "
            "Please use either --parallel or --runner."
        )
    runner = runner or "SequentialRunner"
    if parallel:
        runner = "ParallelRunner"
    else:
        os.environ[KEDRO_RICH_PROGRESS_ENV_VAR_KEY] = "1"

    runner_class = load_obj(runner, "kedro.runner")

    tag = _get_values_as_tuple(tag) if tag else tag
    node_names = _get_values_as_tuple(node_names) if node_names else node_names
    package_name = str(Path.cwd().resolve().name)
    with KedroSession.create(package_name, env=env, extra_params=params) as session:
        session.run(
            tags=tag,
            runner=runner_class(is_async=is_async),
            node_names=node_names,
            from_nodes=from_nodes,
            to_nodes=to_nodes,
            from_inputs=from_inputs,
            to_outputs=to_outputs,
            load_versions=load_version,
            pipeline_name=pipeline,
        )
    if os.environ.get(KEDRO_RICH_PROGRESS_ENV_VAR_KEY):
        del os.environ[KEDRO_RICH_PROGRESS_ENV_VAR_KEY]


@commands.group(cls=rich_click.RichGroup)
def catalog():
    """Commands for working with catalog."""
    pass


catalog.add_command(create_catalog)


# pylint: disable=too-many-locals
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
    mapped_datasets = summarise_datasets_as_list(pipeline_datasets, catalog_datasets)
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
        table = _prepare_rich_table(records=mapped_datasets, pipes=pipelines)
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


def _prepare_rich_table(
    records: List[Dict[str, Any]], pipes: Dict[str, Pipeline]
) -> Table:
    """This method will build a rich.Table object based on the
    a given list of records and a dictionary of registered pipelines

    Args:
        records (List[Dict[str, Any]]): The catalog records
        pipes (Dict[str, Pipeline]): The pipelines to map to linked datasets

    Returns:
        Table: The table to render
    """

    table = Table(show_header=True, header_style=Style(color="white"), box=box.ROUNDED)
    # only include namespace if at least one present in catalog
    includes_namespaces = any(x["namespace"] for x in records)
    collapse_pipes = len(pipes.keys()) > KEDRO_RICH_CATALOG_LIST_THRESHOLD

    # define table headers
    namespace_columns = ["namespace"] if includes_namespaces else []
    pipe_columns = ["pipeline_count"] if collapse_pipes else list(pipes.keys())
    columns_to_add = namespace_columns + ["dataset_name", "dataset_type"] + pipe_columns

    # add table headers
    for column in columns_to_add:
        table.add_column(column, justify="center")

    # add table rows
    for index, row in enumerate(records):

        # work out if the dataset_type is the same / different to next row
        same_section, new_section = _describe_boundary(
            index=index,
            records=records,
            key="dataset_type",
            current_value=row["dataset_type"],
        )

        # add namespace if present
        if includes_namespaces:
            table_namespace = (
                [row["namespace"]] if row["namespace"] else ["[bright_black]n/a[/]"]
            )
        else:
            table_namespace = []

        # get catalog key
        table_dataset_name = [row["key"]]

        # get dataset_type, only show if different from the last record
        table_dataset_type = (
            [f"[magenta][b]{row['dataset_type']}[/][/]"] if new_section else [""]
        )

        # get pipelines attached to this dataset
        dataset_pipes = row["pipelines"]
        # get pipelines registered in this project
        proj_pipes = sorted(pipes.keys())

        # if too many pipelines registered, simply show the count
        if collapse_pipes:
            table_pipes = [str(len(dataset_pipes))]
        else:
            # show ✓ and ✘ if present
            table_pipes = [
                _check_cross(pipe in (set(proj_pipes) & set(dataset_pipes)))
                for pipe in proj_pipes
            ]

        # build full row
        renderables = (
            table_namespace + table_dataset_name + table_dataset_type + table_pipes
        )

        # add row to table
        table.add_row(*renderables, end_section=not same_section)
    return table


def _describe_boundary(
    index: int, records: List[Dict[str, Any]], key: str, current_value: str
) -> Tuple[bool, bool]:
    """
    Give a list of dictionaries, key and current value this method will
    return two booleans detailing if the sequence has changed or not
    """
    same_section = index + 1 < len(records) and records[index + 1][key] == current_value
    new_section = index == 0 or records[index - 1][key] != current_value

    return same_section, new_section


def _check_cross(overlap: bool) -> str:
    """Retrun check or cross mapped to True or False"""
    return "[bold green]✓[/]" if overlap else "[bold red]✘[/]"


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
