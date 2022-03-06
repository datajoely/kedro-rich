"""Command line tools for manipulating a Kedro project.
Intended to be invoked via `kedro`."""
import importlib
import json
import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import click
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
from kedro.utils import load_obj
from rich import box
from rich.style import Style
from rich.table import Table

from kedro_rich.catalog_utils import (
    get_catalog_datasets,
    get_datasets_by_pipeline,
    resolve_catalog_namespace,
    split_catalog_namespace_key,
)
from kedro_rich.settings import KEDRO_RICH_ENABLED


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


@commands.command()
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
        os.environ[KEDRO_RICH_ENABLED] = "False"
        runner = "ParallelRunner"
    else:
        os.environ[KEDRO_RICH_ENABLED] = "True"

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
    del os.environ[KEDRO_RICH_ENABLED]


# pylint: disable=too-many-locals
@commands.command()
@env_option
@click.option(
    "--to-json",
    type=bool,
    default=False,
    help="Output the results as JSON",
    is_flag=True,
)
@click.pass_obj
def list_datasets(metadata: ProjectMetadata, to_json: bool, env: str):
    """This method provides mechanisms to print out the contents of the
    data catalog in a human readable view"""

    # Needed to avoid circular reference
    from rich.console import Console  # pylint: disable=import-outside-toplevel

    # is this the 0.18.x version of doing this?
    registry_py = importlib.import_module(metadata.package_name + ".pipeline_registry")
    pipelines = registry_py.register_pipelines()
    session = _create_session(metadata.package_name, env=env)
    context = session.load_context()
    catalog_datasets = get_catalog_datasets(catalog=context.catalog, drop_params=True)
    pipeline_datasets = get_datasets_by_pipeline(context.catalog, pipelines)
    mapped_datasets = get_dataset_summary(pipeline_datasets, catalog_datasets)

    cons = Console()

    if to_json:
        cons.print_json(json.dumps(mapped_datasets))
    else:

        pipeline_names = sorted(pipelines.keys())
        table = Table(
            show_header=True, header_style=Style(color="white"), box=box.ROUNDED
        )
        table.add_column("namespace")
        table.add_column("dataset_name")
        table.add_column("dataset_type")
        for pipeline_name in pipeline_names:
            table.add_column(pipeline_name, justify="center", footer="hello")

        for index, row in enumerate(mapped_datasets):
            ds_type = row["dataset_type"]

            same_section = (
                index + 1 < len(mapped_datasets)
                and mapped_datasets[index + 1]["dataset_type"] == ds_type
            )

            new_section = (
                index == 0 or mapped_datasets[index - 1]["dataset_type"] != ds_type
            )

            pipeline_mapping = [
                "[bold green]✓[/]" if x in row["pipelines"] else "[bold red]✘[/]"
                for x in pipeline_names
            ]

            table.add_row(
                row["namespace"] if row["namespace"] else "[grey50]n/a[/]",
                row["key"],
                "[magenta][b]" + ds_type + "[/][/]" if new_section else "",
                *pipeline_mapping,
                end_section=not same_section,
            )
        cons.print(table)


def get_dataset_summary(
    pipeline_datasets: Dict[List, str], catalog_datasets: List[Dict, str]
) -> List[Dict[str, Any]]:
    """This method accepts the datasets present in the pipeline registry
    as well as the full data catalog and produces a list of records
    which include key metadata such as the type, namespace, linked pipelines
    and dataset name (ordered by type)
    """
    return sorted(
        (
            {
                **{"dataset_type": v, "pipelines": pipeline_datasets.get(k, []),},
                **dict(
                    zip(
                        ("namespace", "key"),
                        split_catalog_namespace_key(
                            dataset_name=resolve_catalog_namespace(k)
                        ),
                    )
                ),
            }
            for k, v in catalog_datasets.items()
        ),
        key=lambda x: x["dataset_type"],
    )
