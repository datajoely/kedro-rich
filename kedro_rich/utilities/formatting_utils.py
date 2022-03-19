"""This module provides utilities that are helpful
for printing to the rich console"""

from typing import Any, Dict, List, Optional, Tuple

import kedro
from kedro.pipeline import Pipeline
from rich import box
from rich.console import Console
from rich.style import Style
from rich.table import Table

from kedro_rich.constants import KEDRO_RICH_CATALOG_LIST_THRESHOLD


def prepare_rich_table(
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


def get_kedro_logo(color: str = "orange1") -> Optional[List[str]]:
    """This method constructs an ascii Kedro logo"""
    diamond = """
         -
       ·===·
    ·==:   :==·
  ·==:       :==·
    ·==:   :==·
       ·===·
         -
    """.split(
        "\n"
    )
    color_rows = [
        f"[{color}][b]{x}[/b][/{color}]" if x.strip() else "" for x in diamond
    ]

    return color_rows


def print_kedro_pipeline_init_screen():
    """This method prints the Kedro logo and package metadata"""
    logo_rows = get_kedro_logo()
    lib_info = _get_library_info()
    mapping = ((2, "title"), (3, "tagline"), (-3, "github"), (-2, "rtd"))
    for index, key in mapping:
        spacing = (51 - len(logo_rows[index])) * " "
        logo_rows[index] = logo_rows[index] + spacing + lib_info[key]

    str_rows = "\n".join(logo_rows)
    Console().print(str_rows, no_wrap=True)


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


def _get_library_info(title_color: str = "orange1", tagline_color: str = "gray"):
    """This method collects package information to present to the user"""
    tagline_text = "Reproducible, maintainable and modular data science code"
    version_data = dict(
        title=f"[{title_color}][b]KEDRO[/][/{title_color}] ({kedro.__version__})",
        tagline=f"[{tagline_color}][i]{tagline_text}[/][/]",
        github="https://github.com/kedro-org/kedro",
        rtd="https://kedro.readthedocs.io",
    )
    return version_data
