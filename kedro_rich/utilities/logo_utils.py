"""This module prints the Kedro logo to the rich console"""

from typing import List, Optional

import kedro
from rich.console import Console


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
