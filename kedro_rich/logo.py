"""This module prints the Kedro logo to the rich console"""

import kedro
from rich.console import Console


def print_kedro_logo() -> None:
    """
    THis method prints the Kedro logo, version
    """
    text = f"""
[orange1][b]         -         [/b][/orange1]
[orange1][b]       ·===·       [/b][/orange1]  [orange1][b]KEDRO[/b][/orange1] ({kedro.__version__})
[orange1][b]    ·==:   :==·    [/b][/orange1]  [gray][i]Reproducible, maintainable and modular data science code[/i][/gray]
[orange1][b]  ·==:       :==·  [/b][/orange1]
[orange1][b]    ·==:   :==·    [/b][/orange1]  https://github.com/kedro-org/kedro
[orange1][b]       ·===·       [/b][/orange1]  https://kedro.readthedocs.io
[orange1][b]         -         [/b][/orange1]
"""

    console = Console()
    console.print(text, no_wrap=True)
