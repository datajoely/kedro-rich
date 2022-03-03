import os
import time
from datetime import timedelta
from functools import reduce
from typing import Any, Dict, List, Optional, Set, Tuple
from pathlib import Path
import logging

import click
import kedro

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from rich.progress import BarColumn, Progress, ProgressColumn, Task, TaskID
from rich.text import Text
from rich.traceback import install
from rich.logging import RichHandler

from kedro_rich.settings import RICH_ENABLED_ENV    

class RichHooks:
    def __init__(self):
        self.progress = None
        self.task_count = 0
        self.io_datasets_in_catalog = {}
        self.pipeline_inputs = {}
        self.pipeline_outputs = {}
        self.tasks = {}
        #_install_rich_traceback()
        _install_rich_logging_handler()

    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ):
        if os.environ.get(RICH_ENABLED_ENV):

            self.progress = Progress(
                _KedroElapsedColumn(),
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                "{task.fields[activity]}",
            )
            self._init_progress_tasks(pipeline, catalog)
            pipe_name = run_params.get("pipeline_name") or "__default__"
            self.tasks = {
                "overall": self._add_progress_task(
                    description=f"Running [bright_magenta]'{pipe_name}'[/] pipeline",
                    count=self.task_count,
                ),
                "loads": self._add_progress_task(
                    description=f"Loading datasets", count=len(self.pipeline_inputs)
                ),
                "saves": self._add_progress_task(
                    description=f"Saving datasets", count=len(self.pipeline_outputs)
                ),
            }

            self.progress.start()

    @hook_impl
    def after_dataset_loaded(self, dataset_name):
        if self.progress:
            dataset_name_namespaced = dataset_name.replace(".", "__")
            if dataset_name in self.pipeline_inputs:
                dataset_type = self.io_datasets_in_catalog[dataset_name_namespaced]
                dataset_desc = (
                    f"📂{' ':<5}[i]{dataset_name}[/] ([bold cyan]{dataset_type}[/])"
                )
                self.progress.update(
                    self.tasks["loads"], advance=1, activity=dataset_desc
                )

    @hook_impl
    def after_dataset_saved(self, dataset_name: str):
        if self.progress:
            dataset_name_namespaced = dataset_name.replace(".", "__")

            if dataset_name_namespaced in self.pipeline_outputs:
                dataset_split = dataset_name.split(".")
                namespace = ".".join(dataset_split[:-1])
                data = dataset_split[-1]
                if namespace:
                    data_string = f"[blue]{namespace}[/].{data}"
                else:
                    data_string = f"{data}"

                dataset_type = self.io_datasets_in_catalog[dataset_name_namespaced]
                dataset_desc = (
                    f"💾{' ':<5}[i]{data_string}[/] ([bold cyan]{dataset_type}[/])"
                )
                self.progress.update(
                    self.tasks["saves"], advance=1, activity=dataset_desc
                )

    @hook_impl
    def before_node_run(self, node: Node):
        if self.progress:
            self.progress.update(
                self.tasks["overall"],
                activity=f"[violet]𝑓𝑥[/]{' ':<5}[orange1]{node.func.__name__}[/]()",
            )

    @hook_impl
    def after_node_run(self, node):
        if self.progress:
            self.progress.update(self.tasks["overall"], advance=1)
            time.sleep(0.3)

    @hook_impl
    def after_pipeline_run(self):
        if self.progress:
            self.progress.update(
                self.tasks["overall"],
                visible=True,
                activity="[bold green]✓ Pipeline complete[/] ",
            )
            self.progress.update(self.tasks["saves"], completed=100, visible=False)
            self.progress.update(self.tasks["loads"], completed=100, visible=False)
            time.sleep(0.1)

    def _init_progress_tasks(self, pipeline: Pipeline, catalog: DataCatalog):
        self.task_count = len(pipeline.nodes)
        self.io_datasets_in_catalog = self._get_persisted_datasets(catalog)

        (
            self.pipeline_inputs,
            self.pipeline_outputs,
        ) = self._get_persisted_datasets_in_scope(self.io_datasets_in_catalog, pipeline)

    @staticmethod
    def _get_persisted_datasets(catalog: DataCatalog) -> Dict[str, str]:
        non_memory_datasets = {
            k: type(v).__name__
            for k, v in catalog.datasets.__dict__.items()
            if type(v).__name__ != "MemoryDataSet"
        }
        return non_memory_datasets

    @staticmethod
    def _get_persisted_datasets_in_scope(
        non_memory: Dict[str, str], pipe: Pipeline
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        def _clean_names(datasets: List[str], namespace: Optional[str]) -> Set[str]:
            if namespace:
                return {x.replace(".", "__") for x in datasets}
            return set(datasets)

        inputs = reduce(
            lambda a, x: a | _clean_names(x.inputs, x.namespace), pipe.nodes, set()
        )
        outputs = reduce(
            lambda a, x: a | _clean_names(x.outputs, x.namespace), pipe.nodes, set()
        )

        persisted_inputs = {k: v for k, v in non_memory.items() if k in inputs}
        persisted_outputs = {k: v for k, v in non_memory.items() if k in outputs}

        return persisted_inputs, persisted_outputs

    def _add_progress_task(self, description, count) -> TaskID:
        return self.progress.add_task(description, total=count, activity="")


def _install_rich_traceback():
    install(show_locals=False, suppress=[click, kedro])

def _install_rich_logging_handler():
    logging.basicConfig(
        level="INFO",
        handlers=[RichHandler(rich_tracebacks=True)]
    )


class _KedroElapsedColumn(ProgressColumn):
    """Renders time elapsed for top task only"""

    def render(self, task: "Task") -> Text:
        """Show time remaining."""
        if task.id == 0:
            elapsed = task.finished_time if task.finished else task.elapsed
            if elapsed is None:
                return Text("-:--:--", style="cyan")
            delta = timedelta(seconds=int(elapsed))
            return Text(str(delta), style="green")


hooks = RichHooks()