"""This module provides lifecycle hooks to track progress"""
import os
import time
from datetime import timedelta
from functools import reduce
from typing import Any, Dict, List, Optional, Set, Tuple

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from rich.progress import BarColumn, Progress, ProgressColumn, Task, TaskID
from rich.text import Text

from kedro_rich.settings import RICH_ENABLED_ENV


class RichHooks:
    """These set of hooks add progress information to the output of a Kedro run"""

    def __init__(self):
        """This constructor initialises the variables used to manage state"""
        self.progress = None
        self.task_count = 0
        self.io_datasets_in_catalog = {}
        self.pipeline_inputs = {}
        self.pipeline_outputs = {}
        self.tasks = {}

    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ):
        """This method initialises the variables needed to track pipeline process"""
        if bool(
            os.environ.get(RICH_ENABLED_ENV, False)
        ):  # disable under ParallelRunner
            self.progress = Progress(
                _KedroElapsedColumn(),
                "[progress.description]{task.description}",
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                "{task.fields[activity]}",
            )

            # Get pipeline goals
            self._init_progress_tasks(pipeline, catalog)

            # Init tasks
            pipe_name = run_params.get("pipeline_name") or "__default__"
            self.tasks = {
                "overall": self._add_progress_task(
                    description=f"Running [bright_magenta]'{pipe_name}'[/] pipeline",
                    count=self.task_count,
                ),
                "loads": self._add_progress_task(
                    description="Loading datasets", count=len(self.pipeline_inputs)
                ),
                "saves": self._add_progress_task(
                    description="Saving datasets", count=len(self.pipeline_outputs)
                ),
            }

            # Start process
            self.progress.start()

    @hook_impl
    def after_dataset_loaded(self, dataset_name: str):
        """
        Add the last dataset loaded (from persistent storage)
        to progress display
        """
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
        """Add the last dataset persisted to progress display"""
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
        """Add the current function name to progress display"""
        if self.progress:
            self.progress.update(
                self.tasks["overall"],
                activity=f"[violet]𝑓𝑥[/]{' ':<5}[orange1]{node.func.__name__}[/]()",
            )

    @hook_impl
    def after_node_run(self):
        """Increment the task count on node completion"""
        if self.progress:
            self.progress.update(self.tasks["overall"], advance=1)
            time.sleep(0.3)

    @hook_impl
    def after_pipeline_run(self):
        """Hook to complete and clean up progress information on pipeline completion"""
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
        """Identify ephemeral datasets"""
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
        """
        Retrieve datasets (inputs and outputs) which are either
        loaded from or written to disk as part of the pipeline in scope.

        This function also ensures namespaces are correctly rationalised.
        """

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


class _KedroElapsedColumn(ProgressColumn):
    """Renders time elapsed for top task only"""

    def render(self, task: Task) -> Text:
        """Show time remaining."""
        if task.id == 0:
            elapsed = task.finished_time if task.finished else task.elapsed
            if elapsed is None:
                return Text("-:--:--", style="cyan")
            delta = timedelta(seconds=int(elapsed))
            return Text(str(delta), style="green")
        return None


rich_hooks = RichHooks()
