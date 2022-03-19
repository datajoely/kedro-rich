"""This module provides lifecycle hooks to track progress"""
import logging
import os
import time
from datetime import timedelta
from typing import Any, Dict

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
)
from rich.text import Text

from kedro_rich.constants import (
    KEDRO_RICH_PROGRESS_ENV_VAR_KEY,
    KEDRO_RICH_SHOW_DATASET_PROGRESS,
)
from kedro_rich.utilities.catalog_utils import (
    filter_datasets_by_pipeline,
    get_catalog_datasets,
    resolve_pipeline_namespace,
    split_catalog_namespace_key,
)
from kedro_rich.utilities.formatting_utils import print_kedro_pipeline_init_screen


class RichProgressHooks:
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
        """
        This method initialises the variables needed to track pipeline process. This
        will be disabled under parallel runner
        """
        if self._check_if_progress_bar_enabled():
            progress_desc_format = "[progress.description]{task.description}"
            progress_percentage_format = "[progress.percentage]{task.percentage:>3.0f}%"
            progress_activity_format = "{task.fields[activity]}"
            self.progress = Progress(
                _KedroElapsedColumn(),
                progress_desc_format,
                SpinnerColumn(),
                BarColumn(),
                progress_percentage_format,
                progress_activity_format,
            )

            # Get pipeline goals
            self._init_progress_tasks(pipeline, catalog)

            # Init tasks
            pipe_name = run_params.get("pipeline_name") or "__default__"
            input_cnt = len(self.pipeline_inputs)
            output_cnt = len(self.pipeline_outputs)

            dataset_tasks = (
                {
                    "loads": self._add_task(desc="Loading datasets", count=input_cnt),
                    "saves": self._add_task(desc="Saving datasets", count=output_cnt),
                }
                if KEDRO_RICH_SHOW_DATASET_PROGRESS
                else {}
            )

            overall_task = {
                "overall": self._add_task(
                    desc=f"Running [bright_magenta]'{pipe_name}'[/] pipeline",
                    count=self.task_count,
                )
            }

            self.tasks = {**dataset_tasks, **overall_task}

            print_kedro_pipeline_init_screen()

            # Start process
            self.progress.start()
        else:
            logger = logging.getLogger(__name__)
            logger.warning(
                "[orange1 bold]Progress bars are incompatible with ParallelRunner[/]",
            )

    @hook_impl
    def before_dataset_loaded(self, dataset_name: str):
        """
        Add the last dataset loaded (from persistent storage)
        to progress display
        """
        if KEDRO_RICH_SHOW_DATASET_PROGRESS:
            if self.progress:
                dataset_name_namespaced = resolve_pipeline_namespace(dataset_name)
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
        if KEDRO_RICH_SHOW_DATASET_PROGRESS:
            if self.progress:
                dataset_name_namespaced = resolve_pipeline_namespace(dataset_name)

                if dataset_name_namespaced in self.pipeline_outputs:
                    namespace, key = split_catalog_namespace_key(dataset_name)

                    data_string = (
                        f"[blue]{namespace}[/].{key}" if namespace else f"{key}"
                    )

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

    @hook_impl
    def after_pipeline_run(self):
        """Hook to complete and clean up progress information on pipeline completion"""
        if self.progress:
            self.progress.update(
                self.tasks["overall"],
                visible=True,
                activity="[bold green]✓ Pipeline complete[/] ",
            )
            if KEDRO_RICH_SHOW_DATASET_PROGRESS:
                self.progress.update(self.tasks["saves"], completed=100, visible=False)
                self.progress.update(self.tasks["loads"], completed=100, visible=False)
            time.sleep(0.1)  # allows the UI to clean up after the process ends

    def _init_progress_tasks(self, pipeline: Pipeline, catalog: DataCatalog):
        """This method initialises the key Hook constructor attributes"""
        self.task_count = len(pipeline.nodes)
        self.io_datasets_in_catalog = get_catalog_datasets(
            catalog=catalog, exclude=("MemoryDataSet",)
        )
        (self.pipeline_inputs, self.pipeline_outputs,) = filter_datasets_by_pipeline(
            datasets=self.io_datasets_in_catalog, pipeline=pipeline
        )

    def _add_task(self, desc: str, count: int) -> TaskID:
        """This method adds a task to the progress bar"""
        return self.progress.add_task(desc, total=count, activity="")

    @staticmethod
    def _check_if_progress_bar_enabled() -> bool:
        """Convert env variable into boolean"""
        return bool(int(os.environ.get(KEDRO_RICH_PROGRESS_ENV_VAR_KEY, "0")))


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


rich_hooks = RichProgressHooks()
