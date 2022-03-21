"""This module includes helper functions for managing the data catalog"""
import operator
from functools import reduce
from itertools import groupby
from typing import Any, Dict, List, Optional, Set, Tuple

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline


def get_catalog_datasets(
    catalog: DataCatalog, exclude_types: Tuple[str] = (), drop_params: bool = False
) -> Dict[str, str]:
    """Filter to only persisted datasets"""
    datasets_filtered = {
        k: type(v).__name__
        for k, v in catalog.datasets.__dict__.items()
        if type(v).__name__ not in exclude_types
    }
    if drop_params:
        datasets_w_param_filter = {
            k: v
            for k, v in datasets_filtered.items()
            if not k.startswith("params") and not k == "parameters"
        }
        return datasets_w_param_filter
    return datasets_filtered


def filter_datasets_by_pipeline(
    datasets: Dict[str, str], pipeline: Pipeline
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Retrieve datasets (inputs and outputs) which intersect against
    a given pipeline object

    This function also ensures namespaces are correctly rationalised.
    """

    def _clean_names(datasets: List[str], namespace: Optional[str]) -> Set[str]:
        if namespace:
            return {resolve_pipeline_namespace(x) for x in datasets}
        return set(datasets)

    inputs = reduce(
        lambda a, x: a | _clean_names(x.inputs, x.namespace), pipeline.nodes, set()
    )
    outputs = reduce(
        lambda a, x: a | _clean_names(x.outputs, x.namespace), pipeline.nodes, set()
    )

    pipeline_inputs = {
        k: v for k, v in datasets.items() if any(x.endswith(k) for x in inputs)
    }
    pipeline_outputs = {
        k: v for k, v in datasets.items() if any(x.endswith(k) for x in outputs)
    }
    return pipeline_inputs, pipeline_outputs


def get_datasets_by_pipeline(
    catalog: DataCatalog, registry: Dict[str, Pipeline]
) -> Dict[str, List[str]]:
    """This method will return a dictionary of datasets mapped to the
    list of pipelines they are used within

    Args:
        catalog (DataCatalog): The data catalog object
        pipelines (Dict[str, Pipeline]): The pipelines in this project

    Returns:
        Dict[str, List[str]]: The dataset to pipeline groups
    """
    # get non parameter dataset
    catalog_datasets = get_catalog_datasets(catalog=catalog, drop_params=True)

    # get node input and outputs
    pipeline_input_output_datasets = {
        pipeline_name: filter_datasets_by_pipeline(catalog_datasets, pipeline)
        for pipeline_name, pipeline in registry.items()
    }

    # get those that overlap with pipelines
    pipeline_datasets = {
        pipeline_name: reduce(
            lambda input, output: input.keys() | output.keys(), input_outputs
        )
        for pipeline_name, input_outputs in pipeline_input_output_datasets.items()
    }

    # get dataset to pipeline pairs
    dataset_pipeline_pairs = reduce(
        lambda x, y: x + y,
        (
            [(dataset, pipeline) for dataset in datasets]
            for pipeline, datasets in pipeline_datasets.items()
        ),
    )

    # get dataset to pipeline groups
    sorter = sorted(dataset_pipeline_pairs, key=operator.itemgetter(0))

    grouper = groupby(sorter, key=operator.itemgetter(0))

    dataset_pipeline_groups = {
        k: list(map(operator.itemgetter(1), v)) for k, v in grouper
    }
    return dataset_pipeline_groups


def resolve_pipeline_namespace(dataset_name: str) -> str:
    """Resolves the dot to double underscore namespace
    discrepancy between pipeline inputs/outputs and catalog keys
    """
    return dataset_name.replace(".", "__")


def resolve_catalog_namespace(dataset_name: str) -> str:
    """Resolves the double underscore to dot namespace
    discrepancy between catalog keys and pipeline inputs/outputs
    """
    return dataset_name.replace("__", ".")


def split_catalog_namespace_key(dataset_name: str) -> Tuple[Optional[str], str]:
    """This method splits out a catalog name from it's namespace"""
    dataset_split = dataset_name.split(".")
    namespace = ".".join(dataset_split[:-1])
    if namespace:
        dataset_name = dataset_split[-1]
        return namespace, dataset_name
    return None, dataset_name


def report_datasets_as_list(
    pipeline_datasets: Dict[str, List[str]], catalog_datasets: Dict[str, str]
) -> List[Dict[str, Any]]:
    """This method accepts the datasets present in the pipeline registry
    as well as the full data catalog and produces a list of records
    which include key metadata such as the type, namespace, linked pipelines
    and dataset name (ordered by type)
    """
    return sorted(
        (
            {
                **{
                    "dataset_type": v,
                    "pipelines": pipeline_datasets.get(k, []),
                },
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
