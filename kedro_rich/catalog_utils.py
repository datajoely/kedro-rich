"""This module includes helper functions for managing the data catalog"""
from functools import reduce
from typing import Dict, List, Optional, Set, Tuple

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline


def get_catalog_datasets(
    catalog: DataCatalog, exclude: Tuple[str] = ()
) -> Dict[str, str]:
    """Filter to only persisted datasets"""
    datasets = {
        k: type(v).__name__
        for k, v in catalog.datasets.__dict__.items()
        if type(v).__name__ not in exclude
    }
    return datasets


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
            return {x.replace(".", "__") for x in datasets}
        return set(datasets)

    inputs = reduce(
        lambda a, x: a | _clean_names(x.inputs, x.namespace), pipeline.nodes, set()
    )
    outputs = reduce(
        lambda a, x: a | _clean_names(x.outputs, x.namespace), pipeline.nodes, set()
    )

    persisted_inputs = {k: v for k, v in datasets.items() if k in inputs}
    persisted_outputs = {k: v for k, v in datasets.items() if k in outputs}

    return persisted_inputs, persisted_outputs


def resolve_pipeline_namespace(dataset_name: str) -> str:
    """Resolves the dot to double underscore namespace
    discrepancy between pipeline inputs and catalog keys
    """
    return dataset_name.replace(".", "__")


def split_catalog_namespace_key(dataset_name: str) -> Tuple[str, str]:
    """This method splits out a catalog name from it's namespace"""
    dataset_split = dataset_name.split(".")
    namespace = ".".join(dataset_split[:-1])
    data_set_name = dataset_split[-1]
    return namespace, data_set_name
