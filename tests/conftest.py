from kedro.io.data_catalog import DataCatalog
from kedro.io import MemoryDataSet
from kedro.pipeline import Pipeline, node
from kedro.extras.datasets.pickle import PickleDataSet
import pytest


@pytest.fixture()
def data_catalog_fixture() -> DataCatalog:

    return DataCatalog(
        data_sets={
            "dataset_1": PickleDataSet(filepath="test"),
            "dataset_2": MemoryDataSet(),
            "dataset_3": PickleDataSet(filepath="test"),
        },
        feed_dict={"params.modelling_params": {"test_size": 0.3, "split_ratio": 0.7}},
    )


@pytest.fixture()
def pipeline_fixture() -> Pipeline:
    return Pipeline(
        nodes=[
            node(func=lambda x: x, inputs="dataset_1", outputs="dataset_2"),
            node(
                func=lambda x: x,
                inputs="dataset_2",
                outputs="dataset_2.5",
                namespace="test",
            ),
            node(func=lambda x: x, inputs="dataset_2.5", outputs="dataset_3"),
        ]
    )
