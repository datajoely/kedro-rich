from functools import reduce
import pytest

from kedro_rich.utilities.catalog_utils import (
    filter_datasets_by_pipeline,
    get_datasets_by_pipeline,
    resolve_pipeline_namespace,
    resolve_catalog_namespace,
    get_catalog_datasets,
    split_catalog_namespace_key,
    report_datasets_as_list,
)


@pytest.mark.parametrize(
    "given_key,expected_key",
    [
        ("ds.something.something_else.key", "ds__something__something_else__key"),
        ("ds.key", "ds__key"),
        ("key", "key"),
    ],
)
def test_resolve_pipeline_namespace(given_key: str, expected_key: str):
    assert resolve_pipeline_namespace(given_key) == expected_key


@pytest.mark.parametrize(
    "given_key,expected_key",
    [
        ("ds__something__something_else__key", "ds.something.something_else.key"),
        ("ds__key", "ds.key"),
        ("key", "key"),
    ],
)
def test_resolve_catalog_namespace(given_key: str, expected_key: str):
    assert resolve_catalog_namespace(given_key) == expected_key


def test_get_catalog_datasets(data_catalog_fixture):
    datasets_remaining = get_catalog_datasets(
        catalog=data_catalog_fixture, exclude_types=("MemoryDataSet",), drop_params=True
    )
    assert len(datasets_remaining) == 2
    assert all(x == "PickleDataSet" for x in datasets_remaining.values())

    datasets_remaining_keep_everything = get_catalog_datasets(
        catalog=data_catalog_fixture, drop_params=False
    )

    assert len(datasets_remaining_keep_everything) == 4
    assert "MemoryDataSet" in datasets_remaining_keep_everything.values()


def test_filter_datasets_by_pipeline(data_catalog_fixture, pipeline_fixture):
    pipe_datasets = filter_datasets_by_pipeline(
        datasets=get_catalog_datasets(
            catalog=data_catalog_fixture,
            exclude_types=("MemoryDataSet",),
            drop_params=True,
        ),
        pipeline=pipeline_fixture,
    )
    all_dataset_keys = reduce(
        lambda x, y: x | y, [set(x.keys()) for x in pipe_datasets]
    )
    all_dataset_types = reduce(
        lambda x, y: x | y, [set(x.values()) for x in pipe_datasets]
    )
    assert all_dataset_keys == {"dataset_1", "dataset_3"}
    assert all_dataset_types == {"PickleDataSet"}


def test_get_datasets_by_pipeline(data_catalog_fixture, pipeline_fixture):
    dataset_pipes = get_datasets_by_pipeline(
        catalog=data_catalog_fixture, registry={"__default__": pipeline_fixture}
    )
    assert set(reduce(lambda x, y: x + y, dataset_pipes.values())) == {"__default__"}
    assert set(dataset_pipes.keys()) == {"dataset_1", "dataset_2", "dataset_3"}


@pytest.mark.parametrize(
    "given_key,expected_namespace,expected_key",
    [
        ("ds.something.something_else.key", "ds.something.something_else", "key"),
        ("ds.key", "ds", "key"),
        ("key", None, "key"),
    ],
)
def test_split_catalog_namespace_key(given_key, expected_namespace, expected_key):
    namespace, key = split_catalog_namespace_key(given_key)
    assert (namespace, key) == (expected_namespace, expected_key)


def test_report_datasets_as_list(data_catalog_fixture, pipeline_fixture):
    reported_list = report_datasets_as_list(
        catalog_datasets=get_catalog_datasets(
            catalog=data_catalog_fixture,
            exclude_types=("MemoryDataSet",),
            drop_params=True,
        ),
        pipeline_datasets=get_datasets_by_pipeline(
            catalog=data_catalog_fixture, registry={"__default__": pipeline_fixture}
        ),
    )
    assert [
        {
            "dataset_type": "PickleDataSet",
            "pipelines": ["__default__"],
            "namespace": None,
            "key": "dataset_1",
        },
        {
            "dataset_type": "PickleDataSet",
            "pipelines": ["__default__"],
            "namespace": None,
            "key": "dataset_3",
        },
    ] == reported_list
