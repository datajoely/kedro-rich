from copy import deepcopy
from kedro.io import MemoryDataSet
from kedro_rich.utilities.catalog_utils import (
    get_catalog_datasets,
    get_datasets_by_pipeline,
    report_datasets_as_list,
)
from kedro_rich.utilities.formatting_utils import (
    prepare_rich_table,
    get_kedro_logo,
    print_kedro_pipeline_init_screen,
)
from kedro_rich.constants import KEDRO_RICH_CATALOG_LIST_THRESHOLD
from kedro.pipeline import Pipeline
import kedro


def test_get_kedro_logo():
    logo = get_kedro_logo()
    assert len(logo) == 9
    assert max(len(x) for x in logo) == 43


def test_prepare_rich_table_no_namespace(data_catalog_fixture, pipeline_fixture):
    registry = {"__default__": pipeline_fixture}
    catalog_datasets = get_catalog_datasets(data_catalog_fixture, drop_params=True)
    pipeline_datasets = get_datasets_by_pipeline(data_catalog_fixture, registry)
    mapped_datasets = report_datasets_as_list(pipeline_datasets, catalog_datasets)
    table = prepare_rich_table(records=mapped_datasets, registry=registry)
    assert len(table.columns) == 2 + len(registry)
    assert len(table.rows) == len(mapped_datasets)


def test_prepare_rich_table_namespace(data_catalog_fixture, pipeline_fixture):
    registry = {"__default__": pipeline_fixture}
    data_catalog = deepcopy(data_catalog_fixture)
    data_catalog.add("namespace.dataset_name", MemoryDataSet())
    catalog_datasets = get_catalog_datasets(data_catalog, drop_params=True)
    pipeline_datasets = get_datasets_by_pipeline(data_catalog, registry)
    mapped_datasets = report_datasets_as_list(pipeline_datasets, catalog_datasets)
    table = prepare_rich_table(records=mapped_datasets, registry=registry)
    assert len(table.columns) == 3 + len(registry)
    assert len(table.rows) == len(mapped_datasets)


def test_prepare_rich_table_threshold(data_catalog_fixture, pipeline_fixture):
    registry = {"__default__": pipeline_fixture}
    custom_registry = deepcopy(registry)
    for i in range(KEDRO_RICH_CATALOG_LIST_THRESHOLD):
        custom_registry[f"custom_{i}"] = Pipeline([])
    catalog_datasets = get_catalog_datasets(data_catalog_fixture, drop_params=True)
    pipeline_datasets = get_datasets_by_pipeline(data_catalog_fixture, custom_registry)
    mapped_datasets = report_datasets_as_list(pipeline_datasets, catalog_datasets)
    table = prepare_rich_table(records=mapped_datasets, registry=custom_registry)
    assert len(table.columns) == 3
    assert len(table.rows) == len(mapped_datasets)


def test_print_kedro_pipeline_init_screen(capsys):

    print_kedro_pipeline_init_screen()
    captured = capsys.readouterr()

    assert kedro.__version__ in captured.out
