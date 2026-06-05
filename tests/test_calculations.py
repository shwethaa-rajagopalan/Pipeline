import pytest
from pyspark.sql import SparkSession

from calculations import compute_complex_metric
from calculations.core import compute_nii_business_unit


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_compute_complex_metric_basic(spark):
    data = [("A", 10.0, 1.0), ("A", 20.0, 0.0), ("B", 5.0, 1.0)]
    df = spark.createDataFrame(data, schema=["key", "value", "weight"])
    res = compute_complex_metric(df, {"min_weight": 0.0})
    rows = {r['key']: r['complex_metric'] for r in res.collect()}
    assert 'A' in rows and 'B' in rows
    assert rows['B'] == 5.0


def test_compute_nii_business_unit(spark):
    data = [
        ("run1", "BU1", "BSG1", "CC001", "Loan", "USD", 100000.0, 90000.0, 1.0, 0.98, 0.05, 1.0),
    ]
    schema = [
        "run_id",
        "business_unit",
        "business_segment_group",
        "cost_centre",
        "product",
        "currency",
        "forecast_balance",
        "launchpoint_balance",
        "currency_mix",
        "net_exposure_factor",
        "yield_delta",
        "day_count_fraction",
    ]
    df = spark.createDataFrame(data, schema=schema)
    result = compute_nii_business_unit(df)
    row = result.collect()[0]
    assert row["nii_business_unit"] == pytest.approx((100000.0 - 90000.0) * 1.0 * 0.98 * 0.05)
