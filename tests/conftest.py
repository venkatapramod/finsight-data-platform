"""Shared pytest fixtures for FinSight."""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "code", "spark"))
sys.path.insert(0, os.path.join(ROOT, "code", "quality"))

# Spark workers must use the same Python as the test runner.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]

SCHEMA = (
    "step INT, type STRING, amount DOUBLE, nameOrig STRING, "
    "oldbalanceOrg DOUBLE, newbalanceOrig DOUBLE, nameDest STRING, "
    "oldbalanceDest DOUBLE, newbalanceDest DOUBLE, isFraud INT, "
    "isFlaggedFraud INT"
)


def make_txn(**overrides):
    """A legitimate-looking PAYMENT; override fields per test."""
    row = {
        "step": 1, "type": "PAYMENT", "amount": 100.0,
        "nameOrig": "C1", "oldbalanceOrg": 1000.0, "newbalanceOrig": 900.0,
        "nameDest": "M1", "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "isFraud": 0, "isFlaggedFraud": 0,
    }
    row.update(overrides)
    return tuple(row[c] for c in COLUMNS)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("finsight-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def txn_df(spark):
    def build(rows):
        return spark.createDataFrame(rows, SCHEMA)
    return build
