"""Unit tests for code/quality/check_transactions.py."""

from conftest import make_txn
from check_transactions import run_checks


def outcome(results, name):
    return next(passed for check, passed, _ in results if check == name)


def good_rows():
    rows = [make_txn(nameOrig=f"C{i}") for i in range(99)]
    rows.append(make_txn(type="TRANSFER", isFraud=1, nameOrig="C99"))
    return rows


def test_clean_data_passes_everything(txn_df):
    results = run_checks(txn_df(good_rows()), expected_rows=100)
    assert all(passed for _, passed, _ in results), results


def test_wrong_row_count_fails(txn_df):
    results = run_checks(txn_df(good_rows()), expected_rows=101)
    assert not outcome(results, "row count matches expected")


def test_unknown_type_fails(txn_df):
    rows = good_rows() + [make_txn(type="WIRE")]
    assert not outcome(run_checks(txn_df(rows)), "transaction types valid")


def test_negative_amount_fails(txn_df):
    rows = good_rows() + [make_txn(amount=-5.0)]
    assert not outcome(run_checks(txn_df(rows)), "amount >= 0")


def test_null_key_fails(txn_df):
    rows = good_rows() + [make_txn(nameOrig=None)]
    assert not outcome(run_checks(txn_df(rows)), "no nulls in key columns")


def test_missing_column_fails(spark):
    df = spark.createDataFrame([(1, "PAYMENT")], "step INT, type STRING")
    assert not outcome(run_checks(df), "schema: required columns present")
