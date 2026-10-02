"""Unit tests for code/spark/build_customer_360.py."""

import pytest

from conftest import make_txn
from build_customer_360 import (
    account_activity,
    build_customer_360,
    map_profiles_to_accounts,
)


@pytest.fixture
def profiles(spark):
    return spark.createDataFrame(
        [("C900", "Zed", ["savings"]), ("C100", "Ann", ["current_account", "car_loan"])],
        "customerId STRING, name STRING, products ARRAY<STRING>",
    )


@pytest.fixture
def txns(txn_df):
    return txn_df([
        # A1 sends twice and receives once -> 3 transactions
        make_txn(nameOrig="A1", nameDest="C_B2", amount=100.0, step=1),
        make_txn(nameOrig="A1", nameDest="M_SHOP", amount=50.0, step=2),
        make_txn(nameOrig="C_B2", nameDest="A1", amount=30.0, step=3, isFraud=1),
        # A3 sends once
        make_txn(nameOrig="A3", nameDest="M_SHOP", amount=10.0, step=4),
    ])


def test_activity_counts_sent_and_received(txns):
    rows = {r["accountId"]: r for r in account_activity(txns).collect()}
    assert rows["A1"]["txn_sent"] == 2
    assert rows["C_B2"]["txn_sent"] == 1 and rows["C_B2"]["txn_received"] == 1
    assert rows["A3"]["txn_total"] == 1


def test_merchants_are_not_customer_accounts(txns):
    ids = {r["accountId"] for r in account_activity(txns).collect()}
    assert "M_SHOP" not in ids


def test_received_side_only_counts_customer_destinations(txns):
    # A1 receives from C_B2 but its destination ID does not start with "C",
    # so only sends are counted for it.
    a1 = [r for r in account_activity(txns).collect() if r["accountId"] == "A1"][0]
    assert a1["txn_received"] == 0


def test_mapping_is_one_to_one_and_prefers_most_active(profiles, txns):
    m = {r["customerId"]: r["accountId"]
         for r in map_profiles_to_accounts(profiles, account_activity(txns)).collect()}
    assert len(m) == 2 and len(set(m.values())) == 2
    # Lowest customerId gets the most active account (A1: 2 txns vs C_B2 2 txns,
    # tie broken by accountId -> A1 first).
    assert m["C100"] == "A1"
    assert m["C900"] == "C_B2"


def test_mapping_is_deterministic(profiles, txns):
    act = account_activity(txns)
    first = sorted(map_profiles_to_accounts(profiles, act).collect())
    second = sorted(map_profiles_to_accounts(profiles.orderBy("name"), act).collect())
    assert first == second


def test_customer_360_flags_fraud_involvement(profiles, txns):
    rows = {r["customerId"]: r for r in
            build_customer_360(profiles, account_activity(txns)).collect()}
    assert rows["C900"]["fraud_involved"] is True     # C_B2 sent a fraud txn
    assert rows["C100"]["fraud_involved"] is False
    assert rows["C100"]["name"] == "Ann"               # profile fields kept


def test_more_profiles_than_accounts_leaves_extras_unmapped(spark, txns):
    many = spark.createDataFrame(
        [(f"C{i:03d}", f"P{i}", []) for i in range(5)],
        "customerId STRING, name STRING, products ARRAY<STRING>",
    )
    rows = build_customer_360(many, account_activity(txns)).collect()
    assert len(rows) == 5
    assert sum(r["accountId"] is not None for r in rows) == 3
