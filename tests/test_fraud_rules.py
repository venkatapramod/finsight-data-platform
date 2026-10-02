"""Unit tests for code/spark/fraud_rules.py."""

import pytest

from conftest import make_txn
from fraud_rules import (
    RULE_VERSION,
    fraud_condition,
    legacy_rule,
    sender_drained_rule,
)


def is_flagged(txn_df, rule_fn, **txn):
    return txn_df([make_txn(**txn)]).filter(rule_fn()).count() == 1


# ---- v2: sender drained ------------------------------------------------

@pytest.mark.parametrize("txn_type", ["TRANSFER", "CASH_OUT"])
def test_v2_flags_full_balance_drain(txn_df, txn_type):
    assert is_flagged(txn_df, sender_drained_rule,
                      type=txn_type, amount=5000.0, oldbalanceOrg=5000.0,
                      newbalanceOrig=0.0)


def test_v2_ignores_partial_transfer(txn_df):
    assert not is_flagged(txn_df, sender_drained_rule,
                          type="TRANSFER", amount=4000.0, oldbalanceOrg=5000.0)


@pytest.mark.parametrize("txn_type", ["PAYMENT", "DEBIT", "CASH_IN"])
def test_v2_ignores_low_risk_types(txn_df, txn_type):
    assert not is_flagged(txn_df, sender_drained_rule,
                          type=txn_type, amount=5000.0, oldbalanceOrg=5000.0)


def test_v2_ignores_empty_sender_account(txn_df):
    assert not is_flagged(txn_df, sender_drained_rule,
                          type="TRANSFER", amount=0.0, oldbalanceOrg=0.0)


def test_v2_tolerates_rounding(txn_df):
    assert is_flagged(txn_df, sender_drained_rule,
                      type="TRANSFER", amount=1234.565, oldbalanceOrg=1234.57)


def test_v2_handles_null_balance(txn_df):
    assert not is_flagged(txn_df, sender_drained_rule,
                          type="TRANSFER", amount=500.0, oldbalanceOrg=None)


# ---- v1: legacy rule (kept for comparison) -------------------------------

def test_v1_flags_large_transfer_to_zero_balance(txn_df):
    assert is_flagged(txn_df, legacy_rule,
                      type="TRANSFER", amount=250000.0, newbalanceDest=0.0)


def test_v1_ignores_amount_at_threshold(txn_df):
    assert not is_flagged(txn_df, legacy_rule,
                          type="TRANSFER", amount=200000.0, newbalanceDest=0.0)


# ---- current rule ------------------------------------------------------

def test_current_rule_is_v2(txn_df):
    """The live rule must flag the drained account, not the legacy pattern."""
    assert RULE_VERSION.startswith("v2")
    df = txn_df([
        make_txn(nameOrig="DRAINED", type="TRANSFER",
                 amount=5000.0, oldbalanceOrg=5000.0),
        make_txn(nameOrig="LEGACY_ONLY", type="TRANSFER",
                 amount=250000.0, oldbalanceOrg=900000.0, newbalanceDest=0.0),
    ])
    flagged = {r["nameOrig"] for r in df.filter(fraud_condition()).collect()}
    assert flagged == {"DRAINED"}
