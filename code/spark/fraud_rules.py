"""
FinSight fraud rules: single source of truth.

Streaming, evaluation and tests all import from here, so a rule change
is made once and measured once.

Rule history (scored with code/spark/evaluate_fraud_rules.py on the
PaySim labels, tune = steps 1-100, test = steps 101-154):

    v1  legacy_rule()
        TRANSFER/CASH_OUT and amount > 200,000 and newbalanceDest == 0
        test precision 40.79%, recall 32.98%
        Uses newbalanceDest, a post-transaction value.

    v2  sender_drained_rule()   <- current
        TRANSFER/CASH_OUT and the amount equals the sender's full
        opening balance (account drained)
        test precision 100.00%, recall 99.47%
        Uses only amount and oldbalanceOrg, both known at authorisation.

Caveat: v2's near-perfect score reflects how the PaySim simulator
generates fraud (fraud agents empty the victim's account). It should not
be read as real-world performance.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

RULE_VERSION = "v2-sender-drained"

RISKY_TYPES = ("TRANSFER", "CASH_OUT")
LEGACY_AMOUNT_THRESHOLD = 200_000
BALANCE_TOLERANCE = 0.01


def legacy_rule() -> Column:
    """v1: original project-specification rule (kept for comparison)."""
    c = F.col
    return (
        c("type").isin(*RISKY_TYPES)
        & (c("amount") > LEGACY_AMOUNT_THRESHOLD)
        & (c("newbalanceDest") == 0)
    )


def sender_drained_rule() -> Column:
    """v2: risky transaction type that moves the sender's entire balance."""
    c = F.col
    return (
        c("type").isin(*RISKY_TYPES)
        & (c("oldbalanceOrg") > 0)
        & (F.abs(c("amount") - c("oldbalanceOrg")) < BALANCE_TOLERANCE)
    )


def fraud_condition() -> Column:
    """The rule currently used for alerting."""
    return sender_drained_rule()


def fraud_reason() -> Column:
    """Human-readable reason attached to each alert."""
    return F.lit("Sender account drained by TRANSFER/CASH_OUT")
