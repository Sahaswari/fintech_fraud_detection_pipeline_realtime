"""
Tests for Fraud Detection Rules
Validates the fraud detection logic used in the Spark streaming pipeline
without requiring a live Spark/Kafka cluster.

The rules under test:
  1. HIGH_VALUE  — any transaction with amount > $5,000
  2. IMPOSSIBLE_TRAVEL — same user in different countries within 10 minutes
"""

import pytest
from datetime import datetime, timedelta


# ============================================================
# Pure-Python reference implementations of the fraud rules
# (mirrors the logic in spark_jobs/fraud_detection_stream.py)
# ============================================================

HIGH_VALUE_THRESHOLD = 5000  # dollars


def is_high_value_fraud(amount: float) -> bool:
    """Return True if a transaction amount exceeds the high-value threshold."""
    return amount > HIGH_VALUE_THRESHOLD


def detect_impossible_travel(transactions: list, window_minutes: int = 10) -> list:
    """
    Given a list of transaction dicts (with user_id, timestamp, location),
    return a list of (user_id, tx_pair) tuples flagged as impossible travel.

    A pair is flagged when the same user transacts in two *different*
    countries within `window_minutes` minutes.
    """
    # Sort by timestamp
    sorted_tx = sorted(transactions, key=lambda t: t['timestamp'])
    flagged = []

    # Group by user
    user_txns = {}
    for tx in sorted_tx:
        uid = tx['user_id']
        user_txns.setdefault(uid, []).append(tx)

    for uid, txns in user_txns.items():
        for i in range(len(txns)):
            for j in range(i + 1, len(txns)):
                t1 = txns[i]
                t2 = txns[j]
                time_diff = (t2['timestamp'] - t1['timestamp']).total_seconds() / 60
                if time_diff <= window_minutes and t1['location'] != t2['location']:
                    flagged.append((uid, (t1, t2)))

    return flagged


# ===========================
# High-Value Rule Tests
# ===========================

class TestHighValueRule:
    """Unit tests for the high-value fraud detection rule"""

    def test_above_threshold_is_fraud(self):
        """Amount above $5,000 should be flagged"""
        assert is_high_value_fraud(5001) is True
        assert is_high_value_fraud(10000) is True
        assert is_high_value_fraud(15000) is True

    def test_at_threshold_is_not_fraud(self):
        """Exactly $5,000 should NOT be flagged (rule is '> 5000')"""
        assert is_high_value_fraud(5000) is False

    def test_below_threshold_is_not_fraud(self):
        """Normal amounts should not be flagged"""
        assert is_high_value_fraud(4999.99) is False
        assert is_high_value_fraud(100) is False
        assert is_high_value_fraud(0) is False

    def test_edge_just_above(self):
        """$5,000.01 should be flagged"""
        assert is_high_value_fraud(5000.01) is True

    def test_negative_amount(self):
        """Negative/refund amounts should not be flagged"""
        assert is_high_value_fraud(-100) is False

    @pytest.mark.parametrize("amount,expected", [
        (4999, False),
        (5000, False),
        (5001, True),
        (10000, True),
        (250, False),
        (7500.50, True),
    ])
    def test_parametrized_amounts(self, amount, expected):
        """Parametrized check across typical values"""
        assert is_high_value_fraud(amount) == expected


# ===========================
# Impossible Travel Tests
# ===========================

class TestImpossibleTravelRule:
    """Unit tests for the impossible travel fraud detection rule"""

    def _make_tx(self, user_id, location, minutes_offset=0):
        """Helper to build a transaction dict"""
        return {
            'transaction_id': f'tx_{user_id}_{minutes_offset}',
            'user_id': user_id,
            'timestamp': datetime(2025, 1, 1, 12, 0) + timedelta(minutes=minutes_offset),
            'merchant_category': 'ELECTRONICS',
            'amount': 100.0,
            'location': location,
        }

    def test_same_user_different_country_within_window(self):
        """Two transactions in different countries within 10 min → flagged"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'USA', 5),
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 1
        assert flagged[0][0] == 'user_01'

    def test_same_user_same_country(self):
        """Two transactions in the same country → NOT flagged"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'Sri Lanka', 3),
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 0

    def test_same_user_different_country_outside_window(self):
        """Different countries but > 10 min apart → NOT flagged"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'USA', 15),
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 0

    def test_different_users_different_countries(self):
        """Different users in different countries → NOT flagged"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_02', 'USA', 3),
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 0

    def test_exactly_at_window_boundary(self):
        """Exactly 10 minutes apart → should still be flagged (<=10)"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'Germany', 10),
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 1

    def test_multiple_users_mixed(self):
        """Multiple users — only the offending user pair is flagged"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'USA', 5),       # flagged
            self._make_tx('user_02', 'India', 0),
            self._make_tx('user_02', 'India', 3),      # same country — ok
            self._make_tx('user_03', 'UK', 0),
            self._make_tx('user_03', 'Japan', 20),     # outside window — ok
        ]
        flagged = detect_impossible_travel(txns)
        assert len(flagged) == 1
        assert flagged[0][0] == 'user_01'

    def test_empty_transaction_list(self):
        """No transactions → no flags"""
        assert detect_impossible_travel([]) == []

    def test_single_transaction(self):
        """A single transaction can never be impossible travel"""
        txns = [self._make_tx('user_01', 'Sri Lanka', 0)]
        assert detect_impossible_travel(txns) == []

    def test_three_countries_within_window(self):
        """Three different countries for same user within window → multiple flags"""
        txns = [
            self._make_tx('user_01', 'Sri Lanka', 0),
            self._make_tx('user_01', 'USA', 3),
            self._make_tx('user_01', 'Germany', 7),
        ]
        flagged = detect_impossible_travel(txns)
        # Pairs: (SL,USA), (SL,DE), (USA,DE) → 3 flags
        assert len(flagged) == 3
