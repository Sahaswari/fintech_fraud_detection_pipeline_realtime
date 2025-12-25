"""
Tests for Transaction Producer
Validates transaction generation logic and fraud pattern injection
"""

import pytest
import sys
import os
import uuid
from unittest.mock import patch, MagicMock
from datetime import datetime

# Ensure project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Pre-mock KafkaProducer before importing the module
import unittest.mock
_kafka_mock = unittest.mock.MagicMock()
with patch.dict('sys.modules', {'kafka': _kafka_mock}):
    from producers.producer_transaction import TransactionProducer


# ---------------------------------------------------------------------------
# Helper: build a lightweight TransactionProducer without connecting to Kafka
# ---------------------------------------------------------------------------
@pytest.fixture
def producer():
    """Create a TransactionProducer with mocked Kafka connection"""
    with patch.object(TransactionProducer, '__init__', lambda self, **kw: None):
        prod = TransactionProducer.__new__(TransactionProducer)
        # Manually initialize the attributes that __init__ sets
        prod.producer = MagicMock()
        prod.categories = [
            'RESTAURANT', 'ELECTRONICS', 'GROCERY', 'FUEL',
            'PHARMACY', 'CLOTHING', 'ENTERTAINMENT', 'TRAVEL'
        ]
        prod.countries = [
            'Sri Lanka', 'India', 'USA', 'UK', 'Singapore',
            'UAE', 'Australia', 'Canada', 'Germany', 'Japan'
        ]
        prod.user_last_location = {}
        prod.user_last_timestamp = {}
        yield prod


# ===========================
# Normal Transaction Tests
# ===========================

class TestNormalTransaction:
    """Tests for generate_normal_transaction()"""

    def test_normal_transaction_has_required_fields(self, producer):
        """Every normal transaction must contain all required fields"""
        tx = producer.generate_normal_transaction('user_0001')
        required_keys = {'transaction_id', 'user_id', 'timestamp',
                         'merchant_category', 'amount', 'location'}
        assert required_keys.issubset(tx.keys())

    def test_normal_transaction_user_id(self, producer):
        """User ID must match the one passed in"""
        tx = producer.generate_normal_transaction('user_0042')
        assert tx['user_id'] == 'user_0042'

    def test_normal_transaction_amount_range(self, producer):
        """Normal amounts should be between $5 and $500"""
        for _ in range(50):
            tx = producer.generate_normal_transaction('user_0001')
            assert 5 <= tx['amount'] <= 500, (
                f"Normal amount {tx['amount']} out of range [5, 500]"
            )

    def test_normal_transaction_valid_category(self, producer):
        """Merchant category must be one of the predefined categories"""
        valid_categories = {
            'RESTAURANT', 'ELECTRONICS', 'GROCERY', 'FUEL',
            'PHARMACY', 'CLOTHING', 'ENTERTAINMENT', 'TRAVEL'
        }
        for _ in range(30):
            tx = producer.generate_normal_transaction('user_0001')
            assert tx['merchant_category'] in valid_categories

    def test_normal_transaction_valid_location(self, producer):
        """Normal transactions happen in nearby countries"""
        nearby = {'Sri Lanka', 'India', 'Singapore'}
        for _ in range(30):
            tx = producer.generate_normal_transaction('user_0001')
            assert tx['location'] in nearby

    def test_normal_transaction_uuid_format(self, producer):
        """Transaction ID must be a valid UUID"""
        tx = producer.generate_normal_transaction('user_0001')
        parsed = uuid.UUID(tx['transaction_id'])
        assert str(parsed) == tx['transaction_id']

    def test_normal_transaction_timestamp_is_iso(self, producer):
        """Timestamp must be valid ISO 8601"""
        tx = producer.generate_normal_transaction('user_0001')
        parsed = datetime.fromisoformat(tx['timestamp'])
        assert isinstance(parsed, datetime)

    def test_normal_transaction_updates_user_location(self, producer):
        """After generation the user's last-known location must be updated"""
        tx = producer.generate_normal_transaction('user_0010')
        assert 'user_0010' in producer.user_last_location
        assert producer.user_last_location['user_0010'] == tx['location']


# ===========================
# High-Value Fraud Tests
# ===========================

class TestHighValueFraud:
    """Tests for generate_high_value_fraud()"""

    def test_high_value_amount_above_threshold(self, producer):
        """High-value fraud must be above $5,000"""
        for _ in range(50):
            tx = producer.generate_high_value_fraud('user_0001')
            assert tx['amount'] > 5000, (
                f"High-value fraud amount {tx['amount']} is not > 5000"
            )

    def test_high_value_amount_upper_bound(self, producer):
        """High-value fraud should not exceed $15,000"""
        for _ in range(50):
            tx = producer.generate_high_value_fraud('user_0001')
            assert tx['amount'] <= 15000, (
                f"High-value amount {tx['amount']} exceeds upper bound 15000"
            )

    def test_high_value_uses_risky_categories(self, producer):
        """High-value fraud targets specific categories"""
        risky = {'ELECTRONICS', 'TRAVEL', 'ENTERTAINMENT'}
        for _ in range(30):
            tx = producer.generate_high_value_fraud('user_0001')
            assert tx['merchant_category'] in risky

    def test_high_value_has_required_fields(self, producer):
        """High-value transactions must contain all required fields"""
        tx = producer.generate_high_value_fraud('user_0001')
        required_keys = {'transaction_id', 'user_id', 'timestamp',
                         'merchant_category', 'amount', 'location'}
        assert required_keys.issubset(tx.keys())

    def test_high_value_location_any_country(self, producer):
        """High-value fraud can originate from any country"""
        all_countries = set(producer.countries)
        locations = set()
        for _ in range(200):
            tx = producer.generate_high_value_fraud('user_0001')
            locations.add(tx['location'])
        # Should hit at least a few distinct countries
        assert len(locations) >= 3


# ===========================
# Impossible Travel Fraud Tests
# ===========================

class TestImpossibleTravelFraud:
    """Tests for generate_impossible_travel_fraud()"""

    def test_impossible_travel_returns_second_leg(self, producer):
        """The method should return the second (distant) transaction"""
        tx = producer.generate_impossible_travel_fraud('user_0005')
        assert tx is not None
        assert tx['user_id'] == 'user_0005'

    def test_impossible_travel_distant_location(self, producer):
        """Second leg must be in a distant country"""
        distant = {'USA', 'UK', 'Australia', 'Germany'}
        tx = producer.generate_impossible_travel_fraud('user_0005')
        assert tx['location'] in distant

    def test_impossible_travel_normal_amount(self, producer):
        """Impossible travel transactions use normal-range amounts"""
        tx = producer.generate_impossible_travel_fraud('user_0005')
        assert 30 <= tx['amount'] <= 300


# ===========================
# Send Transaction Tests
# ===========================

class TestSendTransaction:
    """Tests for send_transaction()"""

    def test_send_calls_kafka_producer(self, producer):
        """send_transaction should call producer.send()"""
        tx = producer.generate_normal_transaction('user_0001')
        producer.send_transaction(tx)
        producer.producer.send.assert_called_once()

    def test_send_uses_transactions_topic(self, producer):
        """Messages should be sent to the 'transactions' topic"""
        tx = producer.generate_normal_transaction('user_0001')
        producer.send_transaction(tx)
        call_args = producer.producer.send.call_args
        assert call_args[0][0] == 'transactions'

    def test_send_key_is_user_id(self, producer):
        """Kafka message key should be the user_id"""
        tx = producer.generate_normal_transaction('user_0001')
        producer.send_transaction(tx)
        call_kwargs = producer.producer.send.call_args
        assert call_kwargs[1]['key'] == 'user_0001'
