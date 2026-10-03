"""
Fast unit tests for the validation core. No Docker/network required -
run with: python -m pytest test_validation.py -v
"""
import pytest
from validation import validate_record, extract_after, ValidationError


def test_valid_record_passes():
    record = {"id": 1001, "first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com"}
    assert validate_record(record) == record


def test_missing_field_raises():
    record = {"id": 1002, "first_name": "Grace", "email": "grace@example.com"}  # missing last_name
    with pytest.raises(ValidationError, match="last_name"):
        validate_record(record)


def test_wrong_type_raises():
    record = {"id": "not-a-number", "first_name": "Grace", "last_name": "Hopper", "email": "g@example.com"}
    with pytest.raises(ValidationError):
        validate_record(record)


def test_bad_email_raises():
    record = {"id": 1003, "first_name": "Alan", "last_name": "Turing", "email": "not-an-email"}
    with pytest.raises(ValidationError, match="@"):
        validate_record(record)


def test_extract_after_from_debezium_envelope():
    envelope = {"payload": {"after": {"id": 1, "first_name": "A", "last_name": "B", "email": "a@b.com"}}}
    assert extract_after(envelope) == {"id": 1, "first_name": "A", "last_name": "B", "email": "a@b.com"}


def test_extract_after_plain_record_passthrough():
    record = {"id": 1, "first_name": "A", "last_name": "B", "email": "a@b.com"}
    assert extract_after(record) == record
