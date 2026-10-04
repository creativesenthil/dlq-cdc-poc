"""
Shared validation logic used by both the lightweight consumer and the
optional Flink job. A record is "valid" only if it matches REQUIRED_FIELDS
exactly in type, and has a plausible email. Anything else raises
ValidationError, which the caller is expected to route to the DLQ.
"""

import json


REQUIRED_FIELDS = {
    "id": int,
    "first_name": str,
    "last_name": str,
    "email": str,
}


class ValidationError(Exception):
    """Raised when a record fails schema validation."""
    pass


def validate_record(record: dict) -> dict:
    """
    Validates a single CDC record (already unwrapped from the Debezium
    envelope, i.e. just the 'after' payload).

    Returns the record unchanged if valid.
    Raises ValidationError with a human-readable reason if not.
    """
    if not isinstance(record, dict):
        raise ValidationError("record is not a JSON object")

    missing = [f for f in REQUIRED_FIELDS if f not in record or record[f] is None]
    if missing:
        raise ValidationError(
            f"missing required field(s): {', '.join(missing)}"
        )

    type_errors = []

    for field, expected_type in REQUIRED_FIELDS.items():
        value = record[field]

        if not isinstance(value, expected_type):
            type_errors.append(
                f"{field} expected {expected_type.__name__}, "
                f"got {type(value).__name__}"
            )

    if type_errors:
        raise ValidationError("; ".join(type_errors))

    if "@" not in record["email"]:
        raise ValidationError("email missing '@'")

    return record


def extract_after(debezium_envelope: dict) -> dict:
    """
    Extract the MongoDB document from the Debezium envelope.

    Debezium MongoDB emits the 'after' field as a JSON string,
    so deserialize it before validation.
    """

    if isinstance(debezium_envelope, dict) and "payload" in debezium_envelope:
        payload = debezium_envelope["payload"]
        after = payload.get("after")

        if not after:
            return {}

        if isinstance(after, str):
            return json.loads(after)

        return after

    if isinstance(debezium_envelope, dict) and "after" in debezium_envelope:
        after = debezium_envelope.get("after")

        if not after:
            return {}

        if isinstance(after, str):
            return json.loads(after)

        return after

    return debezium_envelope
