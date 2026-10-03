"""
Shared validation logic used by both the lightweight consumer and the
optional Flink job. A record is "valid" only if it matches REQUIRED_FIELDS
exactly in type, and has a plausible email. Anything else raises
ValidationError, which the caller is expected to route to the DLQ.
"""

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
        raise ValidationError(f"missing required field(s): {', '.join(missing)}")

    type_errors = []
    for field, expected_type in REQUIRED_FIELDS.items():
        value = record[field]
        if not isinstance(value, expected_type):
            type_errors.append(
                f"{field} expected {expected_type.__name__}, got {type(value).__name__}"
            )
    if type_errors:
        raise ValidationError("; ".join(type_errors))

    if "@" not in record["email"]:
        raise ValidationError("email missing '@'")

    return record


def extract_after(debezium_envelope: dict) -> dict:
    """
    Debezium change events wrap the actual row under 'after' (insert/update)
    or 'before' (delete). For this POC we only care about inserts/updates.
    Falls back to treating the envelope itself as the record if it isn't
    wrapped (useful for CI, where we produce plain JSON directly).
    """
    if isinstance(debezium_envelope, dict) and "payload" in debezium_envelope:
        payload = debezium_envelope["payload"]
        return payload.get("after") or {}
    if isinstance(debezium_envelope, dict) and "after" in debezium_envelope:
        return debezium_envelope.get("after") or {}
    return debezium_envelope
