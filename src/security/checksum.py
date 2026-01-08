"""
Checksum utilities for ensuring data integrity in message transmission.
Uses SHA256 hashing algorithm.
"""
import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Union


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def calculate_checksum(data: Union[str, Dict[str, Any]]) -> str:
    """
    Calculate SHA256 checksum for data.

    Args:
        data: Data to calculate checksum for (string or dictionary)

    Returns:
        Hexadecimal checksum string
    """
    # Convert dictionary to JSON string if needed
    if isinstance(data, dict):
        data = json.dumps(data, sort_keys=True, cls=DateTimeEncoder)

    # Ensure data is bytes
    if isinstance(data, str):
        data = data.encode('utf-8')

    # Calculate SHA256 hash
    sha256_hash = hashlib.sha256(data)
    return sha256_hash.hexdigest()


def verify_checksum(data: Union[str, Dict[str, Any]], expected_checksum: str) -> bool:
    """
    Verify data integrity by comparing checksums.

    Args:
        data: Data to verify
        expected_checksum: Expected checksum value

    Returns:
        True if checksums match, False otherwise
    """
    calculated_checksum = calculate_checksum(data)
    return calculated_checksum == expected_checksum


def add_checksum(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add checksum to a message dictionary.

    Args:
        message: Message dictionary

    Returns:
        Message with checksum field added
    """
    # Create a copy to avoid modifying original
    message_copy = message.copy()

    # Remove existing checksum if present
    if 'checksum' in message_copy:
        del message_copy['checksum']

    # Calculate checksum on message without checksum field
    checksum = calculate_checksum(message_copy)

    # Add checksum to original message
    message['checksum'] = checksum

    return message


def verify_message_checksum(message: Dict[str, Any]) -> bool:
    """
    Verify message integrity by checking its checksum.

    Args:
        message: Message dictionary with checksum field

    Returns:
        True if message is valid, False otherwise
    """
    if 'checksum' not in message:
        return False

    expected_checksum = message['checksum']

    # Create copy without checksum for verification
    message_copy = message.copy()
    del message_copy['checksum']

    return verify_checksum(message_copy, expected_checksum)


def generate_data_signature(data: str, salt: str = "") -> str:
    """
    Generate a signature for data with optional salt.

    Args:
        data: Data to sign
        salt: Optional salt for additional security

    Returns:
        Signature hash
    """
    combined = f"{data}{salt}"
    return calculate_checksum(combined)
