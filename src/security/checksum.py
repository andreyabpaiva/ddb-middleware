import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Union


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def calculate_checksum(data: Union[str, Dict[str, Any]]) -> str:

    if isinstance(data, dict):
        data = json.dumps(data, sort_keys=True, cls=DateTimeEncoder)

    if isinstance(data, str):
        data = data.encode('utf-8')

    sha256_hash = hashlib.sha256(data)
    return sha256_hash.hexdigest()


def verify_checksum(data: Union[str, Dict[str, Any]], expected_checksum: str) -> bool:

    calculated_checksum = calculate_checksum(data)
    return calculated_checksum == expected_checksum


def add_checksum(message: Dict[str, Any]) -> Dict[str, Any]:

    message_copy = message.copy()

    if 'checksum' in message_copy:
        del message_copy['checksum']

    checksum = calculate_checksum(message_copy)

    message['checksum'] = checksum

    return message


def verify_message_checksum(message: Dict[str, Any]) -> bool:

    if 'checksum' not in message:
        return False

    expected_checksum = message['checksum']

    message_copy = message.copy()
    del message_copy['checksum']

    return verify_checksum(message_copy, expected_checksum)


def generate_data_signature(data: str, salt: str = "") -> str:

    combined = f"{data}{salt}"
    return calculate_checksum(combined)
