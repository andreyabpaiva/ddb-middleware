"""
Tests for communication components.
"""
import pytest
from src.communication.protocol import MessageProtocol
from src.communication import message_types
from src.security.checksum import verify_message_checksum


def test_create_query_message():
    """Test query message creation."""
    message = MessageProtocol.create_query_message(
        sender_id=1,
        query="SELECT * FROM users",
        transaction_id="TXN-001"
    )

    assert message['type'] == message_types.QUERY
    assert message['sender_id'] == 1
    assert message['data']['query'] == "SELECT * FROM users"
    assert message['data']['transaction_id'] == "TXN-001"
    assert 'checksum' in message
    assert verify_message_checksum(message)


def test_create_heartbeat_message():
    """Test heartbeat message creation."""
    message = MessageProtocol.create_heartbeat_message(sender_id=1)

    assert message['type'] == message_types.HEARTBEAT
    assert message['sender_id'] == 1
    assert 'checksum' in message
    assert verify_message_checksum(message)


def test_create_election_message():
    """Test election message creation."""
    message = MessageProtocol.create_election_message(
        sender_id=1,
        receiver_id=2
    )

    assert message['type'] == message_types.ELECTION
    assert message['sender_id'] == 1
    assert message['receiver_id'] == 2
    assert 'checksum' in message


def test_create_transaction_prepare():
    """Test 2PC prepare message creation."""
    message = MessageProtocol.create_transaction_prepare(
        sender_id=1,
        transaction_id="TXN-001",
        query="INSERT INTO users VALUES (1, 'Test')"
    )

    assert message['type'] == message_types.TRANSACTION_PREPARE
    assert message['data']['transaction_id'] == "TXN-001"
    assert 'query' in message['data']


def test_message_encoding_decoding():
    """Test message encoding and decoding."""
    original = MessageProtocol.create_query_message(
        sender_id=1,
        query="SELECT * FROM users"
    )

    encoded = MessageProtocol.encode_message(original)
    decoded = MessageProtocol.decode_message(encoded)

    assert decoded['type'] == original['type']
    assert decoded['sender_id'] == original['sender_id']
    assert verify_message_checksum(decoded)


def test_verify_message():
    """Test message verification."""
    message = MessageProtocol.create_heartbeat_message(sender_id=1)

    assert MessageProtocol.verify_message(message)

    # Tamper with message
    message['data']['tampered'] = True

    assert not MessageProtocol.verify_message(message)
