"""
Protocol implementation for message encoding/decoding.
Handles message serialization, checksums, and message creation.
"""
import json
from datetime import datetime
from typing import Dict, Any, Optional
from src.security.checksum import add_checksum, verify_message_checksum
from src.utils.helpers import generate_message_id, get_timestamp
from src.communication import message_types


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class MessageProtocol:
    """Protocol handler for encoding/decoding messages."""

    @staticmethod
    def encode_message(message: Dict[str, Any]) -> bytes:
        """
        Encode message to JSON bytes.

        Args:
            message: Message dictionary

        Returns:
            JSON encoded bytes
        """
        message_str = json.dumps(message, cls=DateTimeEncoder)
        return message_str.encode('utf-8')

    @staticmethod
    def decode_message(data: bytes) -> Dict[str, Any]:
        """
        Decode message from JSON bytes.

        Args:
            data: JSON encoded bytes

        Returns:
            Message dictionary

        Raises:
            ValueError: If decoding fails
        """
        try:
            message_str = data.decode('utf-8')
            return json.loads(message_str)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to decode message: {e}")

    @staticmethod
    def create_message(
        message_type: str,
        sender_id: int,
        data: Optional[Dict[str, Any]] = None,
        receiver_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a message with standard format.

        Args:
            message_type: Type of message
            sender_id: ID of sending node
            data: Message payload (optional)
            receiver_id: ID of receiving node (optional)

        Returns:
            Message dictionary with checksum
        """
        message = {
            'message_id': generate_message_id(),
            'type': message_type,
            'sender_id': sender_id,
            'timestamp': get_timestamp(),
            'data': data or {}
        }

        if receiver_id is not None:
            message['receiver_id'] = receiver_id

        # Add checksum
        return add_checksum(message)

    @staticmethod
    def create_query_message(
        sender_id: int,
        query: str,
        transaction_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a query message.

        Args:
            sender_id: ID of sending node
            query: SQL query string
            transaction_id: Transaction ID (optional)

        Returns:
            Query message
        """
        data = {'query': query}
        if transaction_id:
            data['transaction_id'] = transaction_id

        return MessageProtocol.create_message(
            message_types.QUERY,
            sender_id,
            data
        )

    @staticmethod
    def create_replication_message(
        sender_id: int,
        query: str,
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Create a replication message.

        Args:
            sender_id: ID of sending node
            query: SQL query to replicate
            transaction_id: Transaction ID

        Returns:
            Replication message
        """
        return MessageProtocol.create_message(
            message_types.REPLICATION,
            sender_id,
            {
                'query': query,
                'transaction_id': transaction_id
            }
        )

    @staticmethod
    def create_heartbeat_message(sender_id: int) -> Dict[str, Any]:
        """
        Create a heartbeat message.

        Args:
            sender_id: ID of sending node

        Returns:
            Heartbeat message
        """
        return MessageProtocol.create_message(
            message_types.HEARTBEAT,
            sender_id
        )

    @staticmethod
    def create_election_message(sender_id: int, receiver_id: int) -> Dict[str, Any]:
        """
        Create an election message.

        Args:
            sender_id: ID of sending node
            receiver_id: ID of receiving node

        Returns:
            Election message
        """
        return MessageProtocol.create_message(
            message_types.ELECTION,
            sender_id,
            receiver_id=receiver_id
        )

    @staticmethod
    def create_coordinator_announcement(sender_id: int) -> Dict[str, Any]:
        """
        Create a coordinator announcement message.

        Args:
            sender_id: ID of new coordinator

        Returns:
            Coordinator announcement message
        """
        return MessageProtocol.create_message(
            message_types.COORDINATOR_ANNOUNCEMENT,
            sender_id
        )

    @staticmethod
    def create_transaction_prepare(
        sender_id: int,
        transaction_id: str,
        query: str
    ) -> Dict[str, Any]:
        """
        Create a transaction prepare message (2PC phase 1).

        Args:
            sender_id: ID of coordinator
            transaction_id: Transaction ID
            query: SQL query to prepare

        Returns:
            Transaction prepare message
        """
        return MessageProtocol.create_message(
            message_types.TRANSACTION_PREPARE,
            sender_id,
            {
                'transaction_id': transaction_id,
                'query': query
            }
        )

    @staticmethod
    def create_transaction_vote(
        sender_id: int,
        transaction_id: str,
        vote: bool
    ) -> Dict[str, Any]:
        """
        Create a transaction vote message (2PC phase 1 response).

        Args:
            sender_id: ID of voting node
            transaction_id: Transaction ID
            vote: True for YES, False for NO

        Returns:
            Transaction vote message
        """
        message_type = message_types.TRANSACTION_VOTE_YES if vote else message_types.TRANSACTION_VOTE_NO

        return MessageProtocol.create_message(
            message_type,
            sender_id,
            {'transaction_id': transaction_id}
        )

    @staticmethod
    def create_transaction_commit(
        sender_id: int,
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Create a transaction commit message (2PC phase 2).

        Args:
            sender_id: ID of coordinator
            transaction_id: Transaction ID

        Returns:
            Transaction commit message
        """
        return MessageProtocol.create_message(
            message_types.TRANSACTION_COMMIT,
            sender_id,
            {'transaction_id': transaction_id}
        )

    @staticmethod
    def create_transaction_abort(
        sender_id: int,
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Create a transaction abort message (2PC phase 2).

        Args:
            sender_id: ID of coordinator
            transaction_id: Transaction ID

        Returns:
            Transaction abort message
        """
        return MessageProtocol.create_message(
            message_types.TRANSACTION_ABORT,
            sender_id,
            {'transaction_id': transaction_id}
        )

    @staticmethod
    def create_response(
        sender_id: int,
        success: bool,
        data: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a response message.

        Args:
            sender_id: ID of responding node
            success: Whether operation was successful
            data: Response data (optional)
            error: Error message (optional)

        Returns:
            Response message
        """
        response_data = {
            'success': success
        }

        if data:
            response_data['result'] = data

        if error:
            response_data['error'] = error

        return MessageProtocol.create_message(
            message_types.QUERY_RESPONSE,
            sender_id,
            response_data
        )

    @staticmethod
    def verify_message(message: Dict[str, Any]) -> bool:
        """
        Verify message integrity using checksum.

        Args:
            message: Message to verify

        Returns:
            True if valid, False otherwise
        """
        return verify_message_checksum(message)

    @staticmethod
    def get_message_type(message: Dict[str, Any]) -> Optional[str]:
        """
        Extract message type from message.

        Args:
            message: Message dictionary

        Returns:
            Message type or None if not found
        """
        return message.get('type')

    @staticmethod
    def get_message_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract data payload from message.

        Args:
            message: Message dictionary

        Returns:
            Data payload
        """
        return message.get('data', {})
