import json
from datetime import datetime
from typing import Dict, Any, Optional
from src.security.checksum import add_checksum, verify_message_checksum
from src.utils.helpers import generate_message_id, get_timestamp
from src.communication import message_types


class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class MessageProtocol:

    @staticmethod
    def encode_message(message: Dict[str, Any]) -> bytes:

        message_str = json.dumps(message, cls=DateTimeEncoder)
        return message_str.encode('utf-8')

    @staticmethod
    def decode_message(data: bytes) -> Dict[str, Any]:

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

        message = {
            'message_id': generate_message_id(),
            'type': message_type,
            'sender_id': sender_id,
            'timestamp': get_timestamp(),
            'data': data or {}
        }

        if receiver_id is not None:
            message['receiver_id'] = receiver_id

        return add_checksum(message)

    @staticmethod
    def create_query_message(
        sender_id: int,
        query: str,
        transaction_id: Optional[str] = None,
        from_coordinator: bool = False
    ) -> Dict[str, Any]:

        data = {'query': query}
        if transaction_id:
            data['transaction_id'] = transaction_id
        if from_coordinator:
            data['from_coordinator'] = True

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

        return MessageProtocol.create_message(
            message_types.HEARTBEAT,
            sender_id
        )

    @staticmethod
    def create_election_message(sender_id: int, receiver_id: int) -> Dict[str, Any]:

        return MessageProtocol.create_message(
            message_types.ELECTION,
            sender_id,
            receiver_id=receiver_id
        )

    @staticmethod
    def create_coordinator_announcement(sender_id: int) -> Dict[str, Any]:

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

        return verify_message_checksum(message)

    @staticmethod
    def get_message_type(message: Dict[str, Any]) -> Optional[str]:

        return message.get('type')

    @staticmethod
    def get_message_data(message: Dict[str, Any]) -> Dict[str, Any]:

        return message.get('data', {})
