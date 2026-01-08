"""
Message type constants for distributed database communication.
Defines all message types used in the system.
"""

# Query and replication messages
QUERY = "QUERY"
REPLICATION = "REPLICATION"

# Heartbeat and monitoring messages
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"

# Election messages
ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR_ANNOUNCEMENT = "COORDINATOR_ANNOUNCEMENT"

# Transaction messages (2-Phase Commit)
TRANSACTION_PREPARE = "TRANSACTION_PREPARE"
TRANSACTION_VOTE_YES = "TRANSACTION_VOTE_YES"
TRANSACTION_VOTE_NO = "TRANSACTION_VOTE_NO"
TRANSACTION_COMMIT = "TRANSACTION_COMMIT"
TRANSACTION_ABORT = "TRANSACTION_ABORT"
TRANSACTION_ROLLBACK = "TRANSACTION_ROLLBACK"

# Response messages
QUERY_RESPONSE = "QUERY_RESPONSE"
REPLICATION_ACK = "REPLICATION_ACK"
REPLICATION_NACK = "REPLICATION_NACK"
ERROR = "ERROR"
ACK = "ACK"

# Status messages
NODE_STATUS = "NODE_STATUS"
HEALTH_CHECK = "HEALTH_CHECK"
HEALTH_RESPONSE = "HEALTH_RESPONSE"

# Lock messages
LOCK_REQUEST = "LOCK_REQUEST"
LOCK_GRANTED = "LOCK_GRANTED"
LOCK_DENIED = "LOCK_DENIED"
LOCK_RELEASE = "LOCK_RELEASE"


def is_transaction_message(message_type: str) -> bool:
    """
    Check if message type is transaction-related.

    Args:
        message_type: Message type to check

    Returns:
        True if transaction message, False otherwise
    """
    transaction_types = [
        TRANSACTION_PREPARE,
        TRANSACTION_VOTE_YES,
        TRANSACTION_VOTE_NO,
        TRANSACTION_COMMIT,
        TRANSACTION_ABORT,
        TRANSACTION_ROLLBACK
    ]
    return message_type in transaction_types


def is_election_message(message_type: str) -> bool:
    """
    Check if message type is election-related.

    Args:
        message_type: Message type to check

    Returns:
        True if election message, False otherwise
    """
    election_types = [ELECTION, ELECTION_OK, COORDINATOR_ANNOUNCEMENT]
    return message_type in election_types


def is_heartbeat_message(message_type: str) -> bool:
    """
    Check if message type is heartbeat-related.

    Args:
        message_type: Message type to check

    Returns:
        True if heartbeat message, False otherwise
    """
    return message_type in [HEARTBEAT, HEARTBEAT_ACK]
