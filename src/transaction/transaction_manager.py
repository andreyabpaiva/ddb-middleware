"""
Transaction manager for ensuring ACID properties.
Manages transaction lifecycle and coordinates with lock manager.
"""
import logging
from typing import Dict, Any, Optional
from enum import Enum
from src.transaction.lock_manager import LockManager, LockType
from src.utils.helpers import generate_transaction_id


class TransactionState(Enum):
    """Transaction state enumeration."""
    ACTIVE = "ACTIVE"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"


class Transaction:
    """Represents a database transaction."""

    def __init__(self, transaction_id: str):
        """
        Initialize transaction.

        Args:
            transaction_id: Unique transaction identifier
        """
        self.transaction_id = transaction_id
        self.state = TransactionState.ACTIVE
        self.queries = []
        self.locks = set()
        self.connection = None


class TransactionManager:
    """Manages database transactions with ACID guarantees."""

    def __init__(self, node_id: int, lock_manager: LockManager = None):
        """
        Initialize transaction manager.

        Args:
            node_id: ID of this node
            lock_manager: Lock manager instance (creates new if None)
        """
        self.node_id = node_id
        self.lock_manager = lock_manager or LockManager()
        self.logger = logging.getLogger(__name__)
        self.active_transactions: Dict[str, Transaction] = {}

    def begin_transaction(self, transaction_id: str = None) -> str:
        """
        Begin a new transaction.

        Args:
            transaction_id: Transaction ID (generated if not provided)

        Returns:
            Transaction ID
        """
        if not transaction_id:
            transaction_id = generate_transaction_id()

        if transaction_id in self.active_transactions:
            self.logger.warning(f"Transaction {transaction_id} already exists")
            return transaction_id

        transaction = Transaction(transaction_id)
        self.active_transactions[transaction_id] = transaction

        self.logger.info(f"Started transaction {transaction_id}")
        return transaction_id

    def add_query(self, transaction_id: str, query: str):
        """
        Add a query to a transaction.

        Args:
            transaction_id: Transaction ID
            query: SQL query string

        Raises:
            ValueError: If transaction not found or not active
        """
        transaction = self._get_transaction(transaction_id)

        if transaction.state != TransactionState.ACTIVE:
            raise ValueError(f"Transaction {transaction_id} is not active")

        transaction.queries.append(query)
        self.logger.debug(f"Added query to transaction {transaction_id}")

    def prepare_transaction(self, transaction_id: str) -> bool:
        """
        Prepare transaction for commit (2PC prepare phase).

        Args:
            transaction_id: Transaction ID

        Returns:
            True if prepared successfully, False otherwise
        """
        transaction = self._get_transaction(transaction_id)

        self.logger.info(f"Preparing transaction {transaction_id}")
        transaction.state = TransactionState.PREPARING

        try:
            # Validate that all necessary locks are acquired
            # In a real implementation, we would validate resources here

            transaction.state = TransactionState.PREPARED
            self.logger.info(f"Transaction {transaction_id} prepared successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to prepare transaction {transaction_id}: {e}")
            transaction.state = TransactionState.ACTIVE
            return False

    def commit_transaction(self, transaction_id: str) -> bool:
        """
        Commit a transaction.

        Args:
            transaction_id: Transaction ID

        Returns:
            True if committed successfully, False otherwise
        """
        transaction = self._get_transaction(transaction_id)

        self.logger.info(f"Committing transaction {transaction_id}")
        transaction.state = TransactionState.COMMITTING

        try:
            # In actual implementation, commit would be handled by query executor
            # Here we just update state

            transaction.state = TransactionState.COMMITTED

            # Release all locks
            self.lock_manager.release_all_locks(transaction_id)

            # Remove from active transactions
            del self.active_transactions[transaction_id]

            self.logger.info(f"Transaction {transaction_id} committed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to commit transaction {transaction_id}: {e}")
            return False

    def abort_transaction(self, transaction_id: str) -> bool:
        """
        Abort a transaction and rollback changes.

        Args:
            transaction_id: Transaction ID

        Returns:
            True if aborted successfully, False otherwise
        """
        if transaction_id not in self.active_transactions:
            self.logger.warning(f"Transaction {transaction_id} not found")
            return False

        transaction = self.active_transactions[transaction_id]

        self.logger.info(f"Aborting transaction {transaction_id}")
        transaction.state = TransactionState.ABORTING

        try:
            # Rollback would be handled by query executor
            # Here we just update state and release locks

            transaction.state = TransactionState.ABORTED

            # Release all locks
            self.lock_manager.release_all_locks(transaction_id)

            # Remove from active transactions
            del self.active_transactions[transaction_id]

            self.logger.info(f"Transaction {transaction_id} aborted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to abort transaction {transaction_id}: {e}")
            return False

    def rollback_transaction(self, transaction_id: str) -> bool:
        """
        Rollback a transaction (alias for abort).

        Args:
            transaction_id: Transaction ID

        Returns:
            True if rolled back successfully, False otherwise
        """
        return self.abort_transaction(transaction_id)

    def acquire_lock(
        self,
        transaction_id: str,
        resource: str,
        lock_type: LockType
    ) -> bool:
        """
        Acquire a lock for a transaction.

        Args:
            transaction_id: Transaction ID
            resource: Resource to lock
            lock_type: Type of lock

        Returns:
            True if lock acquired, False otherwise
        """
        transaction = self._get_transaction(transaction_id)

        success = self.lock_manager.acquire_lock(
            resource=resource,
            transaction_id=transaction_id,
            lock_type=lock_type
        )

        if success:
            transaction.locks.add(resource)

        return success

    def get_transaction_state(self, transaction_id: str) -> Optional[str]:
        """
        Get current state of a transaction.

        Args:
            transaction_id: Transaction ID

        Returns:
            Transaction state or None if not found
        """
        if transaction_id not in self.active_transactions:
            return None

        return self.active_transactions[transaction_id].state.value

    def get_active_transactions(self) -> Dict[str, Any]:
        """
        Get information about all active transactions.

        Returns:
            Dictionary with active transaction info
        """
        return {
            'count': len(self.active_transactions),
            'transactions': {
                txn_id: {
                    'state': txn.state.value,
                    'query_count': len(txn.queries),
                    'locks': list(txn.locks)
                }
                for txn_id, txn in self.active_transactions.items()
            }
        }

    def cleanup_stale_transactions(self, max_age_seconds: int = 3600):
        """
        Clean up stale transactions that have been active too long.

        Args:
            max_age_seconds: Maximum age in seconds before cleanup
        """
        import time

        stale_transactions = []

        for txn_id, txn in self.active_transactions.items():
            # In a real implementation, we would track transaction start time
            # For now, we'll just check if they're in a terminal state
            if txn.state in [TransactionState.COMMITTED, TransactionState.ABORTED]:
                stale_transactions.append(txn_id)

        for txn_id in stale_transactions:
            self.logger.warning(f"Cleaning up stale transaction {txn_id}")
            self.lock_manager.release_all_locks(txn_id)
            del self.active_transactions[txn_id]

    def _get_transaction(self, transaction_id: str) -> Transaction:
        """
        Get transaction by ID.

        Args:
            transaction_id: Transaction ID

        Returns:
            Transaction object

        Raises:
            ValueError: If transaction not found
        """
        if transaction_id not in self.active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")

        return self.active_transactions[transaction_id]

    def set_transaction_connection(self, transaction_id: str, connection):
        """
        Set database connection for a transaction.

        Args:
            transaction_id: Transaction ID
            connection: Database connection
        """
        transaction = self._get_transaction(transaction_id)
        transaction.connection = connection

    def get_transaction_connection(self, transaction_id: str):
        """
        Get database connection for a transaction.

        Args:
            transaction_id: Transaction ID

        Returns:
            Database connection or None
        """
        transaction = self._get_transaction(transaction_id)
        return transaction.connection
