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

    def __init__(self, transaction_id: str):

        self.transaction_id = transaction_id
        self.state = TransactionState.ACTIVE
        self.queries = []
        self.locks = set()
        self.connection = None


class TransactionManager:

    def __init__(self, node_id: int, lock_manager: LockManager = None):

        self.node_id = node_id
        self.lock_manager = lock_manager or LockManager()
        self.logger = logging.getLogger(__name__)
        self.active_transactions: Dict[str, Transaction] = {}

    def begin_transaction(self, transaction_id: str = None) -> str:

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

        transaction = self._get_transaction(transaction_id)

        if transaction.state != TransactionState.ACTIVE:
            raise ValueError(f"Transaction {transaction_id} is not active")

        transaction.queries.append(query)
        self.logger.debug(f"Added query to transaction {transaction_id}")

    def prepare_transaction(self, transaction_id: str) -> bool:

        transaction = self._get_transaction(transaction_id)

        self.logger.info(f"Preparing transaction {transaction_id}")
        transaction.state = TransactionState.PREPARING

        try:

            transaction.state = TransactionState.PREPARED
            self.logger.info(f"Transaction {transaction_id} prepared successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to prepare transaction {transaction_id}: {e}")
            transaction.state = TransactionState.ACTIVE
            return False

    def commit_transaction(self, transaction_id: str) -> bool:

        transaction = self._get_transaction(transaction_id)

        self.logger.info(f"Committing transaction {transaction_id}")
        transaction.state = TransactionState.COMMITTING

        try:

            transaction.state = TransactionState.COMMITTED

            self.lock_manager.release_all_locks(transaction_id)

            del self.active_transactions[transaction_id]

            self.logger.info(f"Transaction {transaction_id} committed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to commit transaction {transaction_id}: {e}")
            return False

    def abort_transaction(self, transaction_id: str) -> bool:

        if transaction_id not in self.active_transactions:
            self.logger.warning(f"Transaction {transaction_id} not found")
            return False

        transaction = self.active_transactions[transaction_id]

        self.logger.info(f"Aborting transaction {transaction_id}")
        transaction.state = TransactionState.ABORTING

        try:

            transaction.state = TransactionState.ABORTED

            self.lock_manager.release_all_locks(transaction_id)

            del self.active_transactions[transaction_id]

            self.logger.info(f"Transaction {transaction_id} aborted successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to abort transaction {transaction_id}: {e}")
            return False

    def rollback_transaction(self, transaction_id: str) -> bool:

        return self.abort_transaction(transaction_id)

    def acquire_lock(
        self,
        transaction_id: str,
        resource: str,
        lock_type: LockType
    ) -> bool:

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

        if transaction_id not in self.active_transactions:
            return None

        return self.active_transactions[transaction_id].state.value

    def get_active_transactions(self) -> Dict[str, Any]:

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

        import time

        stale_transactions = []

        for txn_id, txn in self.active_transactions.items():

            if txn.state in [TransactionState.COMMITTED, TransactionState.ABORTED]:
                stale_transactions.append(txn_id)

        for txn_id in stale_transactions:
            self.logger.warning(f"Cleaning up stale transaction {txn_id}")
            self.lock_manager.release_all_locks(txn_id)
            del self.active_transactions[txn_id]

    def _get_transaction(self, transaction_id: str) -> Transaction:

        if transaction_id not in self.active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")

        return self.active_transactions[transaction_id]

    def set_transaction_connection(self, transaction_id: str, connection):

        transaction = self._get_transaction(transaction_id)
        transaction.connection = connection

    def get_transaction_connection(self, transaction_id: str):

        transaction = self._get_transaction(transaction_id)
        return transaction.connection
