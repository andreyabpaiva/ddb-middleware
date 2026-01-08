"""
Lock manager for transaction concurrency control.
Implements table-level locking with shared and exclusive locks.
"""
import logging
import threading
import time
from typing import Dict, Set, Optional
from enum import Enum


class LockType(Enum):
    """Lock type enumeration."""
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class Lock:
    """Represents a lock on a resource."""

    def __init__(self, lock_type: LockType, transaction_id: str):
        """
        Initialize lock.

        Args:
            lock_type: Type of lock (SHARED or EXCLUSIVE)
            transaction_id: ID of transaction holding the lock
        """
        self.lock_type = lock_type
        self.transaction_id = transaction_id
        self.acquired_at = time.time()


class LockManager:
    """Manages locks for concurrent transaction control."""

    def __init__(self):
        """Initialize lock manager."""
        self.logger = logging.getLogger(__name__)
        self.locks: Dict[str, list[Lock]] = {}  # resource -> list of locks
        self.transaction_locks: Dict[str, Set[str]] = {}  # transaction_id -> set of resources
        self.lock = threading.RLock()  # Thread-safe lock operations
        self.wait_timeout = 30  # seconds

    def acquire_lock(
        self,
        resource: str,
        transaction_id: str,
        lock_type: LockType,
        timeout: float = None
    ) -> bool:
        """
        Acquire a lock on a resource.

        Args:
            resource: Resource identifier (e.g., table name)
            transaction_id: Transaction requesting the lock
            lock_type: Type of lock to acquire
            timeout: Maximum time to wait for lock (seconds)

        Returns:
            True if lock acquired, False otherwise
        """
        timeout = timeout or self.wait_timeout
        start_time = time.time()

        while True:
            with self.lock:
                # Check if we can acquire the lock
                if self._can_acquire(resource, transaction_id, lock_type):
                    # Acquire the lock
                    new_lock = Lock(lock_type, transaction_id)

                    if resource not in self.locks:
                        self.locks[resource] = []
                    self.locks[resource].append(new_lock)

                    # Track transaction's locks
                    if transaction_id not in self.transaction_locks:
                        self.transaction_locks[transaction_id] = set()
                    self.transaction_locks[transaction_id].add(resource)

                    self.logger.debug(
                        f"Transaction {transaction_id} acquired {lock_type.value} lock on {resource}"
                    )
                    return True

            # Check timeout
            if time.time() - start_time >= timeout:
                self.logger.warning(
                    f"Transaction {transaction_id} timed out waiting for {lock_type.value} lock on {resource}"
                )
                return False

            # Wait before retry
            time.sleep(0.1)

    def release_lock(self, resource: str, transaction_id: str) -> bool:
        """
        Release a lock on a resource.

        Args:
            resource: Resource identifier
            transaction_id: Transaction releasing the lock

        Returns:
            True if lock released, False if lock not found
        """
        with self.lock:
            if resource not in self.locks:
                return False

            # Remove locks held by this transaction on this resource
            initial_count = len(self.locks[resource])
            self.locks[resource] = [
                lock for lock in self.locks[resource]
                if lock.transaction_id != transaction_id
            ]

            # Clean up empty lock lists
            if not self.locks[resource]:
                del self.locks[resource]

            # Update transaction's lock tracking
            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].discard(resource)
                if not self.transaction_locks[transaction_id]:
                    del self.transaction_locks[transaction_id]

            released = len(self.locks.get(resource, [])) < initial_count

            if released:
                self.logger.debug(f"Transaction {transaction_id} released lock on {resource}")

            return released

    def release_all_locks(self, transaction_id: str):
        """
        Release all locks held by a transaction.

        Args:
            transaction_id: Transaction to release locks for
        """
        with self.lock:
            if transaction_id not in self.transaction_locks:
                return

            # Get copy of resources to release
            resources = list(self.transaction_locks[transaction_id])

            # Release each lock
            for resource in resources:
                self.release_lock(resource, transaction_id)

            self.logger.debug(f"Released all locks for transaction {transaction_id}")

    def _can_acquire(
        self,
        resource: str,
        transaction_id: str,
        lock_type: LockType
    ) -> bool:
        """
        Check if a lock can be acquired.

        Args:
            resource: Resource to check
            transaction_id: Transaction requesting lock
            lock_type: Type of lock requested

        Returns:
            True if lock can be acquired, False otherwise
        """
        # No existing locks - can acquire
        if resource not in self.locks or not self.locks[resource]:
            return True

        existing_locks = self.locks[resource]

        # Check if transaction already holds a lock
        has_lock = any(lock.transaction_id == transaction_id for lock in existing_locks)

        if lock_type == LockType.SHARED:
            # Can acquire shared lock if:
            # 1. All existing locks are shared, OR
            # 2. Transaction already holds the lock
            if has_lock:
                return True
            return all(lock.lock_type == LockType.SHARED for lock in existing_locks)

        elif lock_type == LockType.EXCLUSIVE:
            # Can acquire exclusive lock only if:
            # 1. Transaction already holds all locks on the resource, OR
            # 2. No other locks exist
            if has_lock:
                return all(lock.transaction_id == transaction_id for lock in existing_locks)
            return False

        return False

    def detect_deadlock(self, transaction_id: str) -> bool:
        """
        Simple deadlock detection.

        Args:
            transaction_id: Transaction to check

        Returns:
            True if deadlock detected, False otherwise
        """
        # Simple timeout-based deadlock detection
        # More sophisticated algorithms (wait-for graph) could be implemented
        with self.lock:
            if transaction_id not in self.transaction_locks:
                return False

            # Check if transaction has been waiting too long
            for resource in self.transaction_locks.get(transaction_id, []):
                if resource in self.locks:
                    for lock in self.locks[resource]:
                        if lock.transaction_id == transaction_id:
                            wait_time = time.time() - lock.acquired_at
                            if wait_time > self.wait_timeout:
                                return True

        return False

    def get_lock_info(self, resource: str = None) -> Dict:
        """
        Get information about current locks.

        Args:
            resource: Specific resource to check (optional)

        Returns:
            Dictionary with lock information
        """
        with self.lock:
            if resource:
                locks = self.locks.get(resource, [])
                return {
                    'resource': resource,
                    'locks': [
                        {
                            'type': lock.lock_type.value,
                            'transaction_id': lock.transaction_id,
                            'duration': time.time() - lock.acquired_at
                        }
                        for lock in locks
                    ]
                }
            else:
                return {
                    'total_resources': len(self.locks),
                    'resources': {
                        res: [
                            {
                                'type': lock.lock_type.value,
                                'transaction_id': lock.transaction_id,
                                'duration': time.time() - lock.acquired_at
                            }
                            for lock in locks
                        ]
                        for res, locks in self.locks.items()
                    }
                }

    def has_lock(self, resource: str, transaction_id: str) -> bool:
        """
        Check if transaction holds a lock on resource.

        Args:
            resource: Resource to check
            transaction_id: Transaction to check

        Returns:
            True if transaction holds lock, False otherwise
        """
        with self.lock:
            if resource not in self.locks:
                return False

            return any(
                lock.transaction_id == transaction_id
                for lock in self.locks[resource]
            )
