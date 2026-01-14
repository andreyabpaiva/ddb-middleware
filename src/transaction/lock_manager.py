import logging
import threading
import time
from typing import Dict, Set, Optional
from enum import Enum


class LockType(Enum):

    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class Lock:

    def __init__(self, lock_type: LockType, transaction_id: str):

        self.lock_type = lock_type
        self.transaction_id = transaction_id
        self.acquired_at = time.time()


class LockManager:

    def __init__(self):

        self.logger = logging.getLogger(__name__)
        self.locks: Dict[str, list[Lock]] = {}  
        self.transaction_locks: Dict[str, Set[str]] = {}  
        self.lock = threading.RLock()  
        self.wait_timeout = 30 

    def acquire_lock(
        self,
        resource: str,
        transaction_id: str,
        lock_type: LockType,
        timeout: float = None
    ) -> bool:

        timeout = timeout or self.wait_timeout
        start_time = time.time()

        while True:
            with self.lock:

                if self._can_acquire(resource, transaction_id, lock_type):

                    new_lock = Lock(lock_type, transaction_id)

                    if resource not in self.locks:
                        self.locks[resource] = []
                    self.locks[resource].append(new_lock)

                    if transaction_id not in self.transaction_locks:
                        self.transaction_locks[transaction_id] = set()
                    self.transaction_locks[transaction_id].add(resource)

                    self.logger.debug(
                        f"Transaction {transaction_id} acquired {lock_type.value} lock on {resource}"
                    )
                    return True

            if time.time() - start_time >= timeout:
                self.logger.warning(
                    f"Transaction {transaction_id} timed out waiting for {lock_type.value} lock on {resource}"
                )
                return False

            time.sleep(0.1)

    def release_lock(self, resource: str, transaction_id: str) -> bool:

        with self.lock:
            if resource not in self.locks:
                return False

            initial_count = len(self.locks[resource])
            self.locks[resource] = [
                lock for lock in self.locks[resource]
                if lock.transaction_id != transaction_id
            ]


            if not self.locks[resource]:
                del self.locks[resource]


            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].discard(resource)
                if not self.transaction_locks[transaction_id]:
                    del self.transaction_locks[transaction_id]

            released = len(self.locks.get(resource, [])) < initial_count

            if released:
                self.logger.debug(f"Transaction {transaction_id} released lock on {resource}")

            return released

    def release_all_locks(self, transaction_id: str):

        with self.lock:
            if transaction_id not in self.transaction_locks:
                return

            resources = list(self.transaction_locks[transaction_id])

            for resource in resources:
                self.release_lock(resource, transaction_id)

            self.logger.debug(f"Released all locks for transaction {transaction_id}")

    def _can_acquire(
        self,
        resource: str,
        transaction_id: str,
        lock_type: LockType
    ) -> bool:

        if resource not in self.locks or not self.locks[resource]:
            return True

        existing_locks = self.locks[resource]

        has_lock = any(lock.transaction_id == transaction_id for lock in existing_locks)

        if lock_type == LockType.SHARED:

            if has_lock:
                return True
            return all(lock.lock_type == LockType.SHARED for lock in existing_locks)

        elif lock_type == LockType.EXCLUSIVE:

            if has_lock:
                return all(lock.transaction_id == transaction_id for lock in existing_locks)
            return False

        return False

    def detect_deadlock(self, transaction_id: str) -> bool:

        with self.lock:
            if transaction_id not in self.transaction_locks:
                return False

            for resource in self.transaction_locks.get(transaction_id, []):
                if resource in self.locks:
                    for lock in self.locks[resource]:
                        if lock.transaction_id == transaction_id:
                            wait_time = time.time() - lock.acquired_at
                            if wait_time > self.wait_timeout:
                                return True

        return False

    def get_lock_info(self, resource: str = None) -> Dict:

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

        with self.lock:
            if resource not in self.locks:
                return False

            return any(
                lock.transaction_id == transaction_id
                for lock in self.locks[resource]
            )
