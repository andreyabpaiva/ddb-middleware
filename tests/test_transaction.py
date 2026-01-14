import pytest
from src.transaction.lock_manager import LockManager, LockType
from src.transaction.transaction_manager import TransactionManager, TransactionState


def test_lock_manager_initialization():
    lock_manager = LockManager()

    assert len(lock_manager.locks) == 0
    assert len(lock_manager.transaction_locks) == 0


def test_acquire_shared_lock():
    lock_manager = LockManager()

    result = lock_manager.acquire_lock(
        resource="users",
        transaction_id="TXN-001",
        lock_type=LockType.SHARED,
        timeout=1
    )

    assert result
    assert lock_manager.has_lock("users", "TXN-001")


def test_acquire_exclusive_lock():
    lock_manager = LockManager()

    result = lock_manager.acquire_lock(
        resource="users",
        transaction_id="TXN-001",
        lock_type=LockType.EXCLUSIVE,
        timeout=1
    )

    assert result
    assert lock_manager.has_lock("users", "TXN-001")


def test_multiple_shared_locks():
    lock_manager = LockManager()

    result1 = lock_manager.acquire_lock(
        resource="users",
        transaction_id="TXN-001",
        lock_type=LockType.SHARED,
        timeout=1
    )

    result2 = lock_manager.acquire_lock(
        resource="users",
        transaction_id="TXN-002",
        lock_type=LockType.SHARED,
        timeout=1
    )

    assert result1
    assert result2
    assert lock_manager.has_lock("users", "TXN-001")
    assert lock_manager.has_lock("users", "TXN-002")


def test_release_lock():
    lock_manager = LockManager()

    lock_manager.acquire_lock(
        resource="users",
        transaction_id="TXN-001",
        lock_type=LockType.SHARED,
        timeout=1
    )

    result = lock_manager.release_lock("users", "TXN-001")

    assert result
    assert not lock_manager.has_lock("users", "TXN-001")


def test_release_all_locks():
    lock_manager = LockManager()

    lock_manager.acquire_lock("users", "TXN-001", LockType.SHARED, timeout=1)
    lock_manager.acquire_lock("orders", "TXN-001", LockType.SHARED, timeout=1)

    lock_manager.release_all_locks("TXN-001")

    assert not lock_manager.has_lock("users", "TXN-001")
    assert not lock_manager.has_lock("orders", "TXN-001")


def test_transaction_manager_initialization():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    assert tx_manager.node_id == 1
    assert len(tx_manager.active_transactions) == 0


def test_begin_transaction():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    tx_id = tx_manager.begin_transaction()

    assert tx_id in tx_manager.active_transactions
    assert tx_manager.get_transaction_state(tx_id) == TransactionState.ACTIVE.value


def test_commit_transaction():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    tx_id = tx_manager.begin_transaction()
    result = tx_manager.commit_transaction(tx_id)

    assert result
    assert tx_id not in tx_manager.active_transactions


def test_abort_transaction():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    tx_id = tx_manager.begin_transaction()
    result = tx_manager.abort_transaction(tx_id)

    assert result
    assert tx_id not in tx_manager.active_transactions


def test_prepare_transaction():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    tx_id = tx_manager.begin_transaction()
    result = tx_manager.prepare_transaction(tx_id)

    assert result
    assert tx_manager.get_transaction_state(tx_id) == TransactionState.PREPARED.value


def test_get_active_transactions():
    lock_manager = LockManager()
    tx_manager = TransactionManager(node_id=1, lock_manager=lock_manager)

    tx1 = tx_manager.begin_transaction()
    tx2 = tx_manager.begin_transaction()

    active = tx_manager.get_active_transactions()

    assert active['count'] == 2
    assert tx1 in active['transactions']
    assert tx2 in active['transactions']
