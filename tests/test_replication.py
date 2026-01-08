"""
Tests for replication functionality.
"""
import pytest
from unittest.mock import Mock
from src.database.replication import ReplicationManager
from src.communication import message_types


def test_replication_manager_initialization():
    """Test replication manager initialization."""
    socket_client = Mock()
    replication_manager = ReplicationManager(
        node_id=1,
        socket_client=socket_client
    )

    assert replication_manager.node_id == 1
    assert replication_manager.socket_client == socket_client
    assert len(replication_manager.replication_log) == 0


def test_get_replication_stats_empty():
    """Test replication statistics when empty."""
    socket_client = Mock()
    replication_manager = ReplicationManager(
        node_id=1,
        socket_client=socket_client
    )

    stats = replication_manager.get_replication_stats()

    assert stats['total_replications'] == 0
    assert stats['successful'] == 0
    assert stats['failed'] == 0
    assert stats['success_rate'] == 0.0


def test_check_replication_consistency_not_found():
    """Test consistency check for non-existent transaction."""
    socket_client = Mock()
    replication_manager = ReplicationManager(
        node_id=1,
        socket_client=socket_client
    )

    result = replication_manager.check_replication_consistency("TXN-999")

    assert not result['consistent']
    assert 'not found' in result['error']


def test_log_replication():
    """Test replication logging."""
    socket_client = Mock()
    replication_manager = ReplicationManager(
        node_id=1,
        socket_client=socket_client
    )

    replication_manager._log_replication(
        transaction_id="TXN-001",
        query="INSERT INTO users VALUES (1, 'Test')",
        successful_nodes=[2, 3],
        failed_nodes=[]
    )

    assert len(replication_manager.replication_log) == 1
    assert replication_manager.replication_log[0]['transaction_id'] == "TXN-001"
    assert replication_manager.replication_log[0]['successful_nodes'] == [2, 3]
    assert replication_manager.replication_log[0]['failed_nodes'] == []
