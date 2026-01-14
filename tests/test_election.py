import pytest
from unittest.mock import Mock, MagicMock
from src.core.election import BullyElection
from src.communication import message_types


def test_election_initialization():
    socket_client = Mock()
    election = BullyElection(
        node_id=2,
        socket_client=socket_client
    )

    assert election.node_id == 2
    assert not election.election_in_progress
    assert election.coordinator_id is None


def test_set_coordinator():
    socket_client = Mock()
    election = BullyElection(node_id=2, socket_client=socket_client)

    election.set_coordinator(3)

    assert election.coordinator_id == 3
    assert not election.is_coordinator()


def test_is_coordinator():
    socket_client = Mock()
    election = BullyElection(node_id=2, socket_client=socket_client)

    election.set_coordinator(2)

    assert election.is_coordinator()


def test_handle_coordinator_announcement():
    socket_client = Mock()
    callback = Mock()
    election = BullyElection(
        node_id=1,
        socket_client=socket_client,
        on_coordinator_elected=callback
    )

    election.handle_coordinator_announcement(3)

    assert election.coordinator_id == 3
    callback.assert_called_once_with(3, is_self=False)


def test_handle_election_message():
    socket_client = Mock()
    election = BullyElection(node_id=2, socket_client=socket_client)

    all_nodes = [
        {'id': 1, 'ip': 'node1', 'port': 5001},
        {'id': 2, 'ip': 'node2', 'port': 5002},
        {'id': 3, 'ip': 'node3', 'port': 5003}
    ]

    response = election.handle_election_message(1, all_nodes)

    assert response['type'] == message_types.ELECTION_OK
    assert response['sender_id'] == 2


def test_reset_election():
    socket_client = Mock()
    election = BullyElection(node_id=2, socket_client=socket_client)

    election.set_coordinator(3)
    election.election_in_progress = True

    election.reset_election()

    assert not election.election_in_progress
    assert election.coordinator_id is None
