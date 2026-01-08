"""
Two-Phase Commit (2PC) protocol implementation.
Ensures atomic commit across distributed nodes.
"""
import logging
from typing import Dict, Any, List, Optional
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types
from src.transaction.transaction_manager import TransactionManager


class TwoPhaseCommitCoordinator:
    """Coordinator for 2-Phase Commit protocol."""

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        transaction_manager: TransactionManager
    ):
        """
        Initialize 2PC coordinator.

        Args:
            node_id: ID of coordinator node
            socket_client: Socket client for communication
            transaction_manager: Transaction manager instance
        """
        self.node_id = node_id
        self.socket_client = socket_client
        self.transaction_manager = transaction_manager
        self.logger = logging.getLogger(__name__)
        self.timeout = 30  # seconds

    def execute_2pc(
        self,
        transaction_id: str,
        query: str,
        participant_nodes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Execute 2-Phase Commit protocol.

        Args:
            transaction_id: Transaction ID
            query: SQL query to execute
            participant_nodes: List of participant node configurations

        Returns:
            Result dictionary with success status and details
        """
        self.logger.info(f"Starting 2PC for transaction {transaction_id} with {len(participant_nodes)} participants")

        # Phase 1: Prepare
        prepare_result = self._phase1_prepare(transaction_id, query, participant_nodes)

        if not prepare_result['success']:
            # Prepare failed - abort transaction
            self.logger.warning(f"Prepare phase failed for transaction {transaction_id}, aborting")
            self._phase2_abort(transaction_id, participant_nodes)
            return {
                'success': False,
                'phase': 'prepare',
                'transaction_id': transaction_id,
                'error': prepare_result['error']
            }

        # Phase 2: Commit
        commit_result = self._phase2_commit(transaction_id, participant_nodes)

        if not commit_result['success']:
            # Commit failed - this is problematic in 2PC
            self.logger.error(f"Commit phase failed for transaction {transaction_id}")
            return {
                'success': False,
                'phase': 'commit',
                'transaction_id': transaction_id,
                'error': commit_result['error']
            }

        self.logger.info(f"2PC completed successfully for transaction {transaction_id}")
        return {
            'success': True,
            'transaction_id': transaction_id,
            'participants': len(participant_nodes)
        }

    def _phase1_prepare(
        self,
        transaction_id: str,
        query: str,
        participant_nodes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Phase 1: Send PREPARE messages and collect votes.

        Args:
            transaction_id: Transaction ID
            query: SQL query to prepare
            participant_nodes: List of participant nodes

        Returns:
            Result dictionary with votes
        """
        self.logger.info(f"Phase 1 (PREPARE) for transaction {transaction_id}")

        # Create PREPARE message
        prepare_message = MessageProtocol.create_transaction_prepare(
            sender_id=self.node_id,
            transaction_id=transaction_id,
            query=query
        )

        # Send to all participants
        votes_yes = []
        votes_no = []

        for node in participant_nodes:
            node_id = node['id']

            # Skip self
            if node_id == self.node_id:
                votes_yes.append(node_id)
                continue

            try:
                response = self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=prepare_message,
                    wait_for_response=True
                )

                if not response:
                    self.logger.warning(f"No response from node {node_id}")
                    votes_no.append(node_id)
                    continue

                response_type = response.get('type')

                if response_type == message_types.TRANSACTION_VOTE_YES:
                    votes_yes.append(node_id)
                    self.logger.debug(f"Node {node_id} voted YES")
                elif response_type == message_types.TRANSACTION_VOTE_NO:
                    votes_no.append(node_id)
                    self.logger.debug(f"Node {node_id} voted NO")
                else:
                    votes_no.append(node_id)
                    self.logger.warning(f"Invalid response from node {node_id}")

            except Exception as e:
                self.logger.error(f"Error communicating with node {node_id}: {e}")
                votes_no.append(node_id)

        # Check if all voted YES
        all_yes = len(votes_no) == 0

        return {
            'success': all_yes,
            'votes_yes': votes_yes,
            'votes_no': votes_no,
            'error': None if all_yes else f"Nodes {votes_no} voted NO or failed to respond"
        }

    def _phase2_commit(
        self,
        transaction_id: str,
        participant_nodes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Phase 2: Send COMMIT messages to all participants.

        Args:
            transaction_id: Transaction ID
            participant_nodes: List of participant nodes

        Returns:
            Result dictionary
        """
        self.logger.info(f"Phase 2 (COMMIT) for transaction {transaction_id}")

        # Create COMMIT message
        commit_message = MessageProtocol.create_transaction_commit(
            sender_id=self.node_id,
            transaction_id=transaction_id
        )

        # Send to all participants
        committed_nodes = []
        failed_nodes = []

        for node in participant_nodes:
            node_id = node['id']

            # Skip self (coordinator commits locally)
            if node_id == self.node_id:
                committed_nodes.append(node_id)
                continue

            try:
                response = self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=commit_message,
                    wait_for_response=True
                )

                if response and response.get('type') == message_types.ACK:
                    committed_nodes.append(node_id)
                    self.logger.debug(f"Node {node_id} committed")
                else:
                    failed_nodes.append(node_id)
                    self.logger.warning(f"Node {node_id} failed to commit")

            except Exception as e:
                self.logger.error(f"Error sending commit to node {node_id}: {e}")
                failed_nodes.append(node_id)

        success = len(failed_nodes) == 0

        return {
            'success': success,
            'committed_nodes': committed_nodes,
            'failed_nodes': failed_nodes,
            'error': None if success else f"Nodes {failed_nodes} failed to commit"
        }

    def _phase2_abort(
        self,
        transaction_id: str,
        participant_nodes: List[Dict[str, Any]]
    ):
        """
        Phase 2: Send ABORT messages to all participants.

        Args:
            transaction_id: Transaction ID
            participant_nodes: List of participant nodes
        """
        self.logger.info(f"Phase 2 (ABORT) for transaction {transaction_id}")

        # Create ABORT message
        abort_message = MessageProtocol.create_transaction_abort(
            sender_id=self.node_id,
            transaction_id=transaction_id
        )

        # Send to all participants
        for node in participant_nodes:
            node_id = node['id']

            if node_id == self.node_id:
                continue

            try:
                self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=abort_message,
                    wait_for_response=False
                )
                self.logger.debug(f"Sent ABORT to node {node_id}")

            except Exception as e:
                self.logger.error(f"Error sending abort to node {node_id}: {e}")


class TwoPhaseCommitParticipant:
    """Participant for 2-Phase Commit protocol."""

    def __init__(
        self,
        node_id: int,
        transaction_manager: TransactionManager,
        query_executor
    ):
        """
        Initialize 2PC participant.

        Args:
            node_id: ID of participant node
            transaction_manager: Transaction manager instance
            query_executor: Query executor instance
        """
        self.node_id = node_id
        self.transaction_manager = transaction_manager
        self.query_executor = query_executor
        self.logger = logging.getLogger(__name__)
        self.prepared_transactions: Dict[str, str] = {}  # transaction_id -> query

    def handle_prepare(
        self,
        transaction_id: str,
        query: str
    ) -> Dict[str, Any]:
        """
        Handle PREPARE message from coordinator.

        Args:
            transaction_id: Transaction ID
            query: SQL query to prepare

        Returns:
            Vote message (YES or NO)
        """
        self.logger.info(f"Handling PREPARE for transaction {transaction_id}")

        try:
            # Begin transaction
            self.transaction_manager.begin_transaction(transaction_id)

            # Prepare the query (validate, acquire locks, etc.)
            can_commit, error = self.query_executor.prepare_query(query, transaction_id)

            if can_commit:
                # Store prepared query
                self.prepared_transactions[transaction_id] = query

                # Vote YES
                self.logger.info(f"Voting YES for transaction {transaction_id}")
                return MessageProtocol.create_transaction_vote(
                    sender_id=self.node_id,
                    transaction_id=transaction_id,
                    vote=True
                )
            else:
                # Vote NO
                self.logger.warning(f"Voting NO for transaction {transaction_id}: {error}")
                self.transaction_manager.abort_transaction(transaction_id)
                return MessageProtocol.create_transaction_vote(
                    sender_id=self.node_id,
                    transaction_id=transaction_id,
                    vote=False
                )

        except Exception as e:
            self.logger.error(f"Error handling PREPARE: {e}")
            self.transaction_manager.abort_transaction(transaction_id)
            return MessageProtocol.create_transaction_vote(
                sender_id=self.node_id,
                transaction_id=transaction_id,
                vote=False
            )

    def handle_commit(self, transaction_id: str) -> Dict[str, Any]:
        """
        Handle COMMIT message from coordinator.

        Args:
            transaction_id: Transaction ID

        Returns:
            ACK message
        """
        self.logger.info(f"Handling COMMIT for transaction {transaction_id}")

        try:
            # Get prepared query
            if transaction_id not in self.prepared_transactions:
                raise ValueError(f"Transaction {transaction_id} not prepared")

            query = self.prepared_transactions[transaction_id]

            # Execute the query
            result = self.query_executor.commit_prepared_query(query, transaction_id)

            # Commit transaction
            self.transaction_manager.commit_transaction(transaction_id)

            # Clean up
            del self.prepared_transactions[transaction_id]

            self.logger.info(f"Committed transaction {transaction_id}")

            return MessageProtocol.create_message(
                message_types.ACK,
                self.node_id,
                {'transaction_id': transaction_id, 'status': 'committed'}
            )

        except Exception as e:
            self.logger.error(f"Error handling COMMIT: {e}")
            return MessageProtocol.create_message(
                message_types.ERROR,
                self.node_id,
                {'transaction_id': transaction_id, 'error': str(e)}
            )

    def handle_abort(self, transaction_id: str):
        """
        Handle ABORT message from coordinator.

        Args:
            transaction_id: Transaction ID
        """
        self.logger.info(f"Handling ABORT for transaction {transaction_id}")

        try:
            # Get prepared query if exists
            query = self.prepared_transactions.get(transaction_id)

            if query:
                self.query_executor.abort_prepared_query(query, transaction_id)
                del self.prepared_transactions[transaction_id]

            # Abort transaction
            self.transaction_manager.abort_transaction(transaction_id)

            self.logger.info(f"Aborted transaction {transaction_id}")

        except Exception as e:
            self.logger.error(f"Error handling ABORT: {e}")
