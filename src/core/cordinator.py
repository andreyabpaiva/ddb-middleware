"""
Coordinator implementation for distributed transaction coordination.
Manages query distribution, replication, and 2PC coordination.
"""
import logging
from typing import Dict, Any, List, Optional
from src.communication.socket_client import SocketClient
from src.database.query_executor import QueryExecutor
from src.database.replication import ReplicationManager
from src.transaction.two_phase_commit import TwoPhaseCommitCoordinator
from src.transaction.transaction_manager import TransactionManager
from src.monitoring.load_balancer import LoadBalancer
from src.utils.helpers import is_write_query, is_read_query, generate_transaction_id


class Coordinator:
    """Coordinator for managing distributed database operations."""

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        query_executor: QueryExecutor,
        replication_manager: ReplicationManager,
        two_pc_coordinator: TwoPhaseCommitCoordinator,
        transaction_manager: TransactionManager,
        load_balancer: LoadBalancer,
        health_checker
    ):
        """
        Initialize coordinator.

        Args:
            node_id: ID of coordinator node
            socket_client: Socket client for communication
            query_executor: Query executor instance
            replication_manager: Replication manager instance
            two_pc_coordinator: 2PC coordinator instance
            transaction_manager: Transaction manager instance
            load_balancer: Load balancer instance
            health_checker: Health checker instance
        """
        self.node_id = node_id
        self.socket_client = socket_client
        self.query_executor = query_executor
        self.replication_manager = replication_manager
        self.two_pc = two_pc_coordinator
        self.transaction_manager = transaction_manager
        self.load_balancer = load_balancer
        self.health_checker = health_checker
        self.logger = logging.getLogger(__name__)

        # Coordinator state
        self.is_active = False

    def activate(self):
        """Activate coordinator role."""
        self.is_active = True
        self.logger.info(f"Node {self.node_id} activated as coordinator")

    def deactivate(self):
        """Deactivate coordinator role."""
        self.is_active = False
        self.logger.info(f"Node {self.node_id} deactivated as coordinator")

    def handle_query(
        self,
        query: str,
        all_nodes: List[Dict[str, Any]],
        transaction_id: str = None
    ) -> Dict[str, Any]:
        """
        Handle incoming query and coordinate execution.

        Args:
            query: SQL query string
            all_nodes: List of all node configurations
            transaction_id: Transaction ID (optional)

        Returns:
            Query result dictionary
        """
        if not self.is_active:
            return {
                'success': False,
                'error': 'Node is not active coordinator'
            }

        self.logger.info(f"Coordinator handling query: {query[:100]}...")

        # Generate transaction ID if not provided
        if not transaction_id:
            transaction_id = generate_transaction_id()

        # Determine query type and route appropriately
        if is_write_query(query):
            return self._handle_write_query(query, all_nodes, transaction_id)
        elif is_read_query(query):
            return self._handle_read_query(query, all_nodes, transaction_id)
        else:
            return {
                'success': False,
                'error': 'Unknown query type',
                'transaction_id': transaction_id
            }

    def _handle_write_query(
        self,
        query: str,
        all_nodes: List[Dict[str, Any]],
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Handle write query using 2-Phase Commit.

        Args:
            query: Write query
            all_nodes: List of all node configurations
            transaction_id: Transaction ID

        Returns:
            Execution result
        """
        self.logger.info(f"Handling write query with 2PC for transaction {transaction_id}")

        # Get available nodes
        available_node_ids = self.health_checker.get_available_nodes()
        participant_nodes = [n for n in all_nodes if n['id'] in available_node_ids or n['id'] == self.node_id]

        try:
            # Execute 2-Phase Commit
            result = self.two_pc.execute_2pc(
                transaction_id=transaction_id,
                query=query,
                participant_nodes=participant_nodes
            )

            if result['success']:
                # Execute locally on coordinator
                local_result = self.query_executor.execute(query, transaction_id)

                return {
                    'success': True,
                    'transaction_id': transaction_id,
                    'coordinator_id': self.node_id,
                    'participants': result.get('participants', 0),
                    'data': local_result.get('data'),
                    'affected_rows': local_result.get('affected_rows', 0)
                }
            else:
                return {
                    'success': False,
                    'transaction_id': transaction_id,
                    'error': result.get('error', 'Transaction failed'),
                    'phase': result.get('phase')
                }

        except Exception as e:
            self.logger.error(f"Error handling write query: {e}")
            return {
                'success': False,
                'transaction_id': transaction_id,
                'error': str(e)
            }

    def _handle_read_query(
        self,
        query: str,
        all_nodes: List[Dict[str, Any]],
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Handle read query using load balancing.

        Args:
            query: Read query
            all_nodes: List of all node configurations
            transaction_id: Transaction ID

        Returns:
            Query result
        """
        self.logger.info(f"Handling read query with load balancing for transaction {transaction_id}")

        try:
            # Get available nodes
            available_node_ids = self.health_checker.get_available_nodes()

            # Add self to available nodes
            if self.node_id not in available_node_ids:
                available_node_ids.append(self.node_id)

            # Select node using load balancer
            selected_node_id = self.load_balancer.select_node(available_node_ids)

            if selected_node_id is None:
                return {
                    'success': False,
                    'error': 'No available nodes for query execution',
                    'transaction_id': transaction_id
                }

            # Record query start
            import time
            start_time = time.time()
            self.load_balancer.record_query_start(selected_node_id)

            # Execute on selected node
            if selected_node_id == self.node_id:
                # Execute locally
                result = self.query_executor.execute(query, transaction_id)
            else:
                # Execute remotely
                selected_node = next(n for n in all_nodes if n['id'] == selected_node_id)
                result = self._execute_remote_query(query, selected_node, transaction_id)

            # Record query end
            response_time = time.time() - start_time
            self.load_balancer.record_query_end(selected_node_id, response_time)

            # Add metadata
            result['selected_node'] = selected_node_id
            result['coordinator_id'] = self.node_id
            result['response_time'] = response_time

            return result

        except Exception as e:
            self.logger.error(f"Error handling read query: {e}")
            return {
                'success': False,
                'transaction_id': transaction_id,
                'error': str(e)
            }

    def _execute_remote_query(
        self,
        query: str,
        node: Dict[str, Any],
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Execute query on a remote node.

        Args:
            query: SQL query
            node: Node configuration
            transaction_id: Transaction ID

        Returns:
            Query result
        """
        try:
            from src.communication.protocol import MessageProtocol

            query_msg = MessageProtocol.create_query_message(
                sender_id=self.node_id,
                query=query,
                transaction_id=transaction_id
            )

            response = self.socket_client.send_message(
                host=node['ip'],
                port=node['port'],
                message=query_msg,
                wait_for_response=True
            )

            if response:
                return response.get('data', {})
            else:
                return {
                    'success': False,
                    'error': 'No response from node'
                }

        except Exception as e:
            self.logger.error(f"Error executing remote query on node {node['id']}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def distribute_write(
        self,
        query: str,
        all_nodes: List[Dict[str, Any]],
        transaction_id: str
    ) -> Dict[str, Any]:
        """
        Distribute write query to all nodes (alternative to 2PC).

        Args:
            query: Write query
            all_nodes: List of all node configurations
            transaction_id: Transaction ID

        Returns:
            Distribution result
        """
        self.logger.info(f"Distributing write query to all nodes")

        # Get available nodes
        available_node_ids = self.health_checker.get_available_nodes()
        target_nodes = [n for n in all_nodes if n['id'] in available_node_ids]

        # Replicate to all nodes
        result = self.replication_manager.replicate_query(
            query=query,
            transaction_id=transaction_id,
            target_nodes=target_nodes,
            wait_for_ack=True
        )

        # Execute locally
        local_result = self.query_executor.execute(query, transaction_id)

        return {
            'success': result['success'] and local_result['success'],
            'transaction_id': transaction_id,
            'replication': result,
            'local': local_result
        }

    def get_coordinator_status(self) -> Dict[str, Any]:
        """
        Get coordinator status information.

        Returns:
            Status dictionary
        """
        return {
            'node_id': self.node_id,
            'is_active': self.is_active,
            'load_balancer': self.load_balancer.get_statistics(),
            'cluster_health': self.health_checker.check_cluster_health()
        }
