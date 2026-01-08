"""
Main Node class that integrates all distributed database components.
Handles message routing, state management, and component coordination.
"""
import logging
from typing import Dict, Any, Optional
from src.communication.socket_server import SocketServer
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types
from src.database.mysql_connector import MySQLConnector
from src.database.query_executor import QueryExecutor
from src.database.replication import ReplicationManager
from src.transaction.lock_manager import LockManager
from src.transaction.transaction_manager import TransactionManager
from src.transaction.two_phase_commit import TwoPhaseCommitCoordinator, TwoPhaseCommitParticipant
from src.monitoring.heartbeat import HeartbeatMonitor
from src.monitoring.health_checker import HealthChecker
from src.monitoring.load_balancer import LoadBalancer
from src.core.election import BullyElection
from src.core.cordinator import Coordinator
from src.utils.config import Config
from src.utils.logger import setup_logger


class Node:
    """Main node class for distributed database middleware."""

    def __init__(self, node_id: int, config: Config):
        """
        Initialize node with all components.

        Args:
            node_id: Unique node identifier
            config: Configuration instance
        """
        self.node_id = node_id
        self.config = config
        self.logger = setup_logger(node_id)

        self.logger.info(f"Initializing node {node_id}")

        # Load configurations
        self.node_config = config.get_node_config(node_id)
        self.all_nodes = config.get_all_nodes()
        self.db_config = config.load_database_config()
        self.heartbeat_config = config.get_heartbeat_config()

        if not self.node_config:
            raise ValueError(f"Configuration not found for node {node_id}")

        # Initialize components
        self._init_communication()
        self._init_database()
        self._init_transactions()
        self._init_monitoring()
        self._init_core()

        self.logger.info(f"Node {node_id} initialized successfully")

    def _init_communication(self):
        """Initialize communication components."""
        self.logger.info("Initializing communication components...")

        # Socket client for outgoing connections
        self.socket_client = SocketClient(timeout=5)

        # Socket server for incoming connections
        self.socket_server = SocketServer(
            host="0.0.0.0",  # Bind to all interfaces
            port=self.node_config['port'],
            message_handler=self._handle_message
        )

    def _init_database(self):
        """Initialize database components."""
        self.logger.info("Initializing database components...")

        # MySQL connector
        self.db_connector = MySQLConnector(
            host=self.node_config['mysql_host'],
            port=self.node_config['mysql_port'],
            database=self.node_config['mysql_database'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            pool_name=f"node_{self.node_id}_pool",
            pool_size=5
        )

        # Query executor
        self.query_executor = QueryExecutor(
            db_connector=self.db_connector,
            node_id=self.node_id
        )

        # Replication manager
        self.replication_manager = ReplicationManager(
            node_id=self.node_id,
            socket_client=self.socket_client
        )

    def _init_transactions(self):
        """Initialize transaction management components."""
        self.logger.info("Initializing transaction components...")

        # Lock manager
        self.lock_manager = LockManager()

        # Transaction manager
        self.transaction_manager = TransactionManager(
            node_id=self.node_id,
            lock_manager=self.lock_manager
        )

        # 2PC Coordinator
        self.two_pc_coordinator = TwoPhaseCommitCoordinator(
            node_id=self.node_id,
            socket_client=self.socket_client,
            transaction_manager=self.transaction_manager
        )

        # 2PC Participant
        self.two_pc_participant = TwoPhaseCommitParticipant(
            node_id=self.node_id,
            transaction_manager=self.transaction_manager,
            query_executor=self.query_executor
        )

    def _init_monitoring(self):
        """Initialize monitoring components."""
        self.logger.info("Initializing monitoring components...")

        # Load balancer
        self.load_balancer = LoadBalancer(
            node_id=self.node_id,
            strategy="round_robin"
        )

        # Health checker (will set heartbeat monitor later)
        self.health_checker = HealthChecker(
            node_id=self.node_id,
            heartbeat_monitor=None,  # Set after creating heartbeat monitor
            election_callback=self._trigger_election
        )

        # Heartbeat monitor
        self.heartbeat_monitor = HeartbeatMonitor(
            node_id=self.node_id,
            socket_client=self.socket_client,
            heartbeat_interval=self.heartbeat_config['heartbeat_interval'],
            heartbeat_timeout=self.heartbeat_config['heartbeat_timeout'],
            failure_callback=self.health_checker.handle_node_failure
        )

        # Update health checker with heartbeat monitor
        self.health_checker.heartbeat_monitor = self.heartbeat_monitor

    def _init_core(self):
        """Initialize core components."""
        self.logger.info("Initializing core components...")

        # Election
        self.election = BullyElection(
            node_id=self.node_id,
            socket_client=self.socket_client,
            on_coordinator_elected=self._on_coordinator_elected
        )

        # Coordinator
        self.coordinator = Coordinator(
            node_id=self.node_id,
            socket_client=self.socket_client,
            query_executor=self.query_executor,
            replication_manager=self.replication_manager,
            two_pc_coordinator=self.two_pc_coordinator,
            transaction_manager=self.transaction_manager,
            load_balancer=self.load_balancer,
            health_checker=self.health_checker
        )

        # Initially, highest ID is coordinator
        initial_coordinator = max(node['id'] for node in self.all_nodes)
        self.election.set_coordinator(initial_coordinator)
        self.health_checker.set_coordinator(initial_coordinator)

        if self.node_id == initial_coordinator:
            self.coordinator.activate()

    def start(self):
        """Start the node and all its components."""
        self.logger.info(f"Starting node {self.node_id}...")

        # Test database connection
        if not self.db_connector.test_connection():
            raise RuntimeError("Failed to connect to database")

        # Start socket server
        self.socket_server.start()

        # Start heartbeat monitor
        self.heartbeat_monitor.start(self.all_nodes)

        self.logger.info(f"Node {self.node_id} started successfully")
        self.logger.info(f"Listening on port {self.node_config['port']}")

    def stop(self):
        """Stop the node and cleanup resources."""
        self.logger.info(f"Stopping node {self.node_id}...")

        # Stop heartbeat monitor
        self.heartbeat_monitor.stop()

        # Stop socket server
        self.socket_server.stop()

        # Close database connections
        self.db_connector.close_pool()

        # Deactivate coordinator if active
        if self.coordinator.is_active:
            self.coordinator.deactivate()

        self.logger.info(f"Node {self.node_id} stopped")

    def _handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle incoming message and route to appropriate handler.

        Args:
            message: Received message

        Returns:
            Response message or None
        """
        try:
            message_type = message.get('type')
            sender_id = message.get('sender_id')

            self.logger.debug(f"Handling {message_type} message from node {sender_id}")

            # Route based on message type
            if message_type == message_types.HEARTBEAT:
                return self._handle_heartbeat(message)
            elif message_type == message_types.QUERY:
                return self._handle_query(message)
            elif message_type == message_types.REPLICATION:
                return self._handle_replication(message)
            elif message_type == message_types.ELECTION:
                return self._handle_election(message)
            elif message_type == message_types.COORDINATOR_ANNOUNCEMENT:
                return self._handle_coordinator_announcement(message)
            elif message_type == message_types.TRANSACTION_PREPARE:
                return self._handle_transaction_prepare(message)
            elif message_type == message_types.TRANSACTION_COMMIT:
                return self._handle_transaction_commit(message)
            elif message_type == message_types.TRANSACTION_ABORT:
                return self._handle_transaction_abort(message)
            else:
                self.logger.warning(f"Unknown message type: {message_type}")
                return None

        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            return MessageProtocol.create_response(
                sender_id=self.node_id,
                success=False,
                error=str(e)
            )

    def _handle_heartbeat(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle heartbeat message."""
        sender_id = message.get('sender_id')
        self.heartbeat_monitor.record_heartbeat(sender_id)

        # Don't send response - client doesn't wait for it
        # This prevents "Broken pipe" errors
        return None

    def _handle_query(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle query message from client or coordinator."""
        data = MessageProtocol.get_message_data(message)
        query = data.get('query')
        transaction_id = data.get('transaction_id')

        # Check if message is from coordinator (has coordinator flag)
        from_coordinator = data.get('from_coordinator', False)

        # If not from coordinator and this is a write query, forward to coordinator
        from src.utils.helpers import is_write_query
        if not from_coordinator and is_write_query(query):
            # This is a client query that needs coordination
            result = self.execute_query(query)
        else:
            # This is either from coordinator or a read query - execute locally
            result = self.query_executor.execute(query, transaction_id)

        return MessageProtocol.create_response(
            sender_id=self.node_id,
            success=result['success'],
            data=result
        )

    def _handle_replication(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle replication message."""
        data = MessageProtocol.get_message_data(message)
        query = data.get('query')
        transaction_id = data.get('transaction_id')
        sender_id = message.get('sender_id')

        return self.replication_manager.handle_replication_request(
            query=query,
            transaction_id=transaction_id,
            sender_id=sender_id,
            query_executor=self.query_executor
        )

    def _handle_election(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle election message."""
        sender_id = message.get('sender_id')
        return self.election.handle_election_message(sender_id, self.all_nodes)

    def _handle_coordinator_announcement(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle coordinator announcement."""
        coordinator_id = message.get('sender_id')
        self.election.handle_coordinator_announcement(coordinator_id)
        return None

    def _handle_transaction_prepare(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle 2PC prepare message."""
        data = MessageProtocol.get_message_data(message)
        transaction_id = data.get('transaction_id')
        query = data.get('query')

        return self.two_pc_participant.handle_prepare(transaction_id, query)

    def _handle_transaction_commit(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle 2PC commit message."""
        data = MessageProtocol.get_message_data(message)
        transaction_id = data.get('transaction_id')

        return self.two_pc_participant.handle_commit(transaction_id)

    def _handle_transaction_abort(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle 2PC abort message."""
        data = MessageProtocol.get_message_data(message)
        transaction_id = data.get('transaction_id')

        self.two_pc_participant.handle_abort(transaction_id)
        return None

    def _trigger_election(self):
        """Trigger coordinator election."""
        self.logger.info("Triggering coordinator election")
        self.election.start_election(self.all_nodes)

    def _on_coordinator_elected(self, coordinator_id: int, is_self: bool):
        """
        Callback when coordinator is elected.

        Args:
            coordinator_id: ID of elected coordinator
            is_self: Whether this node is the coordinator
        """
        self.logger.info(f"Coordinator elected: node {coordinator_id}")

        # Update health checker
        self.health_checker.set_coordinator(coordinator_id)

        # Activate/deactivate coordinator role
        if is_self:
            self.coordinator.activate()
        else:
            self.coordinator.deactivate()

    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a query (for external clients).

        Args:
            query: SQL query string

        Returns:
            Query result
        """
        if self.coordinator.is_active:
            # This node is coordinator - handle directly
            return self.coordinator.handle_query(query, self.all_nodes)
        else:
            # Forward to coordinator
            coordinator_id = self.election.get_coordinator()

            if coordinator_id is None:
                return {
                    'success': False,
                    'error': 'No coordinator available'
                }

            coordinator_node = next(
                (n for n in self.all_nodes if n['id'] == coordinator_id),
                None
            )

            if not coordinator_node:
                return {
                    'success': False,
                    'error': 'Coordinator node not found'
                }

            # Forward query to coordinator
            from src.utils.helpers import generate_transaction_id

            query_msg = MessageProtocol.create_query_message(
                sender_id=self.node_id,
                query=query,
                transaction_id=generate_transaction_id()
            )

            try:
                response = self.socket_client.send_message(
                    host=coordinator_node['ip'],
                    port=coordinator_node['port'],
                    message=query_msg,
                    wait_for_response=True
                )

                return response.get('data', {}) if response else {
                    'success': False,
                    'error': 'No response from coordinator'
                }

            except Exception as e:
                return {
                    'success': False,
                    'error': f'Failed to contact coordinator: {e}'
                }

    def get_status(self) -> Dict[str, Any]:
        """
        Get node status information.

        Returns:
            Status dictionary
        """
        return {
            'node_id': self.node_id,
            'is_coordinator': self.election.is_coordinator(),
            'coordinator_id': self.election.get_coordinator(),
            'heartbeat': self.heartbeat_monitor.get_status(),
            'health': self.health_checker.get_health_stats(),
            'load_balancer': self.load_balancer.get_statistics(),
            'transactions': self.transaction_manager.get_active_transactions()
        }
