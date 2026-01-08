"""
Heartbeat monitor for node liveness detection.
Sends and tracks heartbeat messages between nodes.
"""
import logging
import threading
import time
from typing import Dict, Callable, Optional
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types


class HeartbeatMonitor:
    """Monitors node liveness through heartbeat messages."""

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        heartbeat_interval: int = 5,
        heartbeat_timeout: int = 15,
        failure_callback: Optional[Callable] = None
    ):
        """
        Initialize heartbeat monitor.

        Args:
            node_id: ID of this node
            socket_client: Socket client for communication
            heartbeat_interval: Interval between heartbeats (seconds)
            heartbeat_timeout: Timeout to consider node dead (seconds)
            failure_callback: Callback function when node failure detected
        """
        self.node_id = node_id
        self.socket_client = socket_client
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.failure_callback = failure_callback
        self.logger = logging.getLogger(__name__)

        # Track last heartbeat time for each node
        self.last_heartbeat: Dict[int, float] = {}

        # Track node status
        self.node_status: Dict[int, bool] = {}  # node_id -> is_alive

        # Control flags
        self.running = False
        self.sender_thread = None
        self.checker_thread = None

    def start(self, all_nodes: list):
        """
        Start heartbeat monitoring.

        Args:
            all_nodes: List of all node configurations
        """
        if self.running:
            self.logger.warning("Heartbeat monitor already running")
            return

        self.running = True

        # Initialize node status
        for node in all_nodes:
            node_id = node['id']
            if node_id != self.node_id:
                self.node_status[node_id] = True
                self.last_heartbeat[node_id] = time.time()

        # Start sender thread
        self.sender_thread = threading.Thread(
            target=self._send_heartbeats,
            args=(all_nodes,),
            daemon=True
        )
        self.sender_thread.start()

        # Start checker thread
        self.checker_thread = threading.Thread(
            target=self._check_heartbeats,
            daemon=True
        )
        self.checker_thread.start()

        self.logger.info("Heartbeat monitor started")

    def stop(self):
        """Stop heartbeat monitoring."""
        if not self.running:
            return

        self.logger.info("Stopping heartbeat monitor...")
        self.running = False

        # Wait for threads to finish
        if self.sender_thread and self.sender_thread.is_alive():
            self.sender_thread.join(timeout=5)

        if self.checker_thread and self.checker_thread.is_alive():
            self.checker_thread.join(timeout=5)

        self.logger.info("Heartbeat monitor stopped")

    def _send_heartbeats(self, all_nodes: list):
        """
        Periodically send heartbeat messages to all nodes.

        Args:
            all_nodes: List of all node configurations
        """
        while self.running:
            try:
                # Create heartbeat message
                heartbeat_msg = MessageProtocol.create_heartbeat_message(self.node_id)

                # Send to all other nodes
                for node in all_nodes:
                    node_id = node['id']

                    # Skip self
                    if node_id == self.node_id:
                        continue

                    # Only send to alive nodes
                    if not self.node_status.get(node_id, True):
                        continue

                    try:
                        self.socket_client.send_message(
                            host=node['ip'],
                            port=node['port'],
                            message=heartbeat_msg,
                            wait_for_response=False
                        )
                    except Exception as e:
                        self.logger.debug(f"Failed to send heartbeat to node {node_id}: {e}")

            except Exception as e:
                self.logger.error(f"Error in heartbeat sender: {e}")

            # Wait for next interval
            time.sleep(self.heartbeat_interval)

    def _check_heartbeats(self):
        """Check for missed heartbeats and detect failures."""
        while self.running:
            try:
                current_time = time.time()

                for node_id, last_time in list(self.last_heartbeat.items()):
                    # Calculate time since last heartbeat
                    time_since_heartbeat = current_time - last_time

                    # Check if node has timed out
                    if time_since_heartbeat > self.heartbeat_timeout:
                        # Node is dead
                        if self.node_status.get(node_id, True):
                            self.logger.warning(
                                f"Node {node_id} failed (no heartbeat for {time_since_heartbeat:.1f}s)"
                            )
                            self._mark_node_dead(node_id)

            except Exception as e:
                self.logger.error(f"Error in heartbeat checker: {e}")

            # Check every second
            time.sleep(1)

    def record_heartbeat(self, node_id: int):
        """
        Record a heartbeat from a node.

        Args:
            node_id: ID of node that sent heartbeat
        """
        current_time = time.time()
        self.last_heartbeat[node_id] = current_time

        # If node was dead, mark it alive
        if not self.node_status.get(node_id, True):
            self.logger.info(f"Node {node_id} recovered")
            self.node_status[node_id] = True

    def _mark_node_dead(self, node_id: int):
        """
        Mark a node as dead and trigger failure callback.

        Args:
            node_id: ID of failed node
        """
        self.node_status[node_id] = False

        # Trigger failure callback if provided
        if self.failure_callback:
            try:
                self.failure_callback(node_id)
            except Exception as e:
                self.logger.error(f"Error in failure callback: {e}")

    def get_alive_nodes(self) -> list:
        """
        Get list of alive node IDs.

        Returns:
            List of alive node IDs
        """
        return [
            node_id for node_id, is_alive in self.node_status.items()
            if is_alive
        ]

    def get_dead_nodes(self) -> list:
        """
        Get list of dead node IDs.

        Returns:
            List of dead node IDs
        """
        return [
            node_id for node_id, is_alive in self.node_status.items()
            if not is_alive
        ]

    def is_node_alive(self, node_id: int) -> bool:
        """
        Check if a node is alive.

        Args:
            node_id: Node ID to check

        Returns:
            True if alive, False otherwise
        """
        return self.node_status.get(node_id, False)

    def get_status(self) -> Dict:
        """
        Get heartbeat monitor status.

        Returns:
            Status dictionary
        """
        current_time = time.time()

        return {
            'running': self.running,
            'heartbeat_interval': self.heartbeat_interval,
            'heartbeat_timeout': self.heartbeat_timeout,
            'nodes': {
                node_id: {
                    'alive': is_alive,
                    'last_heartbeat': self.last_heartbeat.get(node_id, 0),
                    'time_since_heartbeat': current_time - self.last_heartbeat.get(node_id, current_time)
                }
                for node_id, is_alive in self.node_status.items()
            }
        }
