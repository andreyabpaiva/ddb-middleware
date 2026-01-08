"""
Health checker for monitoring node and system health.
Checks node availability and triggers coordinator election on failures.
"""
import logging
from typing import Dict, Any, Callable, Optional, List


class HealthChecker:
    """Monitors health of nodes and triggers appropriate actions."""

    def __init__(
        self,
        node_id: int,
        heartbeat_monitor,
        election_callback: Optional[Callable] = None
    ):
        """
        Initialize health checker.

        Args:
            node_id: ID of this node
            heartbeat_monitor: HeartbeatMonitor instance
            election_callback: Callback to trigger election
        """
        self.node_id = node_id
        self.heartbeat_monitor = heartbeat_monitor
        self.election_callback = election_callback
        self.logger = logging.getLogger(__name__)

        # Track coordinator
        self.coordinator_id: Optional[int] = None

        # Health statistics
        self.failure_count: Dict[int, int] = {}  # node_id -> failure count
        self.recovery_count: Dict[int, int] = {}  # node_id -> recovery count

    def check_node_health(self, node_id: int) -> Dict[str, Any]:
        """
        Check health status of a specific node.

        Args:
            node_id: Node ID to check

        Returns:
            Health status dictionary
        """
        is_alive = self.heartbeat_monitor.is_node_alive(node_id)

        return {
            'node_id': node_id,
            'alive': is_alive,
            'status': 'healthy' if is_alive else 'failed',
            'failure_count': self.failure_count.get(node_id, 0),
            'recovery_count': self.recovery_count.get(node_id, 0)
        }

    def check_cluster_health(self) -> Dict[str, Any]:
        """
        Check overall cluster health.

        Returns:
            Cluster health dictionary
        """
        alive_nodes = self.heartbeat_monitor.get_alive_nodes()
        dead_nodes = self.heartbeat_monitor.get_dead_nodes()

        total_nodes = len(alive_nodes) + len(dead_nodes)
        health_percentage = (len(alive_nodes) / total_nodes * 100) if total_nodes > 0 else 0

        return {
            'total_nodes': total_nodes,
            'alive_nodes': len(alive_nodes),
            'dead_nodes': len(dead_nodes),
            'health_percentage': health_percentage,
            'alive_node_ids': alive_nodes,
            'dead_node_ids': dead_nodes,
            'coordinator_alive': self.is_coordinator_alive()
        }

    def handle_node_failure(self, failed_node_id: int):
        """
        Handle node failure event.

        Args:
            failed_node_id: ID of failed node
        """
        self.logger.warning(f"Handling failure of node {failed_node_id}")

        # Increment failure count
        self.failure_count[failed_node_id] = self.failure_count.get(failed_node_id, 0) + 1

        # Check if failed node is the coordinator
        if failed_node_id == self.coordinator_id:
            self.logger.critical(f"Coordinator (node {failed_node_id}) has failed!")
            self._handle_coordinator_failure()

    def handle_node_recovery(self, recovered_node_id: int):
        """
        Handle node recovery event.

        Args:
            recovered_node_id: ID of recovered node
        """
        self.logger.info(f"Node {recovered_node_id} has recovered")

        # Increment recovery count
        self.recovery_count[recovered_node_id] = self.recovery_count.get(recovered_node_id, 0) + 1

    def _handle_coordinator_failure(self):
        """Handle coordinator failure by triggering election."""
        self.logger.info("Triggering coordinator election due to failure")

        # Clear coordinator
        self.coordinator_id = None

        # Trigger election if callback provided
        if self.election_callback:
            try:
                self.election_callback()
            except Exception as e:
                self.logger.error(f"Error triggering election: {e}")

    def set_coordinator(self, coordinator_id: int):
        """
        Set the current coordinator.

        Args:
            coordinator_id: ID of coordinator node
        """
        old_coordinator = self.coordinator_id
        self.coordinator_id = coordinator_id

        if old_coordinator != coordinator_id:
            self.logger.info(f"Coordinator changed from {old_coordinator} to {coordinator_id}")

    def get_coordinator(self) -> Optional[int]:
        """
        Get current coordinator ID.

        Returns:
            Coordinator ID or None
        """
        return self.coordinator_id

    def is_coordinator_alive(self) -> bool:
        """
        Check if coordinator is alive.

        Returns:
            True if coordinator is alive, False otherwise
        """
        if self.coordinator_id is None:
            return False

        return self.heartbeat_monitor.is_node_alive(self.coordinator_id)

    def is_quorum_available(self, required_percentage: float = 0.5) -> bool:
        """
        Check if quorum is available (majority of nodes alive).

        Args:
            required_percentage: Required percentage of alive nodes (default 50%)

        Returns:
            True if quorum available, False otherwise
        """
        cluster_health = self.check_cluster_health()
        health_percentage = cluster_health['health_percentage'] / 100

        return health_percentage >= required_percentage

    def get_health_stats(self) -> Dict[str, Any]:
        """
        Get health statistics.

        Returns:
            Statistics dictionary
        """
        cluster_health = self.check_cluster_health()

        return {
            'cluster': cluster_health,
            'coordinator_id': self.coordinator_id,
            'coordinator_alive': self.is_coordinator_alive(),
            'quorum_available': self.is_quorum_available(),
            'failure_stats': {
                'total_failures': sum(self.failure_count.values()),
                'total_recoveries': sum(self.recovery_count.values()),
                'by_node': {
                    node_id: {
                        'failures': self.failure_count.get(node_id, 0),
                        'recoveries': self.recovery_count.get(node_id, 0)
                    }
                    for node_id in set(list(self.failure_count.keys()) + list(self.recovery_count.keys()))
                }
            }
        }

    def get_available_nodes(self) -> List[int]:
        """
        Get list of available (alive) node IDs.

        Returns:
            List of available node IDs
        """
        return self.heartbeat_monitor.get_alive_nodes()

    def check_node_availability(self, node_id: int) -> bool:
        """
        Check if a specific node is available.

        Args:
            node_id: Node ID to check

        Returns:
            True if available, False otherwise
        """
        return self.heartbeat_monitor.is_node_alive(node_id)

    def update_node_status(self, node_id: int, is_alive: bool):
        """
        Update node status manually.

        Args:
            node_id: Node ID
            is_alive: Whether node is alive
        """
        if is_alive:
            self.handle_node_recovery(node_id)
        else:
            self.handle_node_failure(node_id)
