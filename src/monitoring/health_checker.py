import logging
from typing import Dict, Any, Callable, Optional, List


class HealthChecker:

    def __init__(
        self,
        node_id: int,
        heartbeat_monitor,
        election_callback: Optional[Callable] = None
    ):

        self.node_id = node_id
        self.heartbeat_monitor = heartbeat_monitor
        self.election_callback = election_callback
        self.logger = logging.getLogger(__name__)

        self.coordinator_id: Optional[int] = None

        self.failure_count: Dict[int, int] = {}  
        self.recovery_count: Dict[int, int] = {} 

    def check_node_health(self, node_id: int) -> Dict[str, Any]:

        is_alive = self.heartbeat_monitor.is_node_alive(node_id)

        return {
            'node_id': node_id,
            'alive': is_alive,
            'status': 'healthy' if is_alive else 'failed',
            'failure_count': self.failure_count.get(node_id, 0),
            'recovery_count': self.recovery_count.get(node_id, 0)
        }

    def check_cluster_health(self) -> Dict[str, Any]:

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

        self.logger.warning(f"Handling failure of node {failed_node_id}")

        self.failure_count[failed_node_id] = self.failure_count.get(failed_node_id, 0) + 1

        if failed_node_id == self.coordinator_id:
            self.logger.critical(f"Coordinator (node {failed_node_id}) has failed!")
            self._handle_coordinator_failure()

    def handle_node_recovery(self, recovered_node_id: int):

        self.logger.info(f"Node {recovered_node_id} has recovered")

        self.recovery_count[recovered_node_id] = self.recovery_count.get(recovered_node_id, 0) + 1

    def _handle_coordinator_failure(self):

        self.logger.info("Triggering coordinator election due to failure")

        self.coordinator_id = None

        if self.election_callback:
            try:
                self.election_callback()
            except Exception as e:
                self.logger.error(f"Error triggering election: {e}")

    def set_coordinator(self, coordinator_id: int):

        old_coordinator = self.coordinator_id
        self.coordinator_id = coordinator_id

        if old_coordinator != coordinator_id:
            self.logger.info(f"Coordinator changed from {old_coordinator} to {coordinator_id}")

    def get_coordinator(self) -> Optional[int]:

        return self.coordinator_id

    def is_coordinator_alive(self) -> bool:

        if self.coordinator_id is None:
            return False

        return self.heartbeat_monitor.is_node_alive(self.coordinator_id)

    def is_quorum_available(self, required_percentage: float = 0.5) -> bool:

        cluster_health = self.check_cluster_health()
        health_percentage = cluster_health['health_percentage'] / 100

        return health_percentage >= required_percentage

    def get_health_stats(self) -> Dict[str, Any]:

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

        return self.heartbeat_monitor.get_alive_nodes()

    def check_node_availability(self, node_id: int) -> bool:

        return self.heartbeat_monitor.is_node_alive(node_id)

    def update_node_status(self, node_id: int, is_alive: bool):

        if is_alive:
            self.handle_node_recovery(node_id)
        else:
            self.handle_node_failure(node_id)
