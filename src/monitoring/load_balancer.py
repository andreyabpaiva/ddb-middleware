"""
Load balancer for distributing read queries across nodes.
Implements round-robin and least-loaded node selection strategies.
"""
import logging
import threading
import time
from typing import Dict, List, Optional, Any
from collections import defaultdict


class LoadBalancer:
    """Load balancer for distributing queries across database nodes."""

    def __init__(self, node_id: int, strategy: str = "round_robin"):
        """
        Initialize load balancer.

        Args:
            node_id: ID of this node
            strategy: Load balancing strategy ('round_robin' or 'least_loaded')
        """
        self.node_id = node_id
        self.strategy = strategy
        self.logger = logging.getLogger(__name__)

        # Round-robin state
        self.current_index = 0

        # Load tracking
        self.query_counts: Dict[int, int] = defaultdict(int)
        self.response_times: Dict[int, List[float]] = defaultdict(list)
        self.active_queries: Dict[int, int] = defaultdict(int)

        # Thread safety
        self.lock = threading.Lock()

    def select_node(
        self,
        available_nodes: List[int],
        exclude_nodes: Optional[List[int]] = None
    ) -> Optional[int]:
        """
        Select a node for query execution.

        Args:
            available_nodes: List of available node IDs
            exclude_nodes: List of nodes to exclude (optional)

        Returns:
            Selected node ID or None if no nodes available
        """
        if not available_nodes:
            self.logger.warning("No available nodes for load balancing")
            return None

        # Filter out excluded nodes
        if exclude_nodes:
            available_nodes = [n for n in available_nodes if n not in exclude_nodes]

        if not available_nodes:
            self.logger.warning("All nodes are excluded")
            return None

        # Select based on strategy
        if self.strategy == "round_robin":
            return self._select_round_robin(available_nodes)
        elif self.strategy == "least_loaded":
            return self._select_least_loaded(available_nodes)
        else:
            self.logger.warning(f"Unknown strategy {self.strategy}, using round_robin")
            return self._select_round_robin(available_nodes)

    def _select_round_robin(self, available_nodes: List[int]) -> int:
        """
        Select node using round-robin strategy.

        Args:
            available_nodes: List of available node IDs

        Returns:
            Selected node ID
        """
        with self.lock:
            # Sort for consistent ordering
            sorted_nodes = sorted(available_nodes)

            # Get current node
            node = sorted_nodes[self.current_index % len(sorted_nodes)]

            # Increment index
            self.current_index += 1

            self.logger.debug(f"Round-robin selected node {node}")
            return node

    def _select_least_loaded(self, available_nodes: List[int]) -> int:
        """
        Select node with least load.

        Args:
            available_nodes: List of available node IDs

        Returns:
            Selected node ID
        """
        with self.lock:
            # Calculate load for each node
            node_loads = {}

            for node_id in available_nodes:
                # Load is combination of active queries and average response time
                active = self.active_queries.get(node_id, 0)
                avg_response = self._get_average_response_time(node_id)

                # Weighted load score (lower is better)
                load_score = active * 10 + avg_response

                node_loads[node_id] = load_score

            # Select node with minimum load
            selected_node = min(node_loads, key=node_loads.get)

            self.logger.debug(
                f"Least-loaded selected node {selected_node} "
                f"(load: {node_loads[selected_node]:.2f})"
            )
            return selected_node

    def record_query_start(self, node_id: int):
        """
        Record start of query execution on a node.

        Args:
            node_id: Node executing the query
        """
        with self.lock:
            self.active_queries[node_id] += 1
            self.query_counts[node_id] += 1

    def record_query_end(self, node_id: int, response_time: float):
        """
        Record completion of query execution.

        Args:
            node_id: Node that executed the query
            response_time: Query response time in seconds
        """
        with self.lock:
            # Decrement active queries
            if self.active_queries[node_id] > 0:
                self.active_queries[node_id] -= 1

            # Record response time (keep last 100 samples)
            self.response_times[node_id].append(response_time)
            if len(self.response_times[node_id]) > 100:
                self.response_times[node_id] = self.response_times[node_id][-100:]

    def _get_average_response_time(self, node_id: int) -> float:
        """
        Get average response time for a node.

        Args:
            node_id: Node ID

        Returns:
            Average response time in seconds
        """
        response_times = self.response_times.get(node_id, [])

        if not response_times:
            return 0.0

        return sum(response_times) / len(response_times)

    def get_node_load(self, node_id: int) -> Dict[str, Any]:
        """
        Get load information for a specific node.

        Args:
            node_id: Node ID

        Returns:
            Load information dictionary
        """
        with self.lock:
            return {
                'node_id': node_id,
                'active_queries': self.active_queries.get(node_id, 0),
                'total_queries': self.query_counts.get(node_id, 0),
                'average_response_time': self._get_average_response_time(node_id),
                'recent_response_times': self.response_times.get(node_id, [])[-10:]
            }

    def get_cluster_load(self, available_nodes: List[int]) -> Dict[str, Any]:
        """
        Get load information for entire cluster.

        Args:
            available_nodes: List of available node IDs

        Returns:
            Cluster load information
        """
        with self.lock:
            total_active = sum(self.active_queries.get(n, 0) for n in available_nodes)
            total_queries = sum(self.query_counts.get(n, 0) for n in available_nodes)

            node_loads = {
                node_id: self.get_node_load(node_id)
                for node_id in available_nodes
            }

            return {
                'total_active_queries': total_active,
                'total_queries_processed': total_queries,
                'node_count': len(available_nodes),
                'nodes': node_loads,
                'strategy': self.strategy
            }

    def reset_statistics(self):
        """Reset all load statistics."""
        with self.lock:
            self.query_counts.clear()
            self.response_times.clear()
            self.active_queries.clear()
            self.current_index = 0
            self.logger.info("Load balancer statistics reset")

    def set_strategy(self, strategy: str):
        """
        Change load balancing strategy.

        Args:
            strategy: New strategy ('round_robin' or 'least_loaded')
        """
        if strategy not in ['round_robin', 'least_loaded']:
            self.logger.error(f"Invalid strategy: {strategy}")
            return

        self.strategy = strategy
        self.logger.info(f"Load balancing strategy changed to {strategy}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall load balancer statistics.

        Returns:
            Statistics dictionary
        """
        with self.lock:
            total_queries = sum(self.query_counts.values())
            total_active = sum(self.active_queries.values())

            return {
                'strategy': self.strategy,
                'total_queries_routed': total_queries,
                'total_active_queries': total_active,
                'nodes_tracked': len(self.query_counts),
                'current_round_robin_index': self.current_index
            }

    def distribute_queries(
        self,
        query_count: int,
        available_nodes: List[int]
    ) -> Dict[int, int]:
        """
        Calculate query distribution across nodes.

        Args:
            query_count: Number of queries to distribute
            available_nodes: List of available node IDs

        Returns:
            Dictionary mapping node_id to query count
        """
        if not available_nodes:
            return {}

        distribution = defaultdict(int)

        for _ in range(query_count):
            node = self.select_node(available_nodes)
            if node:
                distribution[node] += 1

        return dict(distribution)
