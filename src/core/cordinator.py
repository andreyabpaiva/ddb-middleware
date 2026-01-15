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

        self.node_id = node_id
        self.socket_client = socket_client
        self.query_executor = query_executor
        self.replication_manager = replication_manager
        self.two_pc = two_pc_coordinator
        self.transaction_manager = transaction_manager
        self.load_balancer = load_balancer
        self.health_checker = health_checker
        self.logger = logging.getLogger(__name__)

        self.is_active = False

    def activate(self):

        self.is_active = True
        self.logger.info(f"Node {self.node_id} activated as coordinator")

    def deactivate(self):

        self.is_active = False
        self.logger.info(f"Node {self.node_id} deactivated as coordinator")

    def handle_query(
        self,
        query: str,
        all_nodes: List[Dict[str, Any]],
        transaction_id: str = None
    ) -> Dict[str, Any]:

        if not self.is_active:
            return {
                'success': False,
                'error': 'Node is not active coordinator'
            }

        self.logger.info(f"Coordinator handling query: {query[:100]}...")

        if not transaction_id:
            transaction_id = generate_transaction_id()

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

        self.logger.info(f"Handling write query with 2PC for transaction {transaction_id}")

        available_node_ids = self.health_checker.get_available_nodes()
        participant_nodes = [n for n in all_nodes if n['id'] in available_node_ids or n['id'] == self.node_id]

        try:

            result = self.two_pc.execute_2pc(
                transaction_id=transaction_id,
                query=query,
                participant_nodes=participant_nodes
            )

            if result['success']:

                local_result = self.query_executor.execute(query, transaction_id)

                return {
                    'success': True,
                    'transaction_id': transaction_id,
                    'node_id': self.node_id, 
                    'coordinator_id': self.node_id,
                    'participants': result.get('participants', 0),
                    'replicated_to': [n['id'] for n in participant_nodes],
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

        self.logger.info(f"Handling read query with load balancing for transaction {transaction_id}")

        try:

            available_node_ids = self.health_checker.get_available_nodes()

            if self.node_id not in available_node_ids:
                available_node_ids.append(self.node_id)

            selected_node_id = self.load_balancer.select_node(available_node_ids)

            if selected_node_id is None:
                return {
                    'success': False,
                    'error': 'No available nodes for query execution',
                    'transaction_id': transaction_id
                }

            import time
            start_time = time.time()
            self.load_balancer.record_query_start(selected_node_id)

            if selected_node_id == self.node_id:

                result = self.query_executor.execute(query, transaction_id)
            else:

                selected_node = next(n for n in all_nodes if n['id'] == selected_node_id)
                result = self._execute_remote_query(query, selected_node, transaction_id)

            response_time = time.time() - start_time
            self.load_balancer.record_query_end(selected_node_id, response_time)

            result['selected_node'] = selected_node_id
            result['node_id'] = selected_node_id 
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

        try:
            from src.communication.protocol import MessageProtocol

            query_msg = MessageProtocol.create_query_message(
                sender_id=self.node_id,
                query=query,
                transaction_id=transaction_id,
                from_coordinator=True
            )

            response = self.socket_client.send_message(
                host=node['ip'],
                port=node['port'],
                message=query_msg,
                wait_for_response=True
            )

            if response:
                data = response.get('data', {})
                if 'result' in data:
                    return data['result']
                return data
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

        self.logger.info(f"Distributing write query to all nodes")

        available_node_ids = self.health_checker.get_available_nodes()
        target_nodes = [n for n in all_nodes if n['id'] in available_node_ids]

        result = self.replication_manager.replicate_query(
            query=query,
            transaction_id=transaction_id,
            target_nodes=target_nodes,
            wait_for_ack=True
        )

        local_result = self.query_executor.execute(query, transaction_id)

        return {
            'success': result['success'] and local_result['success'],
            'transaction_id': transaction_id,
            'replication': result,
            'local': local_result
        }

    def get_coordinator_status(self) -> Dict[str, Any]:

        return {
            'node_id': self.node_id,
            'is_active': self.is_active,
            'load_balancer': self.load_balancer.get_statistics(),
            'cluster_health': self.health_checker.check_cluster_health()
        }
