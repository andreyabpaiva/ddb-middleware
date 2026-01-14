import logging
from typing import Dict, Any, List, Optional
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types


class ReplicationManager:

    def __init__(self, node_id: int, socket_client: SocketClient):

        self.node_id = node_id
        self.socket_client = socket_client
        self.logger = logging.getLogger(__name__)
        self.replication_log = [] 

    def replicate_query(
        self,
        query: str,
        transaction_id: str,
        target_nodes: List[Dict[str, Any]],
        wait_for_ack: bool = True
    ) -> Dict[str, Any]:

        self.logger.info(f"Replicating query to {len(target_nodes)} nodes for transaction {transaction_id}")

        replication_message = MessageProtocol.create_replication_message(
            sender_id=self.node_id,
            query=query,
            transaction_id=transaction_id
        )

        successful_nodes = []
        failed_nodes = []

        for node in target_nodes:
            node_id = node['id']

            if node_id == self.node_id:
                continue

            try:
                response = self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=replication_message,
                    wait_for_response=wait_for_ack
                )

                if wait_for_ack:
                    if response and response.get('type') == message_types.REPLICATION_ACK:
                        successful_nodes.append(node_id)
                        self.logger.debug(f"Node {node_id} acknowledged replication")
                    else:
                        failed_nodes.append(node_id)
                        self.logger.warning(f"Node {node_id} failed to acknowledge replication")
                else:
                    successful_nodes.append(node_id)

            except Exception as e:
                self.logger.error(f"Replication failed for node {node_id}: {e}")
                failed_nodes.append(node_id)

        self._log_replication(
            transaction_id,
            query,
            successful_nodes,
            failed_nodes
        )

        total_targets = len([n for n in target_nodes if n['id'] != self.node_id])
        success_rate = len(successful_nodes) / total_targets if total_targets > 0 else 1.0

        return {
            'success': len(failed_nodes) == 0,
            'transaction_id': transaction_id,
            'total_nodes': total_targets,
            'successful_nodes': successful_nodes,
            'failed_nodes': failed_nodes,
            'success_rate': success_rate
        }

    def handle_replication_request(
        self,
        query: str,
        transaction_id: str,
        sender_id: int,
        query_executor
    ) -> Dict[str, Any]:

        self.logger.info(f"Received replication request from node {sender_id} for transaction {transaction_id}")

        try:
            result = query_executor.execute(query, transaction_id, log_query=True)

            if result['success']:
                response = MessageProtocol.create_message(
                    message_types.REPLICATION_ACK,
                    self.node_id,
                    {
                        'transaction_id': transaction_id,
                        'status': 'success'
                    }
                )
                self.logger.info(f"Replication successful for transaction {transaction_id}")
            else:
                response = MessageProtocol.create_message(
                    message_types.REPLICATION_NACK,
                    self.node_id,
                    {
                        'transaction_id': transaction_id,
                        'status': 'failed',
                        'error': result.get('error', 'Unknown error')
                    }
                )
                self.logger.error(f"Replication failed for transaction {transaction_id}: {result.get('error')}")

            return response

        except Exception as e:
            self.logger.error(f"Error handling replication request: {e}")
            return MessageProtocol.create_message(
                message_types.REPLICATION_NACK,
                self.node_id,
                {
                    'transaction_id': transaction_id,
                    'status': 'error',
                    'error': str(e)
                }
            )

    def check_replication_consistency(
        self,
        transaction_id: str
    ) -> Dict[str, Any]:

        replication_entry = self._get_replication_entry(transaction_id)

        if not replication_entry:
            return {
                'consistent': False,
                'error': 'Transaction not found in replication log'
            }

        successful = replication_entry['successful_nodes']
        failed = replication_entry['failed_nodes']

        return {
            'consistent': len(failed) == 0,
            'transaction_id': transaction_id,
            'successful_nodes': successful,
            'failed_nodes': failed,
            'needs_repair': len(failed) > 0
        }

    def repair_failed_replication(
        self,
        transaction_id: str,
        query: str,
        failed_node_configs: List[Dict[str, Any]]
    ) -> Dict[str, Any]:

        self.logger.info(f"Attempting to repair replication for transaction {transaction_id}")

        return self.replicate_query(
            query=query,
            transaction_id=transaction_id,
            target_nodes=failed_node_configs,
            wait_for_ack=True
        )

    def _log_replication(
        self,
        transaction_id: str,
        query: str,
        successful_nodes: List[int],
        failed_nodes: List[int]
    ):

        log_entry = {
            'transaction_id': transaction_id,
            'query': query[:100], 
            'successful_nodes': successful_nodes,
            'failed_nodes': failed_nodes,
            'timestamp': self._get_timestamp()
        }

        self.replication_log.append(log_entry)

        if len(self.replication_log) > 1000:
            self.replication_log = self.replication_log[-1000:]

    def _get_replication_entry(self, transaction_id: str) -> Optional[Dict[str, Any]]:

        for entry in reversed(self.replication_log):
            if entry['transaction_id'] == transaction_id:
                return entry
        return None

    def _get_timestamp(self) -> str:

        from src.utils.helpers import get_timestamp
        return get_timestamp()

    def get_replication_stats(self) -> Dict[str, Any]:

        if not self.replication_log:
            return {
                'total_replications': 0,
                'successful': 0,
                'failed': 0,
                'success_rate': 0.0
            }

        total = len(self.replication_log)
        successful = sum(1 for entry in self.replication_log if not entry['failed_nodes'])
        failed = total - successful

        return {
            'total_replications': total,
            'successful': successful,
            'failed': failed,
            'success_rate': successful / total if total > 0 else 0.0
        }
