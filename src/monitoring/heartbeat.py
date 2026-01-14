import logging
import threading
import time
from typing import Dict, Callable, Optional
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types


class HeartbeatMonitor:

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        heartbeat_interval: int = 5,
        heartbeat_timeout: int = 15,
        failure_callback: Optional[Callable] = None
    ):
        self.node_id = node_id
        self.socket_client = socket_client
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.failure_callback = failure_callback
        self.logger = logging.getLogger(__name__)

        self.last_heartbeat: Dict[int, float] = {}

        self.node_status: Dict[int, bool] = {}  

        self.running = False
        self.sender_thread = None
        self.checker_thread = None

    def start(self, all_nodes: list):

        if self.running:
            self.logger.warning("Heartbeat monitor already running")
            return

        self.running = True

        for node in all_nodes:
            node_id = node['id']
            if node_id != self.node_id:
                self.node_status[node_id] = True
                self.last_heartbeat[node_id] = time.time()

        self.sender_thread = threading.Thread(
            target=self._send_heartbeats,
            args=(all_nodes,),
            daemon=True
        )
        self.sender_thread.start()

        self.checker_thread = threading.Thread(
            target=self._check_heartbeats,
            daemon=True
        )
        self.checker_thread.start()

        self.logger.info("Heartbeat monitor started")

    def stop(self):

        if not self.running:
            return

        self.logger.info("Stopping heartbeat monitor...")
        self.running = False

        if self.sender_thread and self.sender_thread.is_alive():
            self.sender_thread.join(timeout=5)

        if self.checker_thread and self.checker_thread.is_alive():
            self.checker_thread.join(timeout=5)

        self.logger.info("Heartbeat monitor stopped")

    def _send_heartbeats(self, all_nodes: list):

        while self.running:
            try:

                heartbeat_msg = MessageProtocol.create_heartbeat_message(self.node_id)

                for node in all_nodes:
                    node_id = node['id']

                    if node_id == self.node_id:
                        continue

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

            time.sleep(self.heartbeat_interval)

    def _check_heartbeats(self):

        while self.running:
            try:
                current_time = time.time()

                for node_id, last_time in list(self.last_heartbeat.items()):

                    time_since_heartbeat = current_time - last_time

                    if time_since_heartbeat > self.heartbeat_timeout:

                        if self.node_status.get(node_id, True):
                            self.logger.warning(
                                f"Node {node_id} failed (no heartbeat for {time_since_heartbeat:.1f}s)"
                            )
                            self._mark_node_dead(node_id)

            except Exception as e:
                self.logger.error(f"Error in heartbeat checker: {e}")

            time.sleep(1)

    def record_heartbeat(self, node_id: int):

        current_time = time.time()
        self.last_heartbeat[node_id] = current_time

        if not self.node_status.get(node_id, True):
            self.logger.info(f"Node {node_id} recovered")
            self.node_status[node_id] = True

    def _mark_node_dead(self, node_id: int):

        self.node_status[node_id] = False

        if self.failure_callback:
            try:
                self.failure_callback(node_id)
            except Exception as e:
                self.logger.error(f"Error in failure callback: {e}")

    def get_alive_nodes(self) -> list:

        return [
            node_id for node_id, is_alive in self.node_status.items()
            if is_alive
        ]

    def get_dead_nodes(self) -> list:

        return [
            node_id for node_id, is_alive in self.node_status.items()
            if not is_alive
        ]

    def is_node_alive(self, node_id: int) -> bool:

        return self.node_status.get(node_id, False)

    def get_status(self) -> Dict:

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
