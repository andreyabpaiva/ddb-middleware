import logging
import time
from typing import Dict, Any, Optional, Callable
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types


class BullyElection:

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        on_coordinator_elected: Optional[Callable] = None
    ):

        self.node_id = node_id
        self.socket_client = socket_client
        self.on_coordinator_elected = on_coordinator_elected
        self.logger = logging.getLogger(__name__)


        self.election_in_progress = False
        self.coordinator_id: Optional[int] = None
        self.election_timeout = 10  # seconds
        self.response_timeout = 3  # seconds

    def start_election(self, all_nodes: list) -> Optional[int]:

        if self.election_in_progress:
            self.logger.info("Election already in progress")
            return None

        self.logger.info(f"Node {self.node_id} starting election")
        self.election_in_progress = True

        try:

            higher_nodes = [node for node in all_nodes if node['id'] > self.node_id]

            if not higher_nodes:

                self.logger.info(f"Node {self.node_id} has highest ID, becoming coordinator")
                return self._become_coordinator(all_nodes)

            responses = self._send_election_messages(higher_nodes)

            if responses:
                self.logger.info(f"Received responses from higher nodes: {responses}")

                return self._wait_for_coordinator_announcement()
            else:

                self.logger.info("No response from higher nodes, becoming coordinator")
                return self._become_coordinator(all_nodes)

        finally:
            self.election_in_progress = False

    def _send_election_messages(self, higher_nodes: list) -> list:

        responses = []

        for node in higher_nodes:
            node_id = node['id']

            try:

                election_msg = MessageProtocol.create_election_message(
                    sender_id=self.node_id,
                    receiver_id=node_id
                )


                response = self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=election_msg,
                    wait_for_response=True
                )

                if response and response.get('type') == message_types.ELECTION_OK:
                    responses.append(node_id)
                    self.logger.debug(f"Node {node_id} responded OK to election")

            except Exception as e:
                self.logger.debug(f"Node {node_id} did not respond to election: {e}")

        return responses

    def _become_coordinator(self, all_nodes: list) -> int:

        self.logger.info(f"Node {self.node_id} becoming coordinator")
        self.coordinator_id = self.node_id

        self._announce_coordinator(all_nodes)

        if self.on_coordinator_elected:
            try:
                self.on_coordinator_elected(self.node_id, is_self=True)
            except Exception as e:
                self.logger.error(f"Error in coordinator elected callback: {e}")

        return self.node_id

    def _announce_coordinator(self, all_nodes: list):

        announcement_msg = MessageProtocol.create_coordinator_announcement(self.node_id)

        for node in all_nodes:
            node_id = node['id']

            if node_id == self.node_id:
                continue

            try:
                self.socket_client.send_message(
                    host=node['ip'],
                    port=node['port'],
                    message=announcement_msg,
                    wait_for_response=False
                )
                self.logger.debug(f"Sent coordinator announcement to node {node_id}")

            except Exception as e:
                self.logger.warning(f"Failed to announce to node {node_id}: {e}")

    def _wait_for_coordinator_announcement(self) -> Optional[int]:

        self.logger.info("Waiting for coordinator announcement...")

        time.sleep(self.election_timeout)

        if self.coordinator_id is None:
            self.logger.warning("No coordinator announcement received")

        return self.coordinator_id

    def handle_election_message(
        self,
        sender_id: int,
        all_nodes: list
    ) -> Dict[str, Any]:

        self.logger.info(f"Received ELECTION message from node {sender_id}")

        response = MessageProtocol.create_message(
            message_types.ELECTION_OK,
            self.node_id
        )

        if not self.election_in_progress:

            import threading
            election_thread = threading.Thread(
                target=self.start_election,
                args=(all_nodes,),
                daemon=True
            )
            election_thread.start()

        return response

    def handle_coordinator_announcement(self, coordinator_id: int):

        self.logger.info(f"Node {coordinator_id} announced as coordinator")

        old_coordinator = self.coordinator_id
        self.coordinator_id = coordinator_id

        if self.on_coordinator_elected and old_coordinator != coordinator_id:
            try:
                self.on_coordinator_elected(coordinator_id, is_self=False)
            except Exception as e:
                self.logger.error(f"Error in coordinator elected callback: {e}")

    def get_coordinator(self) -> Optional[int]:

        return self.coordinator_id

    def set_coordinator(self, coordinator_id: int):

        self.coordinator_id = coordinator_id
        self.logger.info(f"Coordinator set to node {coordinator_id}")

    def is_coordinator(self) -> bool:

        return self.coordinator_id == self.node_id

    def reset_election(self):

        self.election_in_progress = False
        self.coordinator_id = None
        self.logger.info("Election state reset")
