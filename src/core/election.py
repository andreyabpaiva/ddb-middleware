"""
Bully election algorithm implementation.
Coordinates leader election when coordinator fails.
"""
import logging
import time
from typing import Dict, Any, Optional, Callable
from src.communication.socket_client import SocketClient
from src.communication.protocol import MessageProtocol
from src.communication import message_types


class BullyElection:
    """Implements the Bully algorithm for coordinator election."""

    def __init__(
        self,
        node_id: int,
        socket_client: SocketClient,
        on_coordinator_elected: Optional[Callable] = None
    ):
        """
        Initialize Bully election.

        Args:
            node_id: ID of this node
            socket_client: Socket client for communication
            on_coordinator_elected: Callback when coordinator is elected
        """
        self.node_id = node_id
        self.socket_client = socket_client
        self.on_coordinator_elected = on_coordinator_elected
        self.logger = logging.getLogger(__name__)

        # Election state
        self.election_in_progress = False
        self.coordinator_id: Optional[int] = None
        self.election_timeout = 10  # seconds
        self.response_timeout = 3  # seconds

    def start_election(self, all_nodes: list) -> Optional[int]:
        """
        Start a new election using Bully algorithm.

        Args:
            all_nodes: List of all node configurations

        Returns:
            Elected coordinator ID
        """
        if self.election_in_progress:
            self.logger.info("Election already in progress")
            return None

        self.logger.info(f"Node {self.node_id} starting election")
        self.election_in_progress = True

        try:
            # Find nodes with higher IDs
            higher_nodes = [node for node in all_nodes if node['id'] > self.node_id]

            if not higher_nodes:
                # This node has the highest ID - become coordinator
                self.logger.info(f"Node {self.node_id} has highest ID, becoming coordinator")
                return self._become_coordinator(all_nodes)

            # Send ELECTION message to higher nodes
            responses = self._send_election_messages(higher_nodes)

            # Check if any higher node responded
            if responses:
                self.logger.info(f"Received responses from higher nodes: {responses}")
                # Wait for coordinator announcement
                return self._wait_for_coordinator_announcement()
            else:
                # No response from higher nodes - become coordinator
                self.logger.info("No response from higher nodes, becoming coordinator")
                return self._become_coordinator(all_nodes)

        finally:
            self.election_in_progress = False

    def _send_election_messages(self, higher_nodes: list) -> list:
        """
        Send ELECTION messages to nodes with higher IDs.

        Args:
            higher_nodes: List of nodes with higher IDs

        Returns:
            List of node IDs that responded with OK
        """
        responses = []

        for node in higher_nodes:
            node_id = node['id']

            try:
                # Create ELECTION message
                election_msg = MessageProtocol.create_election_message(
                    sender_id=self.node_id,
                    receiver_id=node_id
                )

                # Send and wait for response
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
        """
        Become coordinator and announce to all nodes.

        Args:
            all_nodes: List of all node configurations

        Returns:
            Coordinator ID (self)
        """
        self.logger.info(f"Node {self.node_id} becoming coordinator")
        self.coordinator_id = self.node_id

        # Announce to all nodes
        self._announce_coordinator(all_nodes)

        # Trigger callback
        if self.on_coordinator_elected:
            try:
                self.on_coordinator_elected(self.node_id, is_self=True)
            except Exception as e:
                self.logger.error(f"Error in coordinator elected callback: {e}")

        return self.node_id

    def _announce_coordinator(self, all_nodes: list):
        """
        Announce this node as coordinator to all other nodes.

        Args:
            all_nodes: List of all node configurations
        """
        announcement_msg = MessageProtocol.create_coordinator_announcement(self.node_id)

        for node in all_nodes:
            node_id = node['id']

            # Skip self
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
        """
        Wait for coordinator announcement from higher node.

        Returns:
            Coordinator ID or None if timeout
        """
        self.logger.info("Waiting for coordinator announcement...")

        # In a real implementation, this would wait for announcement message
        # For now, we'll use a timeout
        time.sleep(self.election_timeout)

        # If no announcement received, start election again
        if self.coordinator_id is None:
            self.logger.warning("No coordinator announcement received")

        return self.coordinator_id

    def handle_election_message(
        self,
        sender_id: int,
        all_nodes: list
    ) -> Dict[str, Any]:
        """
        Handle incoming ELECTION message.

        Args:
            sender_id: ID of node that sent election message
            all_nodes: List of all node configurations

        Returns:
            Response message
        """
        self.logger.info(f"Received ELECTION message from node {sender_id}")

        # Respond with OK
        response = MessageProtocol.create_message(
            message_types.ELECTION_OK,
            self.node_id
        )

        # Start own election (higher ID nodes should also participate)
        if not self.election_in_progress:
            # Start election in separate thread to avoid blocking
            import threading
            election_thread = threading.Thread(
                target=self.start_election,
                args=(all_nodes,),
                daemon=True
            )
            election_thread.start()

        return response

    def handle_coordinator_announcement(self, coordinator_id: int):
        """
        Handle coordinator announcement message.

        Args:
            coordinator_id: ID of announced coordinator
        """
        self.logger.info(f"Node {coordinator_id} announced as coordinator")

        old_coordinator = self.coordinator_id
        self.coordinator_id = coordinator_id

        # Trigger callback
        if self.on_coordinator_elected and old_coordinator != coordinator_id:
            try:
                self.on_coordinator_elected(coordinator_id, is_self=False)
            except Exception as e:
                self.logger.error(f"Error in coordinator elected callback: {e}")

    def get_coordinator(self) -> Optional[int]:
        """
        Get current coordinator ID.

        Returns:
            Coordinator ID or None
        """
        return self.coordinator_id

    def set_coordinator(self, coordinator_id: int):
        """
        Set coordinator manually (for initialization).

        Args:
            coordinator_id: Coordinator ID
        """
        self.coordinator_id = coordinator_id
        self.logger.info(f"Coordinator set to node {coordinator_id}")

    def is_coordinator(self) -> bool:
        """
        Check if this node is the coordinator.

        Returns:
            True if this node is coordinator, False otherwise
        """
        return self.coordinator_id == self.node_id

    def reset_election(self):
        """Reset election state."""
        self.election_in_progress = False
        self.coordinator_id = None
        self.logger.info("Election state reset")
