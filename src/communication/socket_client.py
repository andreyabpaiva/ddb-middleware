import socket
import logging
from typing import Dict, Any, Optional
from src.communication.protocol import MessageProtocol


class SocketClient:

    def __init__(self, timeout: int = 5):

        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.buffer_size = 65536  # 64KB buffer

    def send_message(
        self,
        host: str,
        port: int,
        message: Dict[str, Any],
        wait_for_response: bool = True
    ) -> Optional[Dict[str, Any]]:

        sock = None
        try:
            # Create socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)

            self.logger.debug(f"Connecting to {host}:{port}")
            sock.connect((host, port))

            encoded_message = MessageProtocol.encode_message(message)
            message_length = len(encoded_message)

            sock.sendall(message_length.to_bytes(4, byteorder='big'))

            sock.sendall(encoded_message)
            self.logger.debug(f"Sent message to {host}:{port}: {message.get('type')}")

            if wait_for_response:
                response = self._receive_message(sock)
                self.logger.debug(f"Received response from {host}:{port}")
                return response

            return None

        except socket.timeout:
            self.logger.error(f"Timeout connecting to {host}:{port}")
            raise TimeoutError(f"Connection to {host}:{port} timed out")

        except socket.error as e:
            self.logger.error(f"Socket error connecting to {host}:{port}: {e}")
            raise ConnectionError(f"Failed to connect to {host}:{port}: {e}")

        except Exception as e:
            self.logger.error(f"Unexpected error sending message to {host}:{port}: {e}")
            raise

        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    def _receive_message(self, sock: socket.socket) -> Dict[str, Any]:

        length_data = self._receive_exact(sock, 4)
        if not length_data:
            raise ValueError("Failed to receive message length")

        message_length = int.from_bytes(length_data, byteorder='big')

        message_data = self._receive_exact(sock, message_length)
        if not message_data:
            raise ValueError("Failed to receive message data")

        message = MessageProtocol.decode_message(message_data)

        if not MessageProtocol.verify_message(message):
            raise ValueError("Message checksum verification failed")

        return message

    def _receive_exact(self, sock: socket.socket, num_bytes: int) -> bytes:

        data = b''
        while len(data) < num_bytes:
            chunk = sock.recv(min(num_bytes - len(data), self.buffer_size))
            if not chunk:
                break
            data += chunk
        return data

    def broadcast_message(
        self,
        nodes: list,
        message: Dict[str, Any],
        wait_for_response: bool = False
    ) -> Dict[int, Optional[Dict[str, Any]]]:

        responses = {}

        for node in nodes:
            node_id = node['id']
            try:
                response = self.send_message(
                    node['ip'],
                    node['port'],
                    message,
                    wait_for_response
                )
                responses[node_id] = response
            except Exception as e:
                self.logger.error(f"Failed to send message to node {node_id}: {e}")
                responses[node_id] = None

        return responses

    def send_to_nodes(
        self,
        node_ids: list,
        all_nodes: list,
        message: Dict[str, Any],
        wait_for_response: bool = False
    ) -> Dict[int, Optional[Dict[str, Any]]]:

        target_nodes = [node for node in all_nodes if node['id'] in node_ids]
        return self.broadcast_message(target_nodes, message, wait_for_response)
