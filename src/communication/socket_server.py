"""
Socket server for receiving messages from other nodes.
Handles incoming connections and message dispatching.
"""
import socket
import threading
import logging
from typing import Dict, Any, Callable, Optional
from src.communication.protocol import MessageProtocol


class SocketServer:
    """Server for receiving messages from other nodes via TCP sockets."""

    def __init__(self, host: str, port: int, message_handler: Callable):
        """
        Initialize socket server.

        Args:
            host: Host address to bind to
            port: Port number to listen on
            message_handler: Callback function to handle received messages
        """
        self.host = host
        self.port = port
        self.message_handler = message_handler
        self.logger = logging.getLogger(__name__)
        self.buffer_size = 65536  # 64KB buffer
        self.running = False
        self.server_socket = None
        self.server_thread = None

    def start(self):
        """Start the socket server in a separate thread."""
        if self.running:
            self.logger.warning("Server is already running")
            return

        self.running = True
        self.server_thread = threading.Thread(target=self._run_server, daemon=True)
        self.server_thread.start()
        self.logger.info(f"Socket server started on {self.host}:{self.port}")

    def stop(self):
        """Stop the socket server."""
        if not self.running:
            return

        self.logger.info("Stopping socket server...")
        self.running = False

        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                self.logger.error(f"Error closing server socket: {e}")

        # Wait for server thread to finish
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)

        self.logger.info("Socket server stopped")

    def _run_server(self):
        """Main server loop."""
        try:
            # Create server socket
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.server_socket.settimeout(1.0)  # Timeout to check running flag

            self.logger.info(f"Listening for connections on {self.host}:{self.port}")

            while self.running:
                try:
                    # Accept connection
                    client_socket, client_address = self.server_socket.accept()
                    self.logger.debug(f"Accepted connection from {client_address}")

                    # Handle client in separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()

                except socket.timeout:
                    # Timeout is expected, continue loop
                    continue

                except Exception as e:
                    if self.running:
                        self.logger.error(f"Error accepting connection: {e}")

        except Exception as e:
            self.logger.error(f"Server error: {e}")

        finally:
            if self.server_socket:
                try:
                    self.server_socket.close()
                except Exception:
                    pass

    def _handle_client(self, client_socket: socket.socket, client_address: tuple):
        """
        Handle a client connection.

        Args:
            client_socket: Client socket
            client_address: Client address tuple (host, port)
        """
        try:
            # Receive message
            message = self._receive_message(client_socket)

            if message:
                self.logger.debug(f"Received message from {client_address}: {message.get('type')}")

                # Process message through handler
                response = self.message_handler(message)

                # Send response if provided
                if response:
                    self._send_message(client_socket, response)
                    self.logger.debug(f"Sent response to {client_address}")

        except ValueError as e:
            self.logger.error(f"Invalid message from {client_address}: {e}")
            # Send error response
            error_response = MessageProtocol.create_response(
                sender_id=0,
                success=False,
                error=str(e)
            )
            try:
                self._send_message(client_socket, error_response)
            except Exception:
                pass

        except BrokenPipeError:
            # Client disconnected, this is normal for heartbeats
            self.logger.debug(f"Client {client_address} disconnected (broken pipe)")

        except Exception as e:
            self.logger.error(f"Error handling client {client_address}: {e}")

        finally:
            try:
                client_socket.close()
            except Exception:
                pass

    def _receive_message(self, sock: socket.socket) -> Optional[Dict[str, Any]]:
        """
        Receive a message from socket.

        Args:
            sock: Socket to receive from

        Returns:
            Decoded message dictionary or None

        Raises:
            ValueError: If message is invalid
        """
        # Receive message length (4 bytes)
        length_data = self._receive_exact(sock, 4)
        if not length_data or len(length_data) < 4:
            return None

        message_length = int.from_bytes(length_data, byteorder='big')

        # Validate message length
        if message_length <= 0 or message_length > 10 * 1024 * 1024:  # Max 10MB
            raise ValueError(f"Invalid message length: {message_length}")

        # Receive actual message
        message_data = self._receive_exact(sock, message_length)
        if not message_data or len(message_data) < message_length:
            raise ValueError("Incomplete message received")

        # Decode message
        message = MessageProtocol.decode_message(message_data)

        # Verify checksum
        if not MessageProtocol.verify_message(message):
            raise ValueError("Message checksum verification failed")

        return message

    def _send_message(self, sock: socket.socket, message: Dict[str, Any]):
        """
        Send a message through socket.

        Args:
            sock: Socket to send through
            message: Message to send
        """
        # Encode message
        encoded_message = MessageProtocol.encode_message(message)
        message_length = len(encoded_message)

        # Send message length first (4 bytes)
        sock.sendall(message_length.to_bytes(4, byteorder='big'))

        # Send actual message
        sock.sendall(encoded_message)

    def _receive_exact(self, sock: socket.socket, num_bytes: int) -> bytes:
        """
        Receive exact number of bytes from socket.

        Args:
            sock: Socket to receive from
            num_bytes: Number of bytes to receive

        Returns:
            Received bytes
        """
        data = b''
        sock.settimeout(5.0)  # 5 second timeout for receiving

        while len(data) < num_bytes:
            chunk = sock.recv(min(num_bytes - len(data), self.buffer_size))
            if not chunk:
                break
            data += chunk

        return data

    def is_running(self) -> bool:
        """
        Check if server is running.

        Returns:
            True if running, False otherwise
        """
        return self.running
