#!/usr/bin/env python3
import socket
import json
import sys
import argparse
from typing import Dict, Any, Optional


class DistributedDBClient:

    def __init__(self, host: str, port: int, timeout: int = 30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.buffer_size = 65536

    def send_query(self, query: str) -> Optional[Dict[str, Any]]:
        sock = None
        try:
            # Create message
            message = {
                'type': 'QUERY',
                'sender_id': 0,  # Client ID
                'data': {
                    'query': query
                },
                'timestamp': self._get_timestamp()
            }

            # Add checksum
            message = self._add_checksum(message)

            # Encode message
            encoded_message = json.dumps(message).encode('utf-8')
            message_length = len(encoded_message)

            # Connect to middleware
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))

            # Send message length
            sock.sendall(message_length.to_bytes(4, byteorder='big'))

            # Send message
            sock.sendall(encoded_message)

            # Receive response length
            length_data = self._receive_exact(sock, 4)
            if not length_data:
                return None

            response_length = int.from_bytes(length_data, byteorder='big')

            # Receive response
            response_data = self._receive_exact(sock, response_length)
            if not response_data:
                return None

            # Decode response
            response = json.loads(response_data.decode('utf-8'))

            return response

        except socket.timeout:
            print(f"Error: Connection timeout")
            return None

        except ConnectionRefusedError:
            print(f"Error: Connection refused. Is the middleware running?")
            return None

        except Exception as e:
            print(f"Error: {e}")
            return None

        finally:
            if sock:
                sock.close()

    def _receive_exact(self, sock: socket.socket, num_bytes: int) -> bytes:
        """
        Receive exact number of bytes.

        Args:
            sock: Socket to receive from
            num_bytes: Number of bytes to receive

        Returns:
            Received bytes
        """
        data = b''
        while len(data) < num_bytes:
            chunk = sock.recv(min(num_bytes - len(data), self.buffer_size))
            if not chunk:
                break
            data += chunk
        return data

    def _get_timestamp(self) -> str:
        """Get current timestamp."""
        from datetime import datetime
        return datetime.utcnow().isoformat()

    def _add_checksum(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Add checksum to message."""
        import hashlib

        message_copy = message.copy()
        if 'checksum' in message_copy:
            del message_copy['checksum']

        message_str = json.dumps(message_copy, sort_keys=True)
        checksum = hashlib.sha256(message_str.encode('utf-8')).hexdigest()

        message['checksum'] = checksum
        return message


def format_result(result: Dict[str, Any]):
    """
    Format and display query result.

    Args:
        result: Query result dictionary
    """
    if not result:
        print("No response received")
        return

    # Extract data from response - handle nested structure
    data = result.get('data', {})
    if 'result' in data:
        data = data['result']  # Unwrap nested result

    success = data.get('success', False)

    if not success:
        print(f"\n❌ Query failed: {data.get('error', 'Unknown error')}")
        return

    print(f"\nQuery successful")
    print(f"Node: {data.get('node_id', 'unknown')}")
    print(f"Transaction ID: {data.get('transaction_id', 'N/A')}")

    # Display results
    if 'data' in data and data['data']:
        rows = data['data']
        print(f"\nResults ({len(rows)} rows):")
        print("=" * 80)

        # Print header
        if rows:
            headers = list(rows[0].keys())
            print(" | ".join(f"{h:15}" for h in headers))
            print("-" * 80)

            # Print rows
            for row in rows:
                values = [str(row.get(h, ''))[:15] for h in headers]
                print(" | ".join(f"{v:15}" for v in values))

        print("=" * 80)

    elif 'affected_rows' in data:
        print(f"Affected rows: {data['affected_rows']}")

    if 'response_time' in data:
        print(f"Response time: {data['response_time']:.3f}s")


def run_interactive_mode(client: DistributedDBClient):
    """
    Run interactive CLI mode.

    Args:
        client: Database client instance
    """
    print("\n" + "=" * 80)
    print("Distributed Database Middleware - Interactive Client")
    print("=" * 80)
    print("\nCommands:")
    print("  - Enter SQL queries (SELECT, INSERT, UPDATE, DELETE)")
    print("  - Type 'EXIT' or 'QUIT' to exit")
    print("  - Type 'HELP' for this message")
    print("=" * 80 + "\n")

    while True:
        try:
            # Get user input
            query = input("ddb> ").strip()

            if not query:
                continue

            # Handle special commands
            if query.upper() in ['EXIT', 'QUIT']:
                print("Goodbye!")
                break

            if query.upper() == 'HELP':
                print("\nSupported SQL commands:")
                print("  - SELECT: Query data")
                print("  - INSERT: Insert new data")
                print("  - UPDATE: Update existing data")
                print("  - DELETE: Delete data")
                print()
                continue

            # Send query
            print("Executing query...")
            result = client.send_query(query)

            # Display result
            format_result(result)
            print()

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break

        except Exception as e:
            print(f"Error: {e}")


def run_single_query(client: DistributedDBClient, query: str):
    """
    Execute a single query and exit.

    Args:
        client: Database client instance
        query: SQL query to execute
    """
    result = client.send_query(query)
    format_result(result)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Distributed Database Middleware Client'
    )

    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Middleware host (default: localhost)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=5001,
        help='Middleware port (default: 5001)'
    )

    parser.add_argument(
        '--query',
        type=str,
        help='Execute single query and exit'
    )

    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Connection timeout in seconds (default: 30)'
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()

    # Create client
    client = DistributedDBClient(
        host=args.host,
        port=args.port,
        timeout=args.timeout
    )

    print(f"Connecting to {args.host}:{args.port}")

    # Run in appropriate mode
    if args.query:
        # Single query mode
        run_single_query(client, args.query)
    else:
        # Interactive mode
        run_interactive_mode(client)


if __name__ == '__main__':
    main()
