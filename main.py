#!/usr/bin/env python3
import sys
import argparse
import signal
import time
from src.core.node import Node
from src.utils.config import Config
from src.utils.logger import setup_logger

# Global node instance for signal handling
node_instance = None


def signal_handler(sig, frame):
    """
    Handle shutdown signals gracefully.

    Args:
        sig: Signal number
        frame: Current stack frame
    """
    print("\nReceived shutdown signal, stopping node...")

    if node_instance:
        node_instance.stop()

    sys.exit(0)


def parse_arguments():
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Distributed Database Middleware Node'
    )

    parser.add_argument(
        '--node-id',
        type=int,
        required=True,
        help='Node ID (1, 2, or 3)'
    )

    parser.add_argument(
        '--config-dir',
        type=str,
        default='config',
        help='Configuration directory path (default: config)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main():
    """Main function to start the node."""
    global node_instance

    # Parse arguments
    args = parse_arguments()

    # Setup basic logging
    import logging
    log_level = getattr(logging, args.log_level)

    print(f"Starting Distributed Database Node {args.node_id}")
    print(f"Configuration directory: {args.config_dir}")
    print(f"Log level: {args.log_level}")

    try:
        # Load configuration
        config = Config(config_dir=args.config_dir)

        # Create node instance
        node_instance = Node(node_id=args.node_id, config=config)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start the node
        node_instance.start()

        print(f"\nNode {args.node_id} is running...")
        print(f"Press Ctrl+C to stop\n")

        # Keep running
        while True:
            time.sleep(1)

            # Optionally print status periodically
            # Uncomment to enable periodic status updates
            # if time.time() % 30 < 1:  # Every 30 seconds
            #     status = node_instance.get_status()
            #     print(f"Status: {status}")

    except KeyboardInterrupt:
        print("\nShutting down...")
        if node_instance:
            node_instance.stop()

    except Exception as e:
        print(f"\nError starting node: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
