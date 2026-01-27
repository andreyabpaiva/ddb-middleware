import sys
import argparse
import signal
import time
from src.core.node import Node
from src.utils.config import Config
from src.utils.logger import setup_logger

node_instance = None

def signal_handler(sig, frame):

    print("\nReceived shutdown signal, stopping node...")

    if node_instance:
        node_instance.stop()

    sys.exit(0)


def parse_arguments():

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
        '--nodes-config',
        type=str,
        default='nodes.json',
        help='Nodes configuration file name (default: nodes.json). Use nodes.local.json for physical machines.'
    )

    parser.add_argument(
        '--local',
        action='store_true',
        help='Shortcut for --nodes-config=nodes.local.json (for physical machine deployment)'
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

    global node_instance

    args = parse_arguments()

    import logging
    log_level = getattr(logging, args.log_level)

    nodes_config_file = args.nodes_config
    if args.local:
        nodes_config_file = 'nodes.local.json'

    print(f"Starting Distributed Database Node {args.node_id}")
    print(f"Configuration directory: {args.config_dir}")
    print(f"Nodes config file: {nodes_config_file}")
    print(f"Log level: {args.log_level}")

    try:
        config = Config(config_dir=args.config_dir, nodes_config_file=nodes_config_file)

        node_instance = Node(node_id=args.node_id, config=config)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        node_instance.start()

        print(f"\nNode {args.node_id} is running...")
        print(f"Press Ctrl+C to stop\n")

        while True:
            time.sleep(1)


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
