import json
import os
from typing import Dict, Any, List, Optional
from pathlib import Path
from dotenv import load_dotenv


class Config:

    def __init__(self, config_dir: str = "config", nodes_config_file: str = "nodes.json"):

        self.config_dir = config_dir
        self.nodes_config_file = nodes_config_file
        self._nodes_config: Optional[Dict[str, Any]] = None
        self._database_config: Optional[Dict[str, Any]] = None

        load_dotenv()

    def load_nodes_config(self) -> Dict[str, Any]:

        if self._nodes_config is None:
            cluster_nodes_env = os.getenv('CLUSTER_NODES')
            if cluster_nodes_env:
                try:
                    self._nodes_config = json.loads(cluster_nodes_env)
                    return self._nodes_config
                except json.JSONDecodeError:
                    pass

            config_path = os.path.join(self.config_dir, self.nodes_config_file)
            with open(config_path, 'r') as f:
                self._nodes_config = json.load(f)
        return self._nodes_config

    def load_database_config(self) -> Dict[str, Any]:

        if self._database_config is None:
            config_path = os.path.join(self.config_dir, "database.json")
            with open(config_path, 'r') as f:
                self._database_config = json.load(f)

            self._database_config['user'] = os.getenv(
                'MYSQL_USER',
                self._database_config.get('user', 'root')
            )
            self._database_config['password'] = os.getenv(
                'MYSQL_PASSWORD',
                self._database_config.get('password', '')
            )

        return self._database_config

    def get_node_config(self, node_id: int) -> Optional[Dict[str, Any]]:

        nodes_config = self.load_nodes_config()
        nodes = nodes_config.get('nodes', [])

        for node in nodes:
            if node.get('id') == node_id:
                node_config = node.copy()

                if os.getenv('NODE_IP'):
                    node_config['ip'] = os.getenv('NODE_IP')
                if os.getenv('NODE_PORT'):
                    node_config['port'] = int(os.getenv('NODE_PORT'))

                if os.getenv('NODE_MYSQL_HOST'):
                    node_config['mysql_host'] = os.getenv('NODE_MYSQL_HOST')
                if os.getenv('NODE_MYSQL_PORT'):
                    node_config['mysql_port'] = int(os.getenv('NODE_MYSQL_PORT'))
                if os.getenv('NODE_MYSQL_DATABASE'):
                    node_config['mysql_database'] = os.getenv('NODE_MYSQL_DATABASE')

                return node_config

        return None

    def get_all_nodes(self) -> List[Dict[str, Any]]:

        nodes_config = self.load_nodes_config()
        return nodes_config.get('nodes', [])

    def get_heartbeat_config(self) -> Dict[str, int]:

        nodes_config = self.load_nodes_config()
        return {
            'heartbeat_interval': nodes_config.get('heartbeat_interval', 5),
            'heartbeat_timeout': nodes_config.get('heartbeat_timeout', 15)
        }

    def get_env(self, key: str, default: Any = None) -> Any:

        return os.getenv(key, default)


_config_instance: Optional[Config] = None


def get_config(config_dir: str = "config", nodes_config_file: str = "nodes.json") -> Config:

    global _config_instance
    if _config_instance is None:
        _config_instance = Config(config_dir=config_dir, nodes_config_file=nodes_config_file)
    return _config_instance


def reset_config():
    global _config_instance
    _config_instance = None
