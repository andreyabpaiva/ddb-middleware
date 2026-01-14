import time
import uuid
from datetime import datetime
from typing import Any, Dict


def generate_transaction_id() -> str:

    timestamp = int(time.time() * 1000)
    unique_id = str(uuid.uuid4())[:8]
    return f"TXN-{timestamp}-{unique_id}"


def generate_message_id() -> str:

    timestamp = int(time.time() * 1000)
    unique_id = str(uuid.uuid4())[:8]
    return f"MSG-{timestamp}-{unique_id}"


def get_timestamp() -> str:

    return datetime.utcnow().isoformat()


def get_unix_timestamp() -> int:

    return int(time.time() * 1000)


def parse_query_type(query: str) -> str:

    query = query.strip().upper()

    if query.startswith('SELECT'):
        return 'SELECT'
    elif query.startswith('INSERT'):
        return 'INSERT'
    elif query.startswith('UPDATE'):
        return 'UPDATE'
    elif query.startswith('DELETE'):
        return 'DELETE'
    elif query.startswith('CREATE'):
        return 'CREATE'
    elif query.startswith('DROP'):
        return 'DROP'
    elif query.startswith('ALTER'):
        return 'ALTER'
    elif query.startswith('TRUNCATE'):
        return 'TRUNCATE'
    else:
        return 'UNKNOWN'


def is_write_query(query: str) -> bool:

    query_type = parse_query_type(query)
    return query_type in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE']


def is_read_query(query: str) -> bool:

    query_type = parse_query_type(query)
    return query_type == 'SELECT'


def format_query_result(result: Any, node_id: int) -> Dict[str, Any]:

    return {
        'data': result,
        'node_id': node_id,
        'timestamp': get_timestamp(),
        'success': True
    }


def format_error_response(error: str, node_id: int = None) -> Dict[str, Any]:

    response = {
        'success': False,
        'error': error,
        'timestamp': get_timestamp()
    }

    if node_id is not None:
        response['node_id'] = node_id

    return response


def retry_on_failure(func, max_retries: int = 3, delay: float = 1.0):

    last_exception = None

    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(delay)

    raise last_exception


def safe_dict_get(dictionary: Dict, *keys, default=None) -> Any:

    current = dictionary

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default

    return current
