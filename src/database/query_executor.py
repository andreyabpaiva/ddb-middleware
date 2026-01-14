import logging
from typing import Dict, Any, Tuple, Optional
from src.database.mysql_connector import MySQLConnector
from src.utils.helpers import parse_query_type, get_timestamp, generate_transaction_id


class QueryExecutor:

    def __init__(self, db_connector: MySQLConnector, node_id: int):

        self.db = db_connector
        self.node_id = node_id
        self.logger = logging.getLogger(__name__)

    def execute(
        self,
        query: str,
        transaction_id: str = None,
        log_query: bool = True
    ) -> Dict[str, Any]:

        if not transaction_id:
            transaction_id = generate_transaction_id()

        query_type = parse_query_type(query)
        self.logger.info(f"Executing {query_type} query: {query[:100]}...")

        fetch_results = query_type == 'SELECT'

        success, result = self.db.execute_query(query, fetch=fetch_results)

        response = {
            'success': success,
            'transaction_id': transaction_id,
            'query_type': query_type,
            'node_id': self.node_id,
            'timestamp': get_timestamp()
        }

        if success:
            if fetch_results:
                response['data'] = result
                response['row_count'] = len(result)
            else:
                response['affected_rows'] = result
            status = 'COMMITTED'
        else:
            response['error'] = result
            status = 'FAILED'

        if log_query:
            self._log_query(transaction_id, query_type, query, status)

        return response

    def execute_select(self, query: str, transaction_id: str = None) -> Dict[str, Any]:

        return self.execute(query, transaction_id, log_query=True)

    def execute_insert(self, query: str, transaction_id: str = None) -> Dict[str, Any]:

        return self.execute(query, transaction_id, log_query=True)

    def execute_update(self, query: str, transaction_id: str = None) -> Dict[str, Any]:

        return self.execute(query, transaction_id, log_query=True)

    def execute_delete(self, query: str, transaction_id: str = None) -> Dict[str, Any]:

        return self.execute(query, transaction_id, log_query=True)

    def prepare_query(
        self,
        query: str,
        transaction_id: str
    ) -> Tuple[bool, Optional[str]]:

        query_type = parse_query_type(query)
        self.logger.info(f"Preparing {query_type} query for transaction {transaction_id}")

        try:
            self._log_query(transaction_id, query_type, query, 'PREPARED')

            return True, None

        except Exception as e:
            error_msg = f"Failed to prepare query: {e}"
            self.logger.error(error_msg)
            self._log_query(transaction_id, query_type, query, 'PREPARE_FAILED')
            return False, error_msg

    def commit_prepared_query(
        self,
        query: str,
        transaction_id: str
    ) -> Dict[str, Any]:

        self.logger.info(f"Committing prepared query for transaction {transaction_id}")
        return self.execute(query, transaction_id, log_query=True)

    def abort_prepared_query(
        self,
        query: str,
        transaction_id: str
    ):
        
        query_type = parse_query_type(query)
        self.logger.info(f"Aborting prepared query for transaction {transaction_id}")
        self._log_query(transaction_id, query_type, query, 'ABORTED')

    def _log_query(
        self,
        transaction_id: str,
        query_type: str,
        query_text: str,
        status: str
    ):
        
        try:
            log_query = """
                INSERT INTO transactions_log
                (transaction_id, query_type, query_text, status, node_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """
            params = (transaction_id, query_type, query_text, status, self.node_id)

            connection = self.db.get_connection()
            cursor = connection.cursor()
            cursor.execute(log_query, params)
            connection.commit()
            cursor.close()
            connection.close()

        except Exception as e:
            self.logger.error(f"Failed to log query: {e}")

    def get_transaction_log(
        self,
        transaction_id: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:

        try:
            if transaction_id:
                query = """
                    SELECT * FROM transactions_log
                    WHERE transaction_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """
                params = (transaction_id, limit)
            else:
                query = """
                    SELECT * FROM transactions_log
                    WHERE node_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """
                params = (self.node_id, limit)

            success, result = self.db.execute_query(query, params, fetch=True)

            return {
                'success': success,
                'data': result if success else [],
                'error': result if not success else None
            }

        except Exception as e:
            self.logger.error(f"Error fetching transaction log: {e}")
            return {
                'success': False,
                'data': [],
                'error': str(e)
            }
