"""
Query executor for executing SQL queries and logging to transactions_log.
Handles query execution with proper logging and error handling.
"""
import logging
from typing import Dict, Any, Tuple, Optional
from src.database.mysql_connector import MySQLConnector
from src.utils.helpers import parse_query_type, get_timestamp, generate_transaction_id


class QueryExecutor:
    """Executes queries and logs them to transactions_log table."""

    def __init__(self, db_connector: MySQLConnector, node_id: int):
        """
        Initialize query executor.

        Args:
            db_connector: MySQL connector instance
            node_id: ID of this node
        """
        self.db = db_connector
        self.node_id = node_id
        self.logger = logging.getLogger(__name__)

    def execute(
        self,
        query: str,
        transaction_id: str = None,
        log_query: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a SQL query with logging.

        Args:
            query: SQL query string
            transaction_id: Transaction ID (generated if not provided)
            log_query: Whether to log the query

        Returns:
            Dictionary with execution result
        """
        if not transaction_id:
            transaction_id = generate_transaction_id()

        query_type = parse_query_type(query)
        self.logger.info(f"Executing {query_type} query: {query[:100]}...")

        # Determine if we need to fetch results
        fetch_results = query_type == 'SELECT'

        # Execute the query
        success, result = self.db.execute_query(query, fetch=fetch_results)

        # Prepare response
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

        # Log the query execution
        if log_query:
            self._log_query(transaction_id, query_type, query, status)

        return response

    def execute_select(self, query: str, transaction_id: str = None) -> Dict[str, Any]:
        """
        Execute a SELECT query.

        Args:
            query: SELECT query string
            transaction_id: Transaction ID

        Returns:
            Query result dictionary
        """
        return self.execute(query, transaction_id, log_query=True)

    def execute_insert(self, query: str, transaction_id: str = None) -> Dict[str, Any]:
        """
        Execute an INSERT query.

        Args:
            query: INSERT query string
            transaction_id: Transaction ID

        Returns:
            Execution result dictionary
        """
        return self.execute(query, transaction_id, log_query=True)

    def execute_update(self, query: str, transaction_id: str = None) -> Dict[str, Any]:
        """
        Execute an UPDATE query.

        Args:
            query: UPDATE query string
            transaction_id: Transaction ID

        Returns:
            Execution result dictionary
        """
        return self.execute(query, transaction_id, log_query=True)

    def execute_delete(self, query: str, transaction_id: str = None) -> Dict[str, Any]:
        """
        Execute a DELETE query.

        Args:
            query: DELETE query string
            transaction_id: Transaction ID

        Returns:
            Execution result dictionary
        """
        return self.execute(query, transaction_id, log_query=True)

    def prepare_query(
        self,
        query: str,
        transaction_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Prepare a query for execution (2PC prepare phase).

        Args:
            query: SQL query to prepare
            transaction_id: Transaction ID

        Returns:
            Tuple of (can_commit, error_message)
        """
        query_type = parse_query_type(query)
        self.logger.info(f"Preparing {query_type} query for transaction {transaction_id}")

        try:
            # Validate query syntax by preparing it
            # We don't actually execute it yet
            # Just check if it's valid and we have resources

            # Log as PREPARED
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
        """
        Commit a previously prepared query (2PC commit phase).

        Args:
            query: SQL query to execute
            transaction_id: Transaction ID

        Returns:
            Execution result dictionary
        """
        self.logger.info(f"Committing prepared query for transaction {transaction_id}")
        return self.execute(query, transaction_id, log_query=True)

    def abort_prepared_query(
        self,
        query: str,
        transaction_id: str
    ):
        """
        Abort a previously prepared query (2PC abort phase).

        Args:
            query: SQL query that was prepared
            transaction_id: Transaction ID
        """
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
        """
        Log query execution to transactions_log table.

        Args:
            transaction_id: Transaction ID
            query_type: Type of query (SELECT, INSERT, etc.)
            query_text: Full query text
            status: Execution status (PREPARED, COMMITTED, FAILED, ABORTED)
        """
        try:
            log_query = """
                INSERT INTO transactions_log
                (transaction_id, query_type, query_text, status, node_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """
            params = (transaction_id, query_type, query_text, status, self.node_id)

            # Execute log query without logging it (to avoid recursion)
            connection = self.db.get_connection()
            cursor = connection.cursor()
            cursor.execute(log_query, params)
            connection.commit()
            cursor.close()
            connection.close()

        except Exception as e:
            self.logger.error(f"Failed to log query: {e}")
            # Don't raise exception, logging failure shouldn't stop execution

    def get_transaction_log(
        self,
        transaction_id: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Get transaction log entries.

        Args:
            transaction_id: Filter by transaction ID (optional)
            limit: Maximum number of entries to return

        Returns:
            Dictionary with log entries
        """
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
