"""
MySQL database connector with connection pooling.
Manages database connections and executes queries.
"""
import logging
import mysql.connector
from mysql.connector import pooling, Error
from typing import Any, List, Tuple, Optional, Dict
import os


class MySQLConnector:
    """MySQL database connector with connection pooling."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str = None,
        password: str = None,
        pool_name: str = "mypool",
        pool_size: int = 5
    ):
        """
        Initialize MySQL connector with connection pool.

        Args:
            host: MySQL server host
            port: MySQL server port
            database: Database name
            user: Database user (defaults to env variable)
            password: Database password (defaults to env variable)
            pool_name: Connection pool name
            pool_size: Connection pool size
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user or os.getenv('MYSQL_USER', 'ddb_user')
        self.password = password or os.getenv('MYSQL_PASSWORD', 'ddb_password')
        self.pool_name = pool_name
        self.pool_size = pool_size
        self.logger = logging.getLogger(__name__)
        self.connection_pool = None

        self._create_connection_pool()

    def _create_connection_pool(self):
        """Create MySQL connection pool."""
        try:
            self.connection_pool = pooling.MySQLConnectionPool(
                pool_name=self.pool_name,
                pool_size=self.pool_size,
                pool_reset_session=True,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                charset='utf8mb4',
                autocommit=False,
                connect_timeout=10
            )
            self.logger.info(f"Connection pool created for {self.host}:{self.port}/{self.database}")

        except Error as e:
            self.logger.error(f"Error creating connection pool: {e}")
            raise

    def get_connection(self):
        """
        Get a connection from the pool.

        Returns:
            MySQL connection

        Raises:
            Error: If unable to get connection
        """
        try:
            return self.connection_pool.get_connection()
        except Error as e:
            self.logger.error(f"Error getting connection from pool: {e}")
            raise

    def execute_query(
        self,
        query: str,
        params: Tuple = None,
        fetch: bool = True
    ) -> Tuple[bool, Any]:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Query parameters (optional)
            fetch: Whether to fetch results (for SELECT queries)

        Returns:
            Tuple of (success, result/error)
        """
        connection = None
        cursor = None

        try:
            connection = self.get_connection()
            cursor = connection.cursor(dictionary=True)

            # Execute query
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch results if needed
            if fetch:
                result = cursor.fetchall()
                return True, result
            else:
                # For write operations, commit the transaction
                connection.commit()
                return True, cursor.rowcount

        except Error as e:
            self.logger.error(f"Database error executing query: {e}")
            if connection:
                connection.rollback()
            return False, str(e)

        except Exception as e:
            self.logger.error(f"Unexpected error executing query: {e}")
            if connection:
                connection.rollback()
            return False, str(e)

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def execute_transaction(
        self,
        queries: List[Tuple[str, Optional[Tuple]]]
    ) -> Tuple[bool, Any]:
        """
        Execute multiple queries in a transaction.

        Args:
            queries: List of (query, params) tuples

        Returns:
            Tuple of (success, result/error)
        """
        connection = None
        cursor = None

        try:
            connection = self.get_connection()
            cursor = connection.cursor(dictionary=True)

            results = []

            # Execute all queries
            for query, params in queries:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Try to fetch if it's a SELECT query
                if query.strip().upper().startswith('SELECT'):
                    results.append(cursor.fetchall())
                else:
                    results.append(cursor.rowcount)

            # Commit transaction
            connection.commit()
            return True, results

        except Error as e:
            self.logger.error(f"Transaction error: {e}")
            if connection:
                connection.rollback()
            return False, str(e)

        except Exception as e:
            self.logger.error(f"Unexpected transaction error: {e}")
            if connection:
                connection.rollback()
            return False, str(e)

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def begin_transaction(self) -> Optional[Any]:
        """
        Begin a new transaction and return connection.

        Returns:
            Database connection or None on error
        """
        try:
            connection = self.get_connection()
            connection.start_transaction()
            return connection
        except Error as e:
            self.logger.error(f"Error starting transaction: {e}")
            return None

    def commit_transaction(self, connection) -> bool:
        """
        Commit a transaction.

        Args:
            connection: Database connection

        Returns:
            True if successful, False otherwise
        """
        try:
            connection.commit()
            connection.close()
            return True
        except Error as e:
            self.logger.error(f"Error committing transaction: {e}")
            return False

    def rollback_transaction(self, connection) -> bool:
        """
        Rollback a transaction.

        Args:
            connection: Database connection

        Returns:
            True if successful, False otherwise
        """
        try:
            connection.rollback()
            connection.close()
            return True
        except Error as e:
            self.logger.error(f"Error rolling back transaction: {e}")
            return False

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def close_pool(self):
        """Close all connections in the pool."""
        # MySQL connector doesn't provide a direct way to close the pool
        # Connections will be closed when they're returned to the pool
        self.logger.info("Connection pool closed")
