"""
HitCraft AI Analytics Engine - Database Connector

This module handles connections to the database, providing methods for interacting
with the database including setup, querying, and connection management.
"""

import contextlib
from typing import Dict, List, Optional, Any, Generator, Tuple

import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.pool import QueuePool

from hitcraft_analytics.config.db_config import (
    DB_CONNECTION_STRING,
    DB_POOL_SIZE,
    DB_MAX_OVERFLOW,
    DB_POOL_TIMEOUT,
    DB_POOL_RECYCLE,
    DB_STATEMENT_TIMEOUT,
    USE_TIMESCALEDB,
    TIMESCALE_CHUNK_INTERVAL
)
from hitcraft_analytics.utils.logging.logger import get_logger

logger = get_logger(__name__)

# Create SQLAlchemy Base
Base = declarative_base()

class DatabaseConnector:
    """
    Handles database connections and operations.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the database connector.
        
        Args:
            connection_string (Optional[str]): Database connection string.
                Defaults to the connection string from DB_CONNECTION_STRING.
        """
        self.connection_string = connection_string or DB_CONNECTION_STRING
        self.engine = None
        self.metadata = MetaData()
        self.session_factory = None
        self._setup_engine()
        
        logger.info("Database connector initialized")
    
    def _setup_engine(self) -> None:
        """
        Set up the SQLAlchemy engine with connection pooling.
        """
        try:
            # Configure connection pool
            self.engine = create_engine(
                self.connection_string,
                poolclass=QueuePool,
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW,
                pool_timeout=DB_POOL_TIMEOUT,
                pool_recycle=DB_POOL_RECYCLE,
                connect_args={
                    "options": f"-c statement_timeout={DB_STATEMENT_TIMEOUT}"
                }
            )
            
            # Create session factory
            self.session_factory = scoped_session(sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            ))
            
            # Bind metadata to engine
            self.metadata.bind = self.engine
            
            logger.info("Database engine setup complete")
            
        except Exception as e:
            logger.error("Failed to setup database engine: %s", str(e))
            raise
    
    @contextlib.contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations.
        
        Yields:
            Generator[Session, None, None]: SQLAlchemy session.
            
        Raises:
            Exception: If any database operation fails.
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Database operation failed: %s", str(e))
            raise
        finally:
            session.close()
    
    def create_tables(self) -> None:
        """
        Create all tables defined in the SQLAlchemy models.
        """
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Tables created successfully")
            
            # If using TimescaleDB, create hypertables
            if USE_TIMESCALEDB:
                self._setup_timescaledb()
                
        except Exception as e:
            logger.error("Failed to create tables: %s", str(e))
            raise
    
    def _setup_timescaledb(self) -> None:
        """
        Set up TimescaleDB hypertables for time-series data.
        """
        with self.session_scope() as session:
            # Check if TimescaleDB extension is installed
            result = session.execute(text("SELECT installed_version FROM pg_available_extensions WHERE name = 'timescaledb'"))
            if result.scalar() is None:
                logger.warning("TimescaleDB extension is not available in the database")
                return
                
            # Create TimescaleDB extension if not already created
            session.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
            
            # Convert time-series tables to hypertables
            # These will be applied to tables with a "time" column
            for table_name in ["events", "metrics", "user_activities"]:
                try:
                    # Check if table exists and is not already a hypertable
                    exists_query = text(f"""
                        SELECT EXISTS (
                            SELECT FROM pg_tables 
                            WHERE schemaname = 'public' 
                            AND tablename = '{table_name}'
                        )
                    """)
                    if not session.execute(exists_query).scalar():
                        logger.info("Table %s does not exist yet, skipping hypertable conversion", table_name)
                        continue
                        
                    # Check if already a hypertable
                    hypertable_query = text(f"""
                        SELECT EXISTS (
                            SELECT FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = '{table_name}'
                        )
                    """)
                    if session.execute(hypertable_query).scalar():
                        logger.info("Table %s is already a hypertable", table_name)
                        continue
                    
                    # Convert to hypertable
                    session.execute(text(f"""
                        SELECT create_hypertable(
                            '{table_name}', 'time',
                            chunk_time_interval => interval '{TIMESCALE_CHUNK_INTERVAL}'
                        )
                    """))
                    
                    logger.info("Converted table %s to a TimescaleDB hypertable", table_name)
                    
                except Exception as e:
                    logger.error("Failed to convert %s to hypertable: %s", table_name, str(e))
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a raw SQL query and return the results as a list of dictionaries.
        
        Args:
            query (str): SQL query string.
            params (Optional[Dict[str, Any]]): Parameters for the query.
            
        Returns:
            List[Dict[str, Any]]: Query results as a list of dictionaries.
        """
        with self.session_scope() as session:
            try:
                result = session.execute(text(query), params or {})
                rows = result.fetchall()
                
                # Convert result rows to dictionaries
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
                
            except Exception as e:
                logger.error("Failed to execute query: %s", str(e))
                raise
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple rows into a table.
        
        Args:
            table_name (str): Name of the table.
            data (List[Dict[str, Any]]): List of dictionaries representing rows to insert.
            
        Returns:
            int: Number of rows inserted.
        """
        if not data:
            logger.warning("No data provided for bulk insert into %s", table_name)
            return 0
            
        with self.session_scope() as session:
            try:
                # Get table reference
                table = Table(table_name, self.metadata, autoload_with=self.engine)
                
                # Insert data
                result = session.execute(table.insert(), data)
                rowcount = result.rowcount
                
                logger.info("Inserted %d rows into %s", rowcount, table_name)
                return rowcount
                
            except Exception as e:
                logger.error("Failed to bulk insert into %s: %s", table_name, str(e))
                raise
    
    def upsert(self, table_name: str, data: Dict[str, Any], 
              conflict_columns: List[str], update_columns: Optional[List[str]] = None) -> bool:
        """
        Insert or update a row in a table (upsert operation).
        
        Args:
            table_name (str): Name of the table.
            data (Dict[str, Any]): Dictionary representing the row data.
            conflict_columns (List[str]): Columns to check for conflicts (unique constraint).
            update_columns (Optional[List[str]]): Columns to update on conflict.
                If None, all columns except conflict columns will be updated.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        with self.session_scope() as session:
            try:
                # Get table reference
                table = Table(table_name, self.metadata, autoload_with=self.engine)
                
                # If update_columns not specified, use all columns except conflict columns
                if update_columns is None:
                    update_columns = [c.name for c in table.columns if c.name not in conflict_columns]
                
                # Build the upsert statement
                stmt = postgresql_upsert(table, data, conflict_columns, update_columns)
                
                # Execute the statement
                session.execute(stmt)
                
                logger.info("Upserted row in %s with conflict on %s", table_name, ", ".join(conflict_columns))
                return True
                
            except Exception as e:
                logger.error("Failed to upsert in %s: %s", table_name, str(e))
                raise
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the database connection.
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        try:
            with self.engine.connect():
                # Get PostgreSQL version
                with self.session_scope() as session:
                    version = session.execute(text("SELECT version()")).scalar()
                    
                return True, f"Connection successful. Database version: {version}"
        except Exception as e:
            error_message = str(e)
            logger.error("Database connection test failed: %s", error_message)
            return False, f"Connection failed: {error_message}"


def postgresql_upsert(table: Table, data: Dict[str, Any], 
                     conflict_columns: List[str], update_columns: List[str]):
    """
    Create a PostgreSQL upsert statement.
    
    Args:
        table (Table): SQLAlchemy Table object.
        data (Dict[str, Any]): Row data.
        conflict_columns (List[str]): Columns to check for conflicts.
        update_columns (List[str]): Columns to update on conflict.
        
    Returns:
        sqlalchemy.sql.expression: Upsert statement.
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    
    # Create insert statement
    insert_stmt = pg_insert(table).values(**data)
    
    # Create on_conflict_do_update statement
    update_dict = {col: insert_stmt.excluded[col] for col in update_columns}
    
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_dict
    )
    
    return upsert_stmt
