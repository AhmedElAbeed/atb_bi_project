"""
SQL Server database utilities for data validation and quality checks
"""
import pyodbc
import logging
from typing import Dict, List, Optional
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)


class SQLServerConnection:
    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "ODBC Driver 17 for SQL Server"
    ):
        # Use environment variables if not provided
        self.server = server or os.getenv("SQL_SERVER_HOST", "host.docker.internal")
        self.database = database or os.getenv("SQL_SERVER_DATABASE", "ATB_BI")
        self.username = username or os.getenv("SQL_SERVER_USER", "airbyte_user")
        self.password = password or os.getenv("SQL_SERVER_PASSWORD", "AZERTY123")
        self.driver = driver
        self.port = os.getenv("SQL_SERVER_PORT", "1434")
        
    def get_connection_string(self) -> str:
        """Build ODBC connection string"""
        if self.password:
            return (
                f"Driver={{{self.driver}}};"
                f"Server={self.server},{self.port};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
            )
        else:
            # Trusted connection (Windows auth)
            return (
                f"Driver={{{self.driver}}};"
                f"Server={self.server},{self.port};"
                f"Database={self.database};"
                f"Trusted_Connection=yes;"
            )

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn_string = self.get_connection_string()
            conn = pyodbc.connect(conn_string, timeout=10)
            yield conn
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str) -> List[tuple]:
        """Execute a SELECT query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_script(self, script: str) -> None:
        """Execute a SQL script (DDL, DML)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(script)
                conn.commit()
                logger.info("Script executed successfully")
            except pyodbc.Error as e:
                conn.rollback()
                logger.error(f"Script execution error: {e}")
                raise
            finally:
                cursor.close()

    def get_row_count(self, schema: str, table: str) -> int:
        """Get row count for a table"""
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def validate_table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists"""
        query = (
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        )
        result = self.execute_query(query)
        return len(result) > 0

    def get_column_info(self, schema: str, table: str) -> Dict:
        """Get column information for a table"""
        query = (
            f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        results = self.execute_query(query)
        return {row[0]: {"type": row[1], "nullable": row[2]} for row in results}

    def get_table_stats(self, schema: str, table: str) -> Dict:
        """Get comprehensive stats for a table"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                stats = {}
                
                # Row count
                cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
                stats["row_count"] = cursor.fetchone()[0]
                
                # Table size
                cursor.execute(f"""
                    SELECT 
                        SUM(s.used_page_count) * 8 / 1024 AS size_mb
                    FROM sys.dm_db_partition_stats s
                    JOIN sys.tables t ON s.object_id = t.object_id
                    WHERE t.name = '{table}'
                """)
                result = cursor.fetchone()
                stats["size_mb"] = result[0] if result and result[0] else 0
                
                # Last modified
                cursor.execute(f"""
                    SELECT MAX(s.modify_date)
                    FROM sys.dm_db_partition_stats s
                    JOIN sys.tables t ON s.object_id = t.object_id
                    WHERE t.name = '{table}'
                """)
                result = cursor.fetchone()
                stats["last_modified"] = str(result[0]) if result and result[0] else None
                
                return stats
            finally:
                cursor.close()


# Expected row counts for validation (adjust based on your data)
EXPECTED_ROW_COUNTS = {
    "PFE_ODS": {
        "ACCOUNT": {"min": 1000, "max": 1000000},
        "CUSTOMER": {"min": 500, "max": 500000},
        "CURRENCY": {"min": 1, "max": 1000},
        "DAO": {"min": 1, "max": 10000},
        "INDUSTRY": {"min": 1, "max": 1000},
        "SECTOR": {"min": 1, "max": 1000},
        "TARGET": {"min": 1, "max": 10000},
    }
}


def validate_ods_tables() -> bool:
    """Validate that all ODS tables are populated with expected row counts"""
    db = SQLServerConnection()
    
    all_valid = True
    for table_name, bounds in EXPECTED_ROW_COUNTS["PFE_ODS"].items():
        try:
            if not db.validate_table_exists("PFE_ODS", table_name):
                logger.error(f"Table {table_name} does not exist in PFE_ODS")
                all_valid = False
                continue
            
            count = db.get_row_count("PFE_ODS", table_name)
            
            if bounds["min"] <= count <= bounds["max"]:
                logger.info(f"✓ {table_name}: {count} rows (valid)")
            else:
                logger.warning(f"✗ {table_name}: {count} rows (expected {bounds['min']}-{bounds['max']})")
                all_valid = False
                
        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            all_valid = False
    
    return all_valid


def validate_dwh_tables() -> bool:
    """Validate that DWH tables are populated"""
    db = SQLServerConnection()
    
    required_tables = {
        "PFE_DWH": [
            "DIM_CUSTOMER", "DIM_BRANCH", "DIM_DATE", "DIM_SECTOR",
            "DIM_INDUSTRY", "DIM_CURRENCY", "DIM_TARGET", "DIM_RISK_PROFILE",
            "FAIT_ACCOUNT", "FAIT_CUSTOMER_RISK"
        ]
    }
    
    all_valid = True
    for table_name in required_tables["PFE_DWH"]:
        try:
            if not db.validate_table_exists("PFE_DWH", table_name):
                logger.error(f"Table {table_name} does not exist in PFE_DWH")
                all_valid = False
                continue
            
            count = db.get_row_count("PFE_DWH", table_name)
            if count > 0:
                logger.info(f"✓ {table_name}: {count} rows")
            else:
                logger.warning(f"✗ {table_name}: {count} rows (empty)")
                all_valid = False
                
        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            all_valid = False
    
    return all_valid
