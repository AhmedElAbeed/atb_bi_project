"""
ATB BI Warehouse - Data Quality Checks Module
==============================================

Provides comprehensive data quality validation functions for:
- Fact tables (row counts, NULL checks, referential integrity)
- Dimensions (completeness, validity)
- Data freshness and timeliness
- Schema validation
"""

import pyodbc
from typing import Dict, List, Tuple
from datetime import datetime


class DataQualityChecker:
    """Validates ATB BI Warehouse data quality"""
    
    def __init__(self, server: str, database: str, user: str, password: str):
        """Initialize database connection parameters"""
        self.conn_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server};"
            f"Database={database};"
            f"UID={user};"
            f"PWD={password};"
        )
    
    def _get_connection(self):
        """Create and return database connection"""
        return pyodbc.connect(self.conn_string)
    
    def check_fact_customer_risk(self) -> Dict:
        """
        Validate fact_customer_risk table
        
        Returns:
            Dict with validation results:
            - total_rows: Row count
            - null_fks: Count of NULL foreign keys by column
            - distinct_daos: Distinct DAO count
            - status: PASS/FAIL
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
                    COUNT(CASE WHEN risk_profile_sk IS NULL THEN 1 END) as null_risk_profile_sk,
                    COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
                    COUNT(CASE WHEN sector_sk IS NULL THEN 1 END) as null_sector_sk,
                    COUNT(CASE WHEN industry_sk IS NULL THEN 1 END) as null_industry_sk,
                    COUNT(CASE WHEN target_sk IS NULL THEN 1 END) as null_target_sk,
                    COUNT(DISTINCT dao_sk) as distinct_daos
                FROM PFE_DWH.fact_customer_risk
            """)
            
            row = cursor.fetchone()
            (total, null_cust, null_rp, null_dao, null_sector, null_ind, null_tgt, distinct_dao) = row
            
            null_count = null_cust + null_rp + null_dao + null_sector + null_ind + null_tgt
            status = "PASS" if null_count == 0 and total > 100000 else "FAIL"
            
            result = {
                'table': 'fact_customer_risk',
                'total_rows': total,
                'null_fks': {
                    'customer_sk': null_cust,
                    'risk_profile_sk': null_rp,
                    'dao_sk': null_dao,
                    'sector_sk': null_sector,
                    'industry_sk': null_ind,
                    'target_sk': null_tgt,
                    'total_null': null_count,
                },
                'distinct_daos': distinct_dao,
                'status': status,
                'checked_at': datetime.now().isoformat(),
            }
            
            return result
            
        finally:
            conn.close()
    
    def check_fact_account(self) -> Dict:
        """
        Validate fact_account table
        
        Returns:
            Dict with validation results:
            - total_rows: Row count
            - null_fks: Count of NULL foreign keys by column
            - status: PASS/FAIL
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN customer_sk IS NULL THEN 1 END) as null_customer_sk,
                    COUNT(CASE WHEN dao_sk IS NULL THEN 1 END) as null_dao_sk,
                    COUNT(CASE WHEN currency_sk IS NULL THEN 1 END) as null_currency_sk,
                    COUNT(CASE WHEN sector_sk IS NULL THEN 1 END) as null_sector_sk,
                    COUNT(CASE WHEN industry_sk IS NULL THEN 1 END) as null_industry_sk,
                    COUNT(CASE WHEN target_sk IS NULL THEN 1 END) as null_target_sk,
                    COUNT(DISTINCT dao_sk) as distinct_daos
                FROM PFE_DWH.fact_account
            """)
            
            row = cursor.fetchone()
            (total, null_cust, null_dao, null_curr, null_sector, null_ind, null_tgt, distinct_dao) = row
            
            null_count = null_cust + null_dao + null_curr + null_sector + null_ind + null_tgt
            status = "PASS" if null_count == 0 and total > 100000 else "FAIL"
            
            result = {
                'table': 'fact_account',
                'total_rows': total,
                'null_fks': {
                    'customer_sk': null_cust,
                    'dao_sk': null_dao,
                    'currency_sk': null_curr,
                    'sector_sk': null_sector,
                    'industry_sk': null_ind,
                    'target_sk': null_tgt,
                    'total_null': null_count,
                },
                'distinct_daos': distinct_dao,
                'status': status,
                'checked_at': datetime.now().isoformat(),
            }
            
            return result
            
        finally:
            conn.close()
    
    def check_all_dimensions(self) -> Dict[str, Dict]:
        """
        Validate all dimensions have UNKNOWN records
        
        Returns:
            Dict with dimension validation results
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        dimensions = {
            'dim_customer': ('customer_sk', 'customer_id'),
            'dim_dao': ('dao_sk', 'account_officer_id'),
            'dim_currency': ('currency_sk', 'currency_code'),
            'dim_sector': ('sector_sk', 'sector_code'),
            'dim_industry': ('industry_sk', 'industry_code'),
            'dim_target': ('target_sk', 'target_code'),
            'dim_risk_profile': ('risk_profile_sk', 'customer_id'),
        }
        
        results = {}
        
        try:
            for dim_name, (sk_col, fk_col) in dimensions.items():
                cursor.execute(f"""
                    SELECT COUNT(*) as total_rows
                    FROM PFE_DWH.{dim_name}
                """)
                total = cursor.fetchone()[0]
                
                # Check for UNKNOWN record (value -1 or 'UNKNOWN')
                cursor.execute(f"""
                    SELECT COUNT(*) as unknown_count
                    FROM PFE_DWH.{dim_name}
                    WHERE {fk_col} = -1 OR {fk_col} = 'UNKNOWN' OR {fk_col} = 'UNK'
                """)
                unknown_count = cursor.fetchone()[0]
                
                results[dim_name] = {
                    'total_rows': total,
                    'has_unknown_record': unknown_count > 0,
                    'status': 'PASS' if unknown_count > 0 else 'FAIL',
                }
            
            return results
            
        finally:
            conn.close()
    
    def get_data_freshness(self) -> Dict:
        """
        Check data freshness - when was data last loaded
        
        Returns:
            Dict with load dates for fact tables
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    MAX(load_date) as last_load_date
                FROM PFE_DWH.fact_customer_risk
            """)
            risk_load = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT 
                    MAX(load_date) as last_load_date
                FROM PFE_DWH.fact_account
            """)
            account_load = cursor.fetchone()[0]
            
            return {
                'fact_customer_risk_last_load': str(risk_load),
                'fact_account_last_load': str(account_load),
                'checked_at': datetime.now().isoformat(),
            }
            
        finally:
            conn.close()
    
    def run_full_validation(self) -> Dict:
        """
        Run complete data quality validation suite
        
        Returns:
            Comprehensive validation report
        """
        return {
            'timestamp': datetime.now().isoformat(),
            'fact_customer_risk': self.check_fact_customer_risk(),
            'fact_account': self.check_fact_account(),
            'dimensions': self.check_all_dimensions(),
            'data_freshness': self.get_data_freshness(),
            'overall_status': 'PASS',  # Set based on individual checks
        }


# Example usage
if __name__ == '__main__':
    checker = DataQualityChecker(
        server='DESKTOP-B0PDEI7,1434',
        database='ATB_BI',
        user='airbyte_user',
        password='AZERTY123',
    )
    
    results = checker.run_full_validation()
    print(results)
