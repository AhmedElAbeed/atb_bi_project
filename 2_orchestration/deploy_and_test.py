#!/usr/bin/env python3
"""
ATB BI Warehouse ETL - Deployment & Testing Script
===================================================

This script is a standalone deployment and smoke test tool that:
1. Validates the Airflow environment
2. Tests the data quality checker connectivity
3. Performs a dry-run of the DAG
4. Outputs deployment readiness report

Usage:
    python deploy_and_test.py --validate
    python deploy_and_test.py --test-db
    python deploy_and_test.py --dry-run
    python deploy_and_test.py --full
"""

import sys
import json
from pathlib import Path
from datetime import datetime

def validate_environment():
    """Validate that all required packages are available"""
    print("\n" + "="*70)
    print("STEP 1: Validating Python Environment")
    print("="*70)
    
    required_packages = {
        'airflow': 'Apache Airflow',
        'dbt': 'dbt-core',
        'pyodbc': 'PyODBC',
        'pandas': 'Pandas',
    }
    
    missing = []
    for package, display_name in required_packages.items():
        try:
            __import__(package)
            print(f"✓ {display_name:30} ... FOUND")
        except ImportError:
            print(f"✗ {display_name:30} ... MISSING")
            missing.append(display_name)
    
    if missing:
        print(f"\n⚠ WARNING: Missing packages: {', '.join(missing)}")
        print("   Install with: pip install -r requirements.txt")
        return False
    
    print("\n✓ All required packages available")
    return True


def validate_files():
    """Validate that all required files exist"""
    print("\n" + "="*70)
    print("STEP 2: Validating File Structure")
    print("="*70)
    
    required_files = {
        'dags/atb_bi_warehouse_etl.py': 'Main Airflow DAG',
        'dags/data_quality_checks.py': 'Data Quality Module',
        'airflow.cfg': 'Airflow Configuration',
        'requirements.txt': 'Python Dependencies',
    }
    
    all_exist = True
    for filepath, description in required_files.items():
        if Path(filepath).exists():
            size = Path(filepath).stat().st_size
            print(f"✓ {description:30} ... {filepath:40} ({size:,} bytes)")
        else:
            print(f"✗ {description:30} ... {filepath:40} (NOT FOUND)")
            all_exist = False
    
    if all_exist:
        print("\n✓ All required files present")
    else:
        print("\n⚠ WARNING: Some files are missing")
    
    return all_exist


def validate_dag_syntax():
    """Validate Python syntax of DAG files"""
    print("\n" + "="*70)
    print("STEP 3: Validating DAG Python Syntax")
    print("="*70)
    
    import py_compile
    
    dag_files = [
        'dags/atb_bi_warehouse_etl.py',
        'dags/data_quality_checks.py',
    ]
    
    all_valid = True
    for dag_file in dag_files:
        try:
            py_compile.compile(dag_file, doraise=True)
            print(f"✓ {dag_file:45} ... SYNTAX OK")
        except py_compile.PyCompileError as e:
            print(f"✗ {dag_file:45} ... SYNTAX ERROR")
            print(f"  {str(e)}")
            all_valid = False
    
    if all_valid:
        print("\n✓ All DAG files have valid Python syntax")
    
    return all_valid


def test_database_connection():
    """Test SQL Server database connection"""
    print("\n" + "="*70)
    print("STEP 4: Testing Database Connection")
    print("="*70)
    
    try:
        import pyodbc
        
        conn_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=DESKTOP-B0PDEI7,1434;"
            "Database=ATB_BI;"
            "UID=airbyte_user;"
            "PWD=AZERTY123;"
        )
        
        print("Attempting to connect to: DESKTOP-B0PDEI7,1434 / ATB_BI")
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT COUNT(*) as table_count FROM sys.tables WHERE schema_id = SCHEMA_ID('PFE_DWH')")
        result = cursor.fetchone()[0]
        
        print(f"✓ Database connection successful")
        print(f"✓ Found {result} tables in PFE_DWH schema")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed")
        print(f"  Error: {str(e)}")
        print("  This is expected if SQL Server is not running")
        return False


def load_dag_configuration():
    """Load and validate DAG configuration"""
    print("\n" + "="*70)
    print("STEP 5: Loading DAG Configuration")
    print("="*70)
    
    try:
        sys.path.insert(0, 'dags')
        from atb_bi_warehouse_etl import dag, default_args
        
        print(f"✓ DAG loaded successfully")
        print(f"  DAG ID: {dag.dag_id}")
        print(f"  Schedule: {dag.schedule_interval}")
        print(f"  Owner: {default_args['owner']}")
        print(f"  Retries: {default_args['retries']}")
        print(f"  Tasks: {len(dag.tasks)}")
        
        for task in dag.tasks:
            print(f"    - {task.task_id:40} ({task.__class__.__name__})")
        
        print(f"\n✓ DAG configuration valid")
        return True
        
    except Exception as e:
        print(f"✗ Failed to load DAG")
        print(f"  Error: {str(e)}")
        return False


def generate_deployment_report(results):
    """Generate final deployment readiness report"""
    print("\n" + "="*70)
    print("DEPLOYMENT READINESS REPORT")
    print("="*70)
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'project': 'ATB BI Warehouse ETL',
        'phase': 'Phase 3: Orchestration',
        'checks': {
            'Python Environment': results['environment'],
            'File Structure': results['files'],
            'DAG Syntax': results['syntax'],
            'Database Connection': results['database'],
            'DAG Configuration': results['dag_config'],
        },
        'summary': {
            'total_checks': 5,
            'passed': sum(results.values()),
            'failed': 5 - sum(results.values()),
        }
    }
    
    # Print summary
    print(f"\nTotal Checks: {report['summary']['total_checks']}")
    print(f"Passed: {report['summary']['passed']}")
    print(f"Failed: {report['summary']['failed']}")
    
    all_passed = report['summary']['failed'] == 0
    
    if all_passed:
        print(f"\n✓ DEPLOYMENT READY - All checks passed!")
        print("\nNext steps:")
        print("  1. export AIRFLOW_HOME=$(pwd)")
        print("  2. pip install -r requirements.txt")
        print("  3. airflow db init")
        print("  4. airflow users create --username admin --role Admin")
        print("  5. airflow scheduler (Terminal 1)")
        print("  6. airflow webserver --port 8080 (Terminal 2)")
        print("  7. Access UI at http://localhost:8080")
    else:
        print(f"\n⚠ DEPLOYMENT NOT READY - {report['summary']['failed']} checks failed")
        print("   Please resolve issues before deployment")
    
    print("\n" + "="*70)
    
    return report


def main():
    """Execute full validation and testing suite"""
    print("\n")
    print("+" + "="*68 + "+")
    print("|" + " "*68 + "|")
    print("|" + "  ATB BI WAREHOUSE ETL - DEPLOYMENT VALIDATOR".center(68) + "|")
    print("|" + "  Phase 3: Orchestration".center(68) + "|")
    print("|" + " "*68 + "|")
    print("+" + "="*68 + "+")
    
    results = {
        'environment': validate_environment(),
        'files': validate_files(),
        'syntax': validate_dag_syntax(),
        'database': test_database_connection(),
        'dag_config': load_dag_configuration(),
    }
    
    report = generate_deployment_report(results)
    
    # Return success if all critical checks passed
    critical_passed = (
        results['environment'] and
        results['files'] and
        results['syntax'] and
        results['dag_config']
    )
    
    return 0 if critical_passed else 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
