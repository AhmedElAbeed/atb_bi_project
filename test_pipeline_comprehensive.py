"""
ATB BI Pipeline Comprehensive Validation Suite

Tests all components of the end-to-end pipeline:
- Airflow DAG validation
- Database connectivity
- Feature loading
- Model artifacts
- Orchestration structure
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent
ML_SRC = PROJECT_ROOT / "4_ml" / "src"
DAGS_DIR = PROJECT_ROOT / "2_orchestration" / "dags"

sys.path.insert(0, str(ML_SRC))
sys.path.insert(0, str(PROJECT_ROOT))


class PipelineValidator:
    """Comprehensive pipeline validation suite."""
    
    def __init__(self):
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "tests": [],
            "summary": {"passed": 0, "failed": 0, "warnings": 0},
            "component_status": {}
        }
    
    def log_test(self, name: str, status: str, message: str, details: Dict = None):
        """Log test result."""
        test_result = {
            "name": name,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "details": details or {}
        }
        self.results["tests"].append(test_result)
        
        status_icon = "✓" if status == "PASS" else "✗" if status == "FAIL" else "⚠"
        logger.info(f"{status_icon} [{status}] {name}: {message}")
        
        if status == "PASS":
            self.results["summary"]["passed"] += 1
        elif status == "FAIL":
            self.results["summary"]["failed"] += 1
        else:
            self.results["summary"]["warnings"] += 1
    
    # =========================================================================
    # SECTION 1: Airflow DAG Validation
    # =========================================================================
    
    def test_dag_syntax(self):
        """Validate all DAG Python syntax."""
        logger.info("\n" + "="*70)
        logger.info("SECTION 1: Airflow DAG Validation")
        logger.info("="*70)
        
        dag_files = [
            ("atb_bi_warehouse_etl.py", "Warehouse ETL DAG"),
            ("atb_ml_orchestration.py", "ML Pipeline DAG"),
            ("atb_master_ml_integration_dag.py", "Master Integration DAG")
        ]
        
        for dag_file, description in dag_files:
            dag_path = DAGS_DIR / dag_file
            
            if not dag_path.exists():
                self.log_test(
                    f"DAG File Exists: {dag_file}",
                    "FAIL",
                    f"DAG file not found: {dag_path}",
                    {"path": str(dag_path)}
                )
                continue
            
            # Check syntax by compiling
            try:
                with open(dag_path, 'r') as f:
                    code = f.read()
                compile(code, str(dag_path), 'exec')
                
                self.log_test(
                    f"DAG Syntax: {dag_file}",
                    "PASS",
                    f"{description} - Python syntax valid",
                    {"file_size": len(code), "lines": len(code.split('\n'))}
                )
            except SyntaxError as e:
                self.log_test(
                    f"DAG Syntax: {dag_file}",
                    "FAIL",
                    f"Syntax error in {dag_file}: {e}",
                    {"error": str(e)}
                )
    
    def test_dag_imports(self):
        """Validate DAG imports."""
        logger.info("\nValidating DAG imports...")
        
        dag_files = [
            ("atb_bi_warehouse_etl.py", "ETL"),
            ("atb_ml_orchestration.py", "ML"),
            ("atb_master_ml_integration_dag.py", "Integration")
        ]
        
        for dag_file, label in dag_files:
            dag_path = DAGS_DIR / dag_file
            
            try:
                with open(dag_path, 'r') as f:
                    content = f.read()
                
                # Check for required imports
                required_imports = {
                    'from airflow': 'Airflow core',
                    'from datetime import': 'Datetime module',
                }
                
                missing = []
                for imp, desc in required_imports.items():
                    if imp not in content:
                        missing.append(desc)
                
                if missing:
                    self.log_test(
                        f"DAG Imports: {label}",
                        "WARNING",
                        f"Missing imports: {', '.join(missing)}",
                        {"dag": dag_file, "missing": missing}
                    )
                else:
                    self.log_test(
                        f"DAG Imports: {label}",
                        "PASS",
                        f"All required imports present"
                    )
            except Exception as e:
                self.log_test(
                    f"DAG Imports: {label}",
                    "FAIL",
                    f"Error checking imports: {e}"
                )
    
    # =========================================================================
    # SECTION 2: Database Connectivity
    # =========================================================================
    
    def test_database_connection(self):
        """Test SQL Server connection."""
        logger.info("\n" + "="*70)
        logger.info("SECTION 2: Database Connectivity")
        logger.info("="*70)
        
        try:
            from data_access import make_engine, WarehouseConnection
            
            logger.info("Testing database connection...")
            conn_config = WarehouseConnection()
            
            self.log_test(
                "Warehouse Config",
                "PASS",
                f"Connection config loaded",
                {
                    "server": conn_config.server,
                    "database": conn_config.database,
                    "port": conn_config.port,
                    "schema": conn_config.schema
                }
            )
            
            # Test connection
            engine = make_engine()
            
            with engine.connect() as conn:
                result = conn.execute("SELECT 1 as test_value")
                conn.commit()
            
            self.log_test(
                "SQL Server Connection",
                "PASS",
                f"Successfully connected to {conn_config.server}",
                {"database": conn_config.database}
            )
        except Exception as e:
            self.log_test(
                "SQL Server Connection",
                "FAIL",
                f"Database connection failed: {e}",
                {"error": str(e)}
            )
    
    def test_warehouse_tables(self):
        """Verify warehouse tables exist."""
        logger.info("\nVerifying warehouse tables...")
        
        try:
            from data_access import make_engine, WarehouseConnection
            
            engine = make_engine()
            conn_config = WarehouseConnection()
            schema = conn_config.schema
            
            required_tables = [
                "dim_customer",
                "dim_dao",
                "dim_currency",
                "dim_sector",
                "dim_industry",
                "dim_target",
                "dim_risk_profile",
                "dim_date",
                "fact_customer_risk",
                "fact_account"
            ]
            
            with engine.connect() as conn:
                found_tables = []
                missing_tables = []
                
                for table in required_tables:
                    query = f"""
                    SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                    """
                    result = conn.execute(query)
                    exists = result.fetchone()[0] > 0
                    
                    if exists:
                        found_tables.append(table)
                    else:
                        missing_tables.append(table)
            
            if missing_tables:
                self.log_test(
                    "Warehouse Tables",
                    "FAIL",
                    f"Missing tables: {', '.join(missing_tables)}",
                    {"found": len(found_tables), "missing": len(missing_tables)}
                )
            else:
                self.log_test(
                    "Warehouse Tables",
                    "PASS",
                    f"All {len(found_tables)} required tables found",
                    {"tables": found_tables}
                )
        except Exception as e:
            self.log_test(
                "Warehouse Tables",
                "FAIL",
                f"Table verification failed: {e}"
            )
    
    def test_warehouse_row_counts(self):
        """Check row counts in fact tables."""
        logger.info("\nVerifying warehouse data...")
        
        try:
            from data_access import make_engine, WarehouseConnection
            
            engine = make_engine()
            conn_config = WarehouseConnection()
            schema = conn_config.schema
            
            tables_to_check = [
                ("fact_customer_risk", 100000),
                ("fact_account", 100000),
                ("dim_customer", 10000)
            ]
            
            with engine.connect() as conn:
                row_counts = {}
                
                for table, min_expected in tables_to_check:
                    query = f"SELECT COUNT(*) as cnt FROM {schema}.{table}"
                    result = conn.execute(query)
                    count = result.fetchone()[0]
                    row_counts[table] = count
                    
                    if count > min_expected:
                        self.log_test(
                            f"Data: {table}",
                            "PASS",
                            f"{count:,} rows found (expected > {min_expected:,})",
                            {"count": count}
                        )
                    else:
                        self.log_test(
                            f"Data: {table}",
                            "WARNING",
                            f"{count:,} rows found (expected > {min_expected:,})",
                            {"count": count}
                        )
        except Exception as e:
            self.log_test(
                "Warehouse Data",
                "FAIL",
                f"Row count check failed: {e}"
            )
    
    # =========================================================================
    # SECTION 3: Feature Engineering & ML
    # =========================================================================
    
    def test_feature_loading(self):
        """Test feature loading from warehouse."""
        logger.info("\n" + "="*70)
        logger.info("SECTION 3: Feature Engineering & ML Validation")
        logger.info("="*70)
        
        try:
            from data_access import load_feature_frame
            
            logger.info("Loading feature frame...")
            df = load_feature_frame()
            
            self.log_test(
                "Feature Loading",
                "PASS",
                f"Loaded {len(df):,} rows × {len(df.columns)} columns",
                {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "columns_list": list(df.columns)[:10]  # First 10
                }
            )
        except Exception as e:
            self.log_test(
                "Feature Loading",
                "FAIL",
                f"Feature loading failed: {e}",
                {"error": str(e)}
            )
    
    def test_feature_engineering(self):
        """Test feature engineering pipeline."""
        logger.info("\nTesting feature engineering...")
        
        try:
            from data_access import load_feature_frame
            from features import build_targets, engineer_features, select_feature_columns
            
            df = load_feature_frame()
            
            # Test target building
            df = build_targets(df, risk_threshold=50)
            if 'risk_target' in df.columns:
                self.log_test(
                    "Feature Engineering: Targets",
                    "PASS",
                    f"Target variable created - Distribution: {df['risk_target'].value_counts().to_dict()}",
                    {"target_distribution": df['risk_target'].value_counts().to_dict()}
                )
            else:
                self.log_test(
                    "Feature Engineering: Targets",
                    "FAIL",
                    "Target variable not found"
                )
            
            # Test feature engineering
            df = engineer_features(df)
            self.log_test(
                "Feature Engineering: Transformations",
                "PASS",
                f"Engineered {len(df.columns)} total columns",
                {"columns": len(df.columns)}
            )
            
            # Test feature selection
            feature_cols, numeric, categorical = select_feature_columns(df)
            self.log_test(
                "Feature Engineering: Selection",
                "PASS",
                f"Selected {len(feature_cols)} features ({len(numeric)} numeric, {len(categorical)} categorical)",
                {
                    "total_features": len(feature_cols),
                    "numeric": len(numeric),
                    "categorical": len(categorical)
                }
            )
        except Exception as e:
            self.log_test(
                "Feature Engineering",
                "FAIL",
                f"Feature engineering failed: {e}",
                {"error": str(e)}
            )
    
    def test_model_artifacts(self):
        """Verify model artifacts exist."""
        logger.info("\nVerifying model artifacts...")
        
        outputs_dir = PROJECT_ROOT / "4_ml" / "outputs"
        models_dir = PROJECT_ROOT / "4_ml" / "models"
        
        expected_artifacts = {
            "outputs": [
                "customer_risk_features.csv",
                "training_summary.json",
                "model_selection_conclusion.json"
            ],
            "notebooks": [
                "4_ml/notebooks/01_eda.ipynb",
                "4_ml/notebooks/03_model_training.ipynb",
                "4_ml/notebooks/05_model_selection_report.ipynb"
            ]
        }
        
        for artifact in expected_artifacts["outputs"]:
            artifact_path = outputs_dir / artifact
            if artifact_path.exists():
                self.log_test(
                    f"Artifact: {artifact}",
                    "PASS",
                    f"Found: {artifact_path}"
                )
            else:
                self.log_test(
                    f"Artifact: {artifact}",
                    "WARNING",
                    f"Not found (will be created during ML execution)"
                )
    
    # =========================================================================
    # SECTION 4: Pipeline Structure
    # =========================================================================
    
    def test_pipeline_structure(self):
        """Validate overall pipeline structure."""
        logger.info("\n" + "="*70)
        logger.info("SECTION 4: Pipeline Structure Validation")
        logger.info("="*70)
        
        required_dirs = {
            "1_ingestion": "Data ingestion layer",
            "2_orchestration": "Orchestration (Airflow)",
            "3_transformation": "dbt transformation",
            "4_ml": "ML pipeline",
            "5_reporting": "Reporting layer"
        }
        
        for dir_name, description in required_dirs.items():
            dir_path = PROJECT_ROOT / dir_name
            if dir_path.exists():
                self.log_test(
                    f"Directory: {dir_name}",
                    "PASS",
                    description
                )
            else:
                self.log_test(
                    f"Directory: {dir_name}",
                    "FAIL",
                    f"Missing directory: {dir_path}"
                )
        
        # Check critical files
        critical_files = {
            "2_orchestration/airflow.cfg": "Airflow configuration",
            "2_orchestration/requirements.txt": "Python dependencies",
            "3_transformation/dbt_project.yml": "dbt project",
            "4_ml/README.md": "ML documentation"
        }
        
        for file_path, description in critical_files.items():
            full_path = PROJECT_ROOT / file_path
            if full_path.exists():
                self.log_test(
                    f"File: {file_path}",
                    "PASS",
                    description
                )
            else:
                self.log_test(
                    f"File: {file_path}",
                    "FAIL",
                    f"Missing file: {full_path}"
                )
    
    def test_dependencies(self):
        """Verify Python dependencies."""
        logger.info("\nVerifying Python dependencies...")
        
        required_packages = [
            "pandas",
            "numpy",
            "scikit-learn",
            "xgboost",
            "mlflow",
            "sqlalchemy",
            "pyodbc",
            "airflow"
        ]
        
        for package in required_packages:
            try:
                __import__(package)
                self.log_test(
                    f"Package: {package}",
                    "PASS",
                    f"{package} is installed"
                )
            except ImportError:
                self.log_test(
                    f"Package: {package}",
                    "FAIL",
                    f"{package} is NOT installed"
                )
    
    # =========================================================================
    # SECTION 5: End-to-End Validation
    # =========================================================================
    
    def test_end_to_end_simulation(self):
        """Simulate end-to-end pipeline."""
        logger.info("\n" + "="*70)
        logger.info("SECTION 5: End-to-End Pipeline Simulation")
        logger.info("="*70)
        
        try:
            # Simulate ETL phase
            logger.info("Simulating ETL phase...")
            from data_access import load_feature_frame
            df = load_feature_frame()
            
            self.log_test(
                "ETL Phase: Feature Loading",
                "PASS",
                f"ETL phase can load {len(df):,} rows"
            )
            
            # Simulate ML phase
            logger.info("Simulating ML phase...")
            from features import build_targets, engineer_features, select_feature_columns
            from features import split_temporal, build_model_frame
            
            df = build_targets(df)
            df = engineer_features(df)
            df = build_model_frame(df)
            feature_cols, numeric, categorical = select_feature_columns(df)
            train, test = split_temporal(df)
            
            self.log_test(
                "ML Phase: Feature Engineering",
                "PASS",
                f"ML phase can process data - Train: {len(train)}, Test: {len(test)}"
            )
            
            # Test model preparation
            X_train = train[feature_cols]
            y_train = train["risk_target"].astype(int)
            
            self.log_test(
                "ML Phase: Data Preparation",
                "PASS",
                f"Training matrix prepared: {X_train.shape}"
            )
            
        except Exception as e:
            self.log_test(
                "End-to-End Simulation",
                "FAIL",
                f"Pipeline simulation failed: {e}",
                {"error": str(e)}
            )
    
    # =========================================================================
    # Main Execution
    # =========================================================================
    
    def run_all_tests(self):
        """Run complete validation suite."""
        logger.info("\n")
        logger.info("╔" + "="*68 + "╗")
        logger.info("║" + " "*15 + "ATB BI PIPELINE COMPREHENSIVE VALIDATION" + " "*13 + "║")
        logger.info("╚" + "="*68 + "╝")
        logger.info(f"Started: {datetime.now().isoformat()}\n")
        
        # Run all test sections
        self.test_dag_syntax()
        self.test_dag_imports()
        self.test_database_connection()
        self.test_warehouse_tables()
        self.test_warehouse_row_counts()
        self.test_feature_loading()
        self.test_feature_engineering()
        self.test_model_artifacts()
        self.test_pipeline_structure()
        self.test_dependencies()
        self.test_end_to_end_simulation()
        
        # Generate summary
        self.generate_report()
    
    def generate_report(self):
        """Generate validation report."""
        logger.info("\n" + "="*70)
        logger.info("VALIDATION SUMMARY")
        logger.info("="*70)
        
        summary = self.results["summary"]
        total = summary["passed"] + summary["failed"] + summary["warnings"]
        
        logger.info(f"\nTotal Tests: {total}")
        logger.info(f"✓ Passed:    {summary['passed']} ({100*summary['passed']/total:.1f}%)")
        logger.info(f"✗ Failed:    {summary['failed']} ({100*summary['failed']/total:.1f}%)")
        logger.info(f"⚠ Warnings:  {summary['warnings']} ({100*summary['warnings']/total:.1f}%)")
        
        # Determine overall status
        if summary["failed"] == 0:
            status = "🟢 PASSED" if summary["warnings"] == 0 else "🟡 PASSED WITH WARNINGS"
        else:
            status = "🔴 FAILED"
        
        logger.info(f"\nOverall Status: {status}")
        
        # Save report
        report_path = PROJECT_ROOT / "PIPELINE_VALIDATION_REPORT.json"
        with open(report_path, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"\nDetailed report saved to: {report_path}")
        
        # Print failed tests for action
        if summary["failed"] > 0:
            logger.info("\n❌ FAILED TESTS (Requires Action):")
            for test in self.results["tests"]:
                if test["status"] == "FAIL":
                    logger.info(f"  • {test['name']}: {test['message']}")
        
        logger.info("\n" + "="*70)
        return status


def main():
    """Run validation suite."""
    validator = PipelineValidator()
    validator.run_all_tests()


if __name__ == "__main__":
    main()
