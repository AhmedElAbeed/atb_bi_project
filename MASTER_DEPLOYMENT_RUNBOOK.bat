@echo off
REM ATB BI Warehouse - Master Deployment Runbook (Windows)
REM Complete end-to-end deployment guide for Phases 1-3
REM ====================================================

echo.
echo ==========================================
echo ATB BI WAREHOUSE - MASTER DEPLOYMENT RUNBOOK
echo ==========================================
echo.

REM Step 1: Environment Setup
echo [1/8] Setting up Python environment...
python -m venv venv
call venv\Scripts\activate.bat
echo ^✓ Virtual environment created

REM Step 2: Install Dependencies
echo.
echo [2/8] Installing dependencies...
python -m pip install --upgrade pip
pip install -r 2_orchestration\requirements.txt
echo ^✓ Dependencies installed

REM Step 3: Validate DAG Files
echo.
echo [3/8] Validating DAG files...
python -m py_compile 2_orchestration\dags\atb_bi_warehouse_etl.py
python -m py_compile 2_orchestration\dags\data_quality_checks.py
echo ^✓ DAG syntax valid

REM Step 4: Run Deployment Validator
echo.
echo [4/8] Running deployment validator...
cd 2_orchestration
python deploy_and_test.py
cd ..
echo ^✓ Deployment validation complete

REM Step 5: Initialize Airflow
echo.
echo [5/8] Initializing Airflow database...
set AIRFLOW_HOME=%CD%\2_orchestration
airflow db init
echo ^✓ Airflow database initialized

REM Step 6: Create Admin User
echo.
echo [6/8] Creating Airflow admin user...
airflow users create ^
  --username admin ^
  --firstname Administrator ^
  --lastname User ^
  --role Admin ^
  --email admin@atb.local ^
  --password admin123
echo ^✓ Admin user created

REM Step 7: Configure Airflow Variables
echo.
echo [7/8] Configuring Airflow variables...
for /f %%i in ('cd') do set CURR_DIR=%%i
airflow variables set DBT_PROJECT_DIR "%CURR_DIR%\3_transformation"
airflow variables set DBT_PROFILE "atb_bi_transformation"
airflow variables set DBT_TARGET "prod"
echo ^✓ Airflow variables set

REM Step 8: Display Next Steps
echo.
echo [8/8] Deployment complete!
echo ==========================================
echo.
echo Next steps to start the pipeline:
echo.
echo Terminal 1: Start Airflow Scheduler
echo   set AIRFLOW_HOME=%CD%\2_orchestration
echo   airflow scheduler
echo.
echo Terminal 2: Start Airflow Web Server
echo   set AIRFLOW_HOME=%CD%\2_orchestration
echo   airflow webserver --port 8080
echo.
echo Then access the Web UI at:
echo   http://localhost:8080
echo.
echo Username: admin
echo Password: admin123
echo.
echo ==========================================
echo.
pause
