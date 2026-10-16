@echo off
REM Start ATB BI Airflow — survives PC reboot when Docker Desktop auto-starts
cd /d "%~dp0"

echo [1/3] Starting Postgres first...
docker compose up -d postgres
echo Waiting for Postgres to be healthy...
:wait_pg
docker inspect atb-airflow-postgres --format "{{.State.Health.Status}}" 2>nul | findstr /i "healthy" >nul
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    goto wait_pg
)
echo Postgres is healthy.

echo [2/3] Running Airflow DB init/migrate (if needed)...
docker compose up airflow-init

echo [3/3] Starting Airflow services...
docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer

echo.
echo ========================================
echo  Airflow UI: http://localhost:8080
echo  Login: admin / admin
echo  Main DAG: atb_master_pipeline
echo ========================================
docker compose ps
pause
