@echo off
echo ============================================
echo   ATB BI Dashboard - Streamlit Launcher
echo ============================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again.
    pause
    exit /b 1
)

echo [1/4] Python found!
echo.

REM Check if .env file exists
if not exist ".env" (
    echo WARNING: .env file not found!
    echo Please create a .env file with your SQL Server credentials.
    echo.
    echo Example .env file:
    echo   ATB_SQL_SERVER=DESKTOP-B0PDEI7
    echo   ATB_SQL_DATABASE=ATB_BI
    echo   ATB_SQL_USERNAME=airbyte_user
    echo   ATB_SQL_PASSWORD=AZERTY123
    echo   ATB_SQL_PORT=1434
    echo   ATB_SQL_DRIVER=ODBC Driver 18 for SQL Server
    echo   ATB_DASHBOARD_SOURCE=PFE_DWH.fact_customer_risk
    echo   ATB_DASHBOARD_TOP_N=10000
    echo   MLFLOW_TRACKING_URI=file:///C:/Users/Ahmed/Desktop/atb_bi_project/4_ml/mlruns
    echo   ATB_MLFLOW_EXPERIMENT=atb_bi_customer_risk
    echo   ATB_AIRBYTE_URL=http://localhost:8006
    echo   ATB_AIRBYTE_UI_URL=http://localhost:8000
    echo.
    pause
)

echo [2/4] Configuration found!
echo.

REM Install dependencies if needed
echo [3/4] Checking dependencies...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo [4/4] Dependencies ready!
echo.
echo ============================================
echo   Starting ATB BI Dashboard...
echo   Access the dashboard at: http://localhost:8501
echo   Press Ctrl+C to stop the server
echo ============================================
echo.

REM Run Streamlit
streamlit run app.py --server.headless=true --server.address=0.0.0.0 --server.port=8501

pause