FROM python:3.11-slim

WORKDIR /app

# Install ODBC and Microsoft SQL Server driver.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg2 ca-certificates apt-transport-https unixodbc-dev build-essential \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 msodbcsql17 \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Build with repo root as context so app can access 4_ml artifacts.
COPY 5_reporting/streamlit/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt \
    && python -c "import plotly; import streamlit; import pandas; print('streamlit deps OK')"

COPY 5_reporting/streamlit /app/5_reporting/streamlit
COPY 4_ml /app/4_ml
COPY logo /app/logo

WORKDIR /app/5_reporting/streamlit
EXPOSE 8501
ENV STREAMLIT_SERVER_HEADLESS=true
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
