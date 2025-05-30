FROM apache/airflow:3.0.1

#install dependencies for the python operator
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# If you need the Microsoft ODBC driver for SQL Server
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gnupg \
        curl \
        unixodbc-dev \
        gcc \
        python3-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
