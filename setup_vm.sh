#!/usr/bin/env bash
# ============================================================
# One-time GCP VM setup script
# OS: Ubuntu 22.04 / Debian 11+
# Usage: bash setup_vm.sh
# ============================================================
set -euo pipefail

AIRFLOW_HOME="${HOME}/airflow"

echo "=== [1/5] Install Java 17 ==="
sudo apt-get update -q
sudo apt-get install -y openjdk-17-jdk
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc

echo "=== [2/5] Install Python packages ==="
pip install --upgrade pip
pip install \
    apache-airflow==2.9.3 \
    pyspark==3.5.1 \
    apache-sedona==1.8.1 \
    pandas pyarrow \
    "snowflake-connector-python[pandas]"

echo "=== [3/5] Initialize Airflow ==="
export AIRFLOW_HOME="${AIRFLOW_HOME}"
echo "export AIRFLOW_HOME=${AIRFLOW_HOME}" >> ~/.bashrc

airflow db migrate

# Create admin account (first-time only)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password project405_team9!

echo "=== [4/5] Configure dags_folder ==="
# Point Airflow directly at the shared DAGs directory
sed -i 's|^dags_folder = .*|dags_folder = /shared/final_project/dags|' "${AIRFLOW_HOME}/airflow.cfg"

echo "=== [5/5] Start Airflow (background) ==="
airflow scheduler &
airflow webserver --port 8080 &

echo ""
echo "============================================"
echo " Setup complete!"
echo " Airflow UI: http://<VM_EXTERNAL_IP>:8080"
echo " Username: admin / Password: project405_team9!"
echo "============================================"
