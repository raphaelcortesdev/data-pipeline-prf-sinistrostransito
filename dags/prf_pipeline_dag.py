"""
DAG DO PIPELINE PRF - AIRFLOW

AUTOR: Raphael Cortes
DATA: 2026
"""

import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================================
# RESOLUÇÃO DE PATH (Airflow + VSCode)
# ============================================================================

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ============================================================================
# WRAPPERS COM LAZY IMPORT (Evita DagBag Import Timeout)
# ============================================================================

def run_setup_task():
    """Importa e executa o provisionamento do Data Warehouse (Banco + Tabelas)."""
    from warehouse.setup import main as setup_main
    setup_main()

def run_download_task():
    """Importa e executa a ingestão apenas quando a task roda."""
    from src.ingestion.download_prf_data import main as download_main
    download_main()

def run_silver_task():
    """Importa e executa o processamento apenas quando a task roda."""
    from src.processing.silver_process import main as process_main
    process_main()

def run_load_task():
    """Importa e executa a carga no DW apenas quando a task roda."""
    from src.load.load import main as load_main
    load_main()

# ============================================================================
# CONFIGURAÇÃO PADRÃO DA DAG
# ============================================================================

default_args = {
    'owner': 'Raphael Cortes',
    'timezone': 'America/Sao_Paulo',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

# ============================================================================
# DEFINIÇÃO DA DAG
# ============================================================================

dag = DAG(
    dag_id='prf_pipeline',
    description='Pipeline completo de DDL, ingestão, processamento e carga de dados PRF',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['prf', 'pipeline', 'producao'],
    max_active_runs=1,
)

# ============================================================================
# DEFINIÇÃO DAS TASKS
# ============================================================================

# TASK 0: SETUP / PROVISIONAMENTO DO BANCO DE DADOS
task_setup = PythonOperator(
    task_id='setup_warehouse',
    python_callable=run_setup_task,
    dag=dag,
)

# TASK 1: DOWNLOAD (Camada Bronze)
task_download = PythonOperator(
    task_id='download_prf_data',
    python_callable=run_download_task,
    dag=dag,
)

# TASK 2: PROCESSAMENTO (Camada Silver)
task_process = PythonOperator(
    task_id='silver_process',
    python_callable=run_silver_task,
    dag=dag,
)

# TASK 3: CARGA (Data Warehouse PostgreSQL)
task_load = PythonOperator(
    task_id='load_warehouse',
    python_callable=run_load_task,
    dag=dag,
)

# ============================================================================
# ORDEM DE EXECUÇÃO
# ============================================================================

# Provisiona o banco -> Baixa Bronze -> Trata Silver -> Carrega Gold/DW
task_setup >> task_download >> task_process >> task_load