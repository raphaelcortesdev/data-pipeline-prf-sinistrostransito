# ============================================================================
# DOCKERFILE - AIRFLOW COM DEPENDÊNCIAS DO PROJETO PRF
# ============================================================================

# Imagem base do Airflow 2.7.3 com Python 3.11
FROM apache/airflow:2.7.3-python3.11

# Usuário que executa (airflow é criado pela imagem base)
USER root

# ============================================================================
# INSTALAR DEPENDÊNCIAS DO SISTEMA
# ============================================================================

RUN apt-get update && apt-get install -y \
    # Ferramentas úteis
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# CRIAR DIRETÓRIOS DO PROJETO E AJUSTAR PERMISSÕES
# (feito como root antes de mudar de usuário)
# ============================================================================

# Criar estrutura de diretórios e dar permissão para o usuário airflow
RUN mkdir -p /app/{dags,src,data,config,warehouse,logs,plugins} \
    && chown -R airflow:root /app

# ============================================================================
# VOLTAR PARA USUÁRIO AIRFLOW (Crucial para o pip)
# ============================================================================

USER airflow

# ============================================================================
# COPIAR requirements.txt E INSTALAR DEPENDÊNCIAS PYTHON
# ============================================================================

# Copiar requirements do projeto
COPY requirements.txt /tmp/requirements.txt

# Instalar pacotes Python com pip (agora rodando corretamente como airflow)
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ============================================================================
# VARIÁVEIS DE AMBIENTE (Airflow)
# ============================================================================

ENV AIRFLOW_HOME=/home/airflow/airflow
ENV PYTHONUNBUFFERED=1

# ============================================================================
# VOLUME E WORKING DIRECTORY
# ============================================================================

WORKDIR /app

# ============================================================================
# PORTA (Airflow Webserver)
# ============================================================================

EXPOSE 8080

# ============================================================================
# COMANDO PADRÃO (sobrescrito por docker-compose)
# ============================================================================

CMD ["airflow", "webserver"]