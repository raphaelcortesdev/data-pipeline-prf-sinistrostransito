![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.1.4+-150458?logo=pandas&logoColor=white)
![Pandera](https://img.shields.io/badge/Pandera-Schema-3D85C6)
![Parquet](https://img.shields.io/badge/Storage-Parquet-008080)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.7.3-017CEE?logo=apache-airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Status](https://img.shields.io/badge/status-concluído-brightgreen)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/raphael-cortes-b0b544305/)
[![Instagram](https://img.shields.io/badge/Instagram-E4405F?style=flat&logo=instagram&logoColor=white)](https://www.instagram.com/raphaelcorte_s/)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-25D366?style=flat&logo=whatsapp&logoColor=white)](https://wa.me/5561998294492)

# PRF Data Engineering Pipeline

Projeto de Engenharia de Dados end-to-end de sinistros de trânsito da PRF (Polícia Rodoviária Federal), com pipelines profissionais, processamento otimizado, carga em um Data Warehouse relacional focado em performance e orquestração automatizada em ambiente containerizado.

---

## 📊 Visão Geral

Este projeto implementa um **data engineering pipeline completo** para processar dados de sinistros rodoviários brasileiros (2017–2025), com foco em qualidade de dados, escalabilidade, modelagem dimensional (Star Schema) e orquestração via Apache Airflow.

**Dados Processados:**
* 9 anos históricos
* ~1.5M de registros
* 41 colunas originais transformadas e tipadas

🔗 [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf) (Agrupados por pessoa)

---

## 🛠️ Stack Utilizada

| Componente          | Ferramenta       | Propósito                    |
| ------------------- | ---------------- | ---------------------------- |
| **Orquestração**    | Apache Airflow   | Agendamento e monitoramento da DAG |
| **Infraestrutura**  | Docker Compose   | Containerização de serviços (Isolamento total) |
| **Data Processing** | Python + Pandas  | Processamento, manipulação e deduplicação   |
| **Validação**       | Pandera          | Schema validation e garantia de qualidade   |
| **Storage**         | Parquet          | Formato columnar otimizado para I/O   |
| **Warehouse**       | PostgreSQL 16    | Desempenho otimizado para consulta analítica |
| **Driver**          | psycopg2         | Batch insert e persistência no banco |

---

## 🚀 Como Usar (Ambiente Docker)

### 1. Pré-requisitos e Configuração
* **Docker Desktop** (ou Docker Engine + Docker Compose) instalado.
* Crie o arquivo de variáveis de ambiente:
```bash
cp .env.example .env
```
*(Certifique-se de configurar suas senhas e variáveis no `.env` recém-criado).*

### 2. Subindo a Infraestrutura
Com um único comando, levante todo o pipeline (Postgres, Airflow Webserver e Scheduler) em background:
```bash
docker compose up -d
```

### 3. Executando o Pipeline
1. Acesse o painel do Airflow em `http://localhost:8081` no seu navegador.
2. Faça login (Padrão: Usuário `admin`, Senha `admin` - *ou conforme seu .env*).
3. Habilite a DAG `prf_pipeline` (Unpause) e clique no botão **Trigger DAG** (Play).
4. O processo (Download → Limpeza → Carga) ocorrerá de forma 100% automatizada.

### 4. Consultando os Dados
Conecte sua ferramenta favorita (DBeaver, pgAdmin ou extensão do VS Code) ao banco gerado:
* **Host:** `localhost` (ou `127.0.0.1`)
* **Porta:** `5432`
* **Usuário / Senha / Banco:** Consulte as chaves `DB_USER`, `DB_PASSWORD` e `DB_NAME` do seu `.env`.

### 5. Encerrando o Ambiente
Para desligar preservando os dados processados e os logs:
```bash
docker compose down
```
*(**Aviso:** Adicionar a flag `-v` a este comando excluirá todos os volumes, deletando permanentemente o Data Warehouse).*

<details>
<summary><strong>▶ Execução Manual / Local (Sem Docker)</strong></summary>

Caso queira debugar ou executar os scripts individualmente sem o Airflow:

**1. Setup do Ambiente Python:**
```bash
conda create -n prf-pipeline python=3.11
conda activate prf-pipeline
# ou via venv: python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

**2. Execução Passo a Passo:**
```bash
python src/ingestion/download_prf_data.py
python src/processing/silver_process.py
python warehouse/setup.py
python src/load/load.py
```
*(Nota: Para executar o DW localmente sem o container Postgres, você precisará de uma instância própria do PostgreSQL rodando na sua máquina).*
</details>

---

## 📁 Arquitetura e Fluxo

```text
Bronze (CSV)       →        Silver (Parquet)      →       Gold (DW PostgreSQL)
data/bronze/       →        data/silver/          →       Star Schema (Docker Container)
├─ acidentes2017.csv  →     ├─ acidentes2017.parquet →    ├─ fato
├─ ...                →     └─ ...                   →    ├─ dim_pessoa, dim_tempo, etc.
```

**Fluxo da DAG `prf_pipeline`:**
1. `setup_warehouse`: Provisionamento DDL.
2. `download_prf_data`: Ingestão Bronze.
3. `silver_process`: Limpeza, Validação Pandera e conversão Parquet.
4. `load_warehouse`: Carga Fato/Dimensões via dicionários de Cache e Batch Insert.

---

## 🎯 Etapas e Otimizações

*   **Ingestão e Validação (Etapa #1):** Padronização de encoding (UTF-8), remoção prévia de duplicatas e validação estrita de 34 colunas garantindo integridade das regras de negócio.
*   **Modelagem e Performance (Etapa #2):** Modelagem Star Schema isolada. A etapa de *Load* foi reescrita utilizando Dicionários Python em RAM (eliminando consultas N+1) e `psycopg2.extras.execute_batch`, transformando cargas que levavam horas em segundos. Cláusulas de `ON CONFLICT` garantem idempotência.
*   **Orquestração e Containerização (Etapa #3):** Encapsulamento completo da infraestrutura eliminando gargalos de SO. Resolução de caminhos via injeção de dependência (`sys.path`) e sincronização de *secret keys* entre containers para distribuição de logs.

---

## 🛠️ Troubleshooting (Resolução de Problemas Comuns)

Esta seção documenta desafios de rede e infraestrutura encontrados durante a containerização:

<details>
<summary><strong>▶ Falha de Autenticação no PostgreSQL (Password authentication failed)</strong></summary>

**Sintoma:** O VS Code/DBeaver recusa a senha configurada ao tentar conectar na porta `5432`.
**Causa:** Conflito com uma instalação local do PostgreSQL no Windows/Linux que já utiliza a porta `5432`. A ferramenta de banco de dados conecta ao banco local, e não ao container Docker.
**Solução:** 
1. Verifique se o Postgres local está rodando em background e pare o serviço.
2. Alternativamente, altere o mapeamento de portas no `docker-compose.yaml` (ex: `"5433:5432"`) e conecte na nova porta externa. Certifique-se de usar a mesma senha definida no arquivo `.env`.
</details>

<details>
<summary><strong>▶ pgAdmin 4 não abre ou exibe "The pgAdmin 4 server could not be contacted"</strong></summary>

**Sintoma:** Ao tentar abrir o pgAdmin local, o servidor Python falha ao subir.
**Causa:** O pgAdmin geralmente tenta alocar a porta fixa `5050`. Se o Docker ou o Airflow estiverem rodando processos em segundo plano que esbarrem nessa porta de rede, o aplicativo travará.
**Solução:** Nas configurações do pgAdmin (ícone de engrenagem), desmarque a opção "Fixed port number". O pgAdmin buscará dinamicamente uma porta ociosa e abrirá normalmente.
</details>

<details>
<summary><strong>▶ Erro 403 Forbidden nos Logs do Airflow (DAG)</strong></summary>

**Sintoma:** As execuções funcionam, mas a UI não renderiza os logs da saída padrão (Stdout), exibindo `Client error '403 FORBIDDEN'`.
**Causa:** O *Scheduler* (que salva o log) e o *Webserver* (que lê o log) rodam em containers separados e não possuem uma chave de confiança mútua (Secret Key), bloqueando a comunicação interna.
**Solução:** Adicione a mesma variável de ambiente `AIRFLOW__WEBSERVER__SECRET_KEY` em ambos os serviços (`airflow` e `airflow-scheduler`) dentro do `docker-compose.yaml` para permitir a troca de informações, e garanta que o volume `airflow_data` esteja mapeado em ambos.
</details>

---

## 📜 Licença e Atualizações

MIT License | **Última Atualização:** Julho de 2026

## 👨‍💻 Autor

**Raphael Cortes Gomes - Cientista de Dados/Sanitarista**

*   **LinkedIn:** [linkedin.com/in/raphael-cortes-b0b544305](https://www.linkedin.com/in/raphael-cortes-b0b544305/)
*   **Instagram:** [@raphaelcorte_s](https://www.instagram.com/raphaelcorte_s/)
*   **WhatsApp:** [Falar no WhatsApp](https://wa.me/5561998294492)