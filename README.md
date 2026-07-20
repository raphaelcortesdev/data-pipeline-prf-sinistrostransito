![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.1.4+-150458?logo=pandas&logoColor=white)
![Pandera](https://img.shields.io/badge/Pandera-Schema-3D85C6)
![Parquet](https://img.shields.io/badge/Storage-Parquet-008080)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/raphael-cortes-b0b544305/)
[![Instagram](https://img.shields.io/badge/Instagram-E4405F?style=flat&logo=instagram&logoColor=white)](https://www.instagram.com/raphaelcorte_s/)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-25D366?style=flat&logo=whatsapp&logoColor=white)](https://wa.me/5561998294492)

# PRF Data Engineering Pipeline

Projeto de Engenharia de Dados end-to-end de sinistros de trânsito da PRF (Polícia Rodoviária Federal), com pipelines profissionais, processamento otimizado e carga em um Data Warehouse relacional focado em performance.

**Status do Projeto:** ✅ **Etapas #1 e #2 concluídas** | 🔄 Etapa #3 em desenvolvimento

---

## 📊 Visão Geral

Este projeto implementa um **data engineering pipeline completo** para processar dados de acidentes rodoviários brasileiros (2017–2025), com foco em:

* Qualidade de dados
* Escalabilidade
* Modelagem dimensional
* Performance de ETL

**Dados:**

* 9 anos (2017–2025)
* ~1.5M registros
* 41 colunas por CSV

🔗 [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)
Datasets utilizados: **"Agrupados por pessoa"**

---

## 🛠️ Stack Utilizada

| Componente          | Ferramenta    | Propósito                    |
| ------------------- | ------------- | ---------------------------- |
| **Linguagem**       | Python 3.11+  | Processamento e orquestração |
| **Data Processing** | Pandas 2.1.4+ | Manipulação e deduplicação   |
| **Validação**       | Pandera       | Schema validation            |
| **Storage**         | Parquet       | Formato columnar eficiente   |
| **Warehouse**       | PostgreSQL    | Desempenho otimizado para consulta <br> e análise de grandes quantidades de dados.|
| **Driver**          | psycopg2      | Batch insert                 |

---

## 📁 Arquitetura Geral

```
Bronze (CSV)          →        Silver (Parquet)         →       Gold (Data Warehouse - PostgreSQL)

data/01_bronze/       →        data/02_silver/          →        Star Schema
├─ acidentes2017.csv  →        ├─ acidentes2017.parquet →        ├─ fato
├─ acidentes2018.csv  →        ├─ acidentes2018.parquet →        ├─ dim_pessoa
├─ ...                →        └─ ...                   →        ├─ dim_tempo
└─ acidentes2025.csv                                    →        └─ demais dimensões
```

**Fluxo:**

* Extração → `download_prf_data.py`
* Limpeza + Validação → `silver_process.py`
* ETL + Carga → `load.py`

---

## 📂 Estrutura do Repositório

```
prf-sinistros-pipeline/
│
├── data/
│   ├── 01_bronze/          # Dados brutos (CSVs originais)
│   └── 02_silver/          # Dados limpos + validados (Parquets)
│
├── src/
│   ├── 01_ingestion/       # Download e extração de dados
│   │   └── download_prf_data.py
│   │
│   ├── 02_processing/      # Limpeza + Validação unificadas
│   │   └── silver_process.py
│   │
│   └── 03_load/            # Carregamento no DW
│       └── load.py         # ETL com Cache em RAM e Batch Insert
│
├── warehouse/              # Infraestrutura do Data Warehouse (Gold Layer)
│   ├── schema.sql          # Schema PostgreSQL (Star Schema)
│   └── setup.py            # Setup do banco e execução do DDL
│
├── config/                 # Configurações
│   └── prf_download_urls.json
│
├── .env.example            # Variáveis de ambiente (DB_HOST, DB_USER, etc)
└── README.md               # Este arquivo
```

---

## 🎯 Etapas do Projeto

### ✅ Etapa #1: Schema Validation & Ingestion

**Objetivo:** Garantir dados limpos e válidos.

* Download automatizado
* Remoção de duplicatas
* Normalização UTF-8
* Validação com Pandera (34 colunas)
* Conversão para Parquet

---

### ✅ Etapa #2: Data Warehousing & ETL

**Objetivo:** Construir DW analítico e otimizar carga.

#### 🔹 Modelagem (Star Schema)

* Tabela `fato` central

* 7 dimensões:

  * `dim_pessoa`
  * `dim_tempo`
  * `dim_local`
  * `dim_clima`
  * `dim_pista`
  * `dim_veiculo`
  * `dim_classificacao`

* Constraints `UNIQUE` para integridade

#### 🔹 Preparação com Pandas

* `.drop_duplicates()`
* `.dropna()`
* Conversão de tipos (`pd.NA → SQL`)

#### 🔹 Performance do ETL

* ⚡ Cache em RAM (elimina N+1 queries)
* ⚡ Batch insert (`execute_batch`)
* ⚡ Lotes de 5.000 registros

**Impacto:**

* Redução de horas → segundos por ano
* 1.5M registros carregados
* 0 registros órfãos

---

### 🔄 Etapa #3: Orquestração

**Objetivo:** Automação com Airflow

**Status:** Em análise

---

## 🚀 Como Usar

### Pré-requisitos

```bash
python --version  # 3.11+
```

### 🔐 Configuração

Crie um `.env` baseado no `.env.example`.

---

### ⚙️ Setup do Ambiente

Ambiente Conda(recomendado):
```bash
conda create -n prf-pipeline python=3.11
conda activate prf-pipeline
pip install -r requirements.txt
```
Ou venv(ammbiente nativo python), se preferir:
```bash
python -m venv venv

# Ativar no Linux/Mac
source venv/bin/activate

# Ativar no Windows (Prompt de Comando)
venv\Scripts\activate
# ou no PowerShell: .\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

---

### 📥 1. Download dos Dados

```bash
python src/01_ingestion/download_prf_data.py
```

---

### 🧹 2. Limpeza & Validação

```bash
python src/02_processing/silver_process.py
```

---

### 🏗️ 3. Setup do Banco

```bash
python warehouse/setup.py
```

**Reset completo:**

```bash
python warehouse/setup.py --fresh
```
Na primeira vez que rodar, o `--fresh` não faz diferença. Mas caso queria em algum apagar o banco para repopular novamente, use o `--fresh`

---

### 📦 4. Carga de Dados (ETL)

```bash
python src/03_load/load.py
```

---

## ❓ FAQ

**Por que os CSVs não estão no repositório?**
→ Arquivos ocupam 2–5GB. Use o script de download.

**Como o gargalo foi resolvido?**
→ Combinação de:

* Pandas (deduplicação)
* Cache em RAM
* Batch insert com psycopg2

---

## 📜 Licença

MIT License

---

## 📅 Última Atualização

Julho de 2026

**Etapa Atual:** ✅ #2 Concluída
**Próxima:** 🔄 #3 Orquestração

## 👨‍💻 Autor

**Raphael Cortes Gomes - Cientista de Dados/Sanitarista**

Entre em contato ou acompanhe meu trabalho através dos links abaixo:

*   **LinkedIn:** [linkedin.com/in/raphael-cortes-b0b544305](https://www.linkedin.com/in/raphael-cortes-b0b544305/)
*   **Instagram:** [@raphaelcorte_s](https://www.instagram.com/raphaelcorte_s/)
*   **WhatsApp:** [Falar no WhatsApp](https://wa.me/5561998294492)