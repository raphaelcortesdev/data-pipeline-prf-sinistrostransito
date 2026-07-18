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

Projeto de Engenharia de Dados end-to-end de sinistros de trânsito da PRF (Polícia Rodoviária Federal) usando pipelines de dados profissionais, arquitetura medallion, orquestração com Apache Airflow e carga dos dados em um Data Warehouse SQL.

**Status do Projeto:** ✅ **Etapa #1 concluída** | Etapa #2 em desenvolvimento

---

## 📊 Visão Geral

Este projeto implementa um **data engineering pipeline** completo para processar dados de acidentes rodoviários brasileiros (2017–2025), com foco em qualidade de dados, escalabilidade e demonstração de competências em engenharia de dados.

Dados: 9 anos de dados (2017-2025) | ~1.5M registros | 41 colunas por arquivo .csv

[Clique aqui para acessar os Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf). Os datasets usados são os denominados "Agrupados por pessoa"

---

## 🛠️ Tecnologia Stack

| Componente | Ferramenta | Propósito |
|---|---|---|
| **Linguagem** | Python 3.11+ | Processamento e orquestração |
| **Data Processing** | pandas 2.1.4+ | Manipulação de dados |
| **Validação** | Pandera 0.18.1+ | Schema validation |
| **Storage Intermediário** | Parquet | Formato eficiente columnar |
| **Warehouse SQL** | A definir | Armazenamento relacional (Etapa #3) |
| **Orquestração** | A definir | Agendamento de DAGs (Etapa #4) |
| **Containerização** | Docker | Ambiente isolado (opcional) |
| **Visualização** | A definir | Dashboards BI (pós-projeto) |

---

## 📁 Arquitetura Geral

```
🗂️ Bronze                           🔄 Silver                 ⭐ Gold           
data/01_bronze/                   data/02_silver/            data/03_gold/              
 ├─ acidentes2017.csv       ├─ acidentes2017.parquet       [Em desenvolvimento]       
 ├─ acidentes2018.csv       ├─ acidentes2018.parquet      
 ├─ ...                     └─ ...                                        
 └─ acidentes2025.csv                                                                
       ⬇️                              ⬇️                      
    Extração                   Validação + Limpeza           
(download_prf_data.py)          (silver_process.py)      
```

### Estrutura do Repositório
 
```
prf-sinistros-pipeline/
│
├── data/
│   ├── 01_bronze/          # Dados brutos (CSVs originais)
│   ├── 02_silver/          # Dados limpos + validados (Parquets)
│   └── 03_gold/            # Dados agregados + otimizados
│
├── src/
│   ├── 01_ingestion/       # Download e extração de dados
│   │   └── download_prf_data.py
│   │
│   ├── 02_processing/      # Limpeza + Validação unificadas
│   │   └── silver_process.py
│   │
│   └── 03_load/            # Carregamento no DW [TODO]
│       └── load_to_dw.py
│
├── warehouse/              # Infraestrutura do Data Warehouse
│   ├── schema.sql          # Schema PostgreSQL [TODO]
│   └── setup.py            # Setup do banco [TODO]
│
├── dags/                   # Orquestração Airflow [TODO]
│   └── prf_pipeline_dag.py
│
├── config/                 # Configurações
│   └── prf_download_urls.json
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md              # Este arquivo
```

---

## 🎯 Etapas do Projeto

### ✅ Etapa #1: Schema Validation (Concluída)

**Objetivo:** Garantir que os dados brutos sejam estruturalmente válidos, limpos e prontos para carga no DW.

**Escopo Realizado:**

1. **Análise de CSVs 2017–2025** ✅
   - Delimitador: `;` (ponto-e-vírgula)
   - Encoding: `LATIN-1`
   - 41 colunas com tipos padronizados

2. **Limpeza de Dados** ✅
   - Duplicatas em `pesid` (manter primeiro)
   - Conversão de tipos para validação e otimização de espaço no DW
   - Fusão de `data_inversa` + `horario` → `data_hora`
   - Normalização de casas decimais (vírgula → ponto)
   - Padronização de valores enum
   - Tratamento de ausências (`NA`, `Não Informado` → `pd.NA`)

3. **Validação com Pandera** ✅
   - Schema de 34 colunas com constraints de tipos, ranges e valores permitidos
   - Detecção de anomalias com relatório detalhado
   - Logs de sucesso/falha por arquivo

4. **Output: Silver Layer** ✅
   - Parquets validados em `data/02_silver/`
   - Prontos para posterior carga no Data Warehouse

**Desafios Resolvidos:**
- Schema inconsistente entre anos
- Tipos nullable (Int8/Int16/Int32 do pandas)
- Latitude/longitude parseadas incorretamente
- `pd.NA` vs `None` em Parquet

---

### 🔄 Etapa #2: Transform & Load Modules

**Objetivo:** Transformar dados validados (Silver) em estruturas otimizadas (Gold) e preparar carga no warehouse.

Status: Em desenvolvimento

---

### 🏗️ Etapa #3: Data Warehousing

**Objetivo:** Desenhar e implementar warehouse otimizado para queries analíticas.

Status: Em análise

---

### 🏁 Etapa #4: Pipeline & Orchestration

**Objetivo:** Orquestrar todo o fluxo com Airflow e automação end-to-end.

Status: Em análise

---

## 🚀 Como Usar

### Pré-requisitos

```bash
# Python 3.11+
python --version
```

### Setup do Ambiente

**Opção 1: Conda (recomendado)**
```bash
conda create -n prf-pipeline python=3.11
conda activate prf-pipeline
pip install -r requirements.txt
```

**Opção 2: venv (padrão Python)**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 1. Download dos Dados (Etapa #1 ✅)
 
```bash
python src/01_ingestion/download_prf_data.py
# → Baixa 9 CSVs de 2017-2025 em data/01_bronze/
```

### 2. Limpeza & Validação (Etapa #1 ✅)
 
```bash
python src/02_processing/silver_process.py
# → Processa CSVs, valida com Pandera, salva em data/02_silver/
```

**Output esperado:**
```
Iniciando processamento de arquivos da Bronze layer...
✅ acidentes2017.csv → Silver layer (validado com sucesso!)
✅ acidentes2018.csv → Silver layer (validado com sucesso!)
...
📊 RESUMO DO PROCESSAMENTO DA SILVER LAYER
✅ Sucessos (Silver): 9
❌ Falhas: 0
🎉 Todos os arquivos foram processados e salvos com sucesso!
```

### 3. Próximas Etapas

Transformação, carga e orquestração serão implementadas nas próximas etapas.

---

## 📋 Schema de Dados (41 colunas)

| Campo | Tipo | Nulo | Notas |
|---|---|---|---|
| `id` | Int32 | Não | Identificador do acidente (pode repetir) |
| `pesid` | Int64 | Não | Identificador único do envolvido |
| `data_hora` | DateTime | Não | Data + Hora do acidente |
| `dia_semana` | String | Não | segunda-feira...domingo |
| `uf` | String | Não | UF brasileiro (ex: SP, RJ, MG) |
| `br` | Int16 | Sim | Número da rodovia federal |
| `km` | Float | Sim | Quilometragem no trecho |
| `municipio` | String | Não | Município do acidente |
| `causa_acidente` | String | Sim | Descrição da causa |
| `tipo_acidente` | String | Sim | Classificação (colisão, tombamento, etc.) |
| `classificacao_acidente` | String | Sim | Com/Sem Vítimas |
| `fase_dia` | String | Sim | Amanhecer / Pleno dia / Anoitecer / Plena Noite |
| `sentido_via` | String | Sim | Crescente / Decrescente |
| `condicao_metereologica` | String | Sim | Tempo / Chuva / Neblina / etc |
| `tipo_pista` | String | Sim | Simples / Múltipla / Dupla |
| `tracado_via` | String | Sim | Reta / Curva / Aclive / Declive |
| `uso_solo` | String | Sim | Sim / Não |
| `id_veiculo` | Int32 | Sim | Identificador do veículo |
| `tipo_veiculo` | String | Sim | Carro / Caminhão / Moto / Ônibus |
| `marca` | String | Sim | Marca do veículo |
| `ano_fabricacao_veiculo` | Int16 | Sim | Ano de fabricação (1900–2027) |
| `tipo_envolvido` | String | Sim | Condutor / Passageiro / Pedestre / etc |
| `estado_fisico` | String | Sim | Ileso / Lesões Leves / Graves / Óbito |
| `idade` | Int16 | Sim | Idade (0–110) |
| `sexo` | String | Sim | Masculino / Feminino |
| `ilesos` | Int8 | Sim | Flag 0/1 |
| `feridos_leves` | Int8 | Sim | Flag 0/1 |
| `feridos_graves` | Int8 | Sim | Flag 0/1 |
| `mortos` | Int8 | Sim | Flag 0/1 |
| `latitude` | Float64 | Sim | WGS84 (-90...90) |
| `longitude` | Float64 | Sim | WGS84 (-180...180) |
| `regional` | String | Sim | Regional da PRF |
| `delegacia` | String | Sim | Delegacia responsável |
| `uop` | String | Sim | Unidade de Operações |
| + 8 colunas adicionais | — | — | (confira `silver_process.py`) |

---

## ❓ FAQ

### P: Por que não versionar os CSVs no Git?

**R:** CSVs ocupam 2–5 GB (9 anos × 1.7M registros). Armazenar dados brutos:
- ❌ Torna repositório impraticável
- ❌ Viola boas práticas de engenharia de dados
- ❌ Dificulta atualizações

**Solução:** Use `download_prf_data.py` para baixar automaticamente.

Se offline: baixe manualmente de https://www.prf.gov.br e coloque em `data/01_bronze/`, depois rode `silver_process.py`.

### P: E se o site da PRF mudar ou cair?

**R:** Boas práticas:
1. **Cache local:** Script memoriza downloads bem-sucedidos
2. **Fallback manual:** Sempre pode baixar manualmente se necessário
3. **Documentação:** URLs mantidas em `config/prf_download_urls.json`
4. **Versionamento crítico:** Para snapshots históricos, considere Git LFS ou storage externo (S3, GCP) — implementado na Etapa #4

### P: Como atualizar com novos anos de dados?

**R:**
```bash
python src/01_ingestion/download_prf_data.py
# Script detecta novos anos automaticamente e processa
```

### P: Posso rodar o pipeline sem internet?

**R:** **Sim**, se CSVs já existem em `data/01_bronze/`:

```bash
python src/02_processing/silver_process.py  # Bronze → Silver
```

---

## 🔗 Recursos Externos

- [PRF Dataset](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)
- [pandas Documentation](https://pandas.pydata.org/)
- [Pandera Docs](https://pandera.readthedocs.io/)

---

## 📧 Contribuições

Aberto para feedback e melhorias! Abra uma issue ou PR.

---

## 📜 Licença

MIT License — Veja LICENSE para detalhes.

---

**Última Atualização:** Julho de 2026  
**Etapa Atual:** ✅ #1 (Schema Validation) — Concluída  
**Próxima:** 🔄 #2 (Transform & Load Modules)