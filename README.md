# PRF Data Engineering Pipeline

Projeto de Engenharia de Dados end-to-end de sinistros de trânsito da PRF (Polícia Rodoviária Federal) usando pipelines de dados profissionais, arquitetura medallion, orquestração com Apache Airflow e carga dos dados em um Data Warehouse SQL.

**Status do Projeto:** ✅ **Etapa #1 concluída** | Etapa #2 em desenvolvimento

---

## 📊 Visão Geral

Este projeto implementa um **data engineering pipeline** completo para processar dados de acidentes rodoviários brasileiros (2017–2025), com foco em qualidade de dados, escalabilidade e demonstração de competências em engenharia de dados.

Dados: 9 anos de dados (2017-2025) | ~1.5M registros | 35 colunas por arquivo .csv

[Clique aqui para acessar os Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf). Os datasets usados são os denominados "Agrupados por pessoa"

---
## 🛠️ Tecnologia Stack

| Componente | Ferramenta | Versão | Propósito |
|---|---|---|---|
| **Linguagem** | Python | 3.11+ | Processamento e orquestração |
| **Data Processing** | pandas | 2.1.4+ | Manipulação de dados |
| **Validação** | Pandera | 0.18.1+ | Schema validation |
| **Storage** | Parquet | — | Formato eficiente columnar |
| **Warehouse** | A definir | A definir | Armazenamento relacional |
| **Orquestração** | A definir |A definir | Agendamento de DAGs |
| **Containerização** | A definir | A definir | Ambiente isolado |
| **Dashboard** |A definir | A definir | Visualizações (opcional) |
| **Testes** | A definir | A definir | Unit & integration tests |

---

### Arquitetura Geral

```
🗂️ Bronze                           🔄 Silver                 ⭐ Gold           
data/01_bronze/                   data/02_silver/            data/03_gold/              
 ├─ acidentes2017.csv       ├─ acidentes2017.parquet       Em desenvolvimento        
 ├─ acidentes2018.csv       ├─ acidentes2018.parquet      
 ├─ ...                     └─ ...                                        
 └─ acidentes2025.csv                                                                
       ⬇️                              ⬇️                      
    Extração                   Validação + Limpeza           
(download_prf_data.py)          (silver_process.py)      
```

---
### 📁 Estrutura do repositório
 
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
│   ├── schema.sql          # Schema PostgreSQL
│   └── setup.py            # Setup do banco
│
├── dags/                   # Orquestração Airflow [TODO]
│   └── prf_pipeline_dag.py
│
├── config/                 # Configurações
│   └── prf_download_urls.json
│
├── docker-compose.yaml     # Serviços (PostgreSQL, Airflow, Metabase)
├── Dockerfile              # Imagem customizada
├── requirements.txt        # Dependências Python
├── .env                    # Variáveis de ambiente
└── README.md              # Este arquivo
```
---

## 🎯 Etapas do Projeto

### ✅ Etapa #1: Schema Validation (Concluída)

**Objetivo:** Garantir que os dados brutos sejam estruturalmente válidos, limpos e prontos para carga no DW.

**Arquivos Principais:**
- `src/01_ingestion/download_prf_data.py` — Download e extração automatizada dos dados disponíveis no site da PRF (de 2017 a 2025)
- `src/02_processing/silver_process.py` — Limpeza dos dados com Pandas, validação com Pandera e armazenamento dos arquivos em formato parquet na camada silver.


**Escopo:**
1. **Análise de CSVs 2017–2025** ✅
   - Delimitador: `;` (ponto-e-vírgula)
   - Encoding: `LATIN-1`
   - 41 colunas com tipos padronizados

2. **Limpeza de Dados** ✅
   - Duplicatas em `pesid` (manter primeiro)
   - Conversão de tipos para validação e otimização de espaço no Data Warehouse (Int8/Int16/Int32/Int64/Float64)
   - Fusão de `data_inversa` + `horario` → `data_hora`
   - Normalização de casas decimais (vírgula → ponto em `km`, `latitude`, `longitude`)
   - Padronização de valores enum (dia_semana, fase_dia, sexo, etc.)
   - Tratamento de ausências (valores `NA`, `Não Informado` → `pd.NA`)

3. **Validação com Pandera** ✅
   - Schema de 34 colunas com constraints de tipos, ranges, valores permitidos
   - Detecção de anomalias e relatório detalhado
   - Logs de sucesso/falha por arquivo

4. **Output: Silver Layer** ✅
   - Parquets validados em `data/02_silver/`
   - Prontos para posterior carga no Data Warehouse (Etapa #2)


**Desafios Resolvidos:**
- Schema inconsistente entre anos → Normalização na limpeza
- Tipos flutuantes → Conversão explícita com nullable Int8/Int16/Int32
- Latitude/longitude parseadas incorretamente → Tratamento manual via string
- `pd.NA` vs `None` → Uso de Float64/Int8/Int16 nativo do pandas

---

### 🔄 Etapa #2: Transform & Load Modules (Em Desenvolvimento)


### 🏗️ Etapa #3: Data Warehousing (Planejada)


### 🏁 Etapa #4: Pipeline Setup & Orchestration (Planejada)
---

## 🚀 Como Usar
 
### Pré-requisitos
 
```bash
# Python 3.11+
python --version
 
# Dependências
pip install -r requirements.txt

```
 
### 1. Download dos Dados (Etapa #1 ✅)
 
```bash
python src/01_ingestion/download_prf_data.py
# → Baixa 9 CSVs em data/01_bronze/
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
 
### 3. Transformação & Carga [EM DESENVOLVIMENTO]
 
### 4. Orquestração com Airflow [EM DESENVOLVIMENTO]

 
---

### Esquema (41 colunas)

| Campo | Tipo | Nulo | Notas |
|---|---|---|---|
| `id` | Int32 | Não | Identificador do acidente (pode repetir) |
| `pesid` | Int64 | Não | Identificador único do envolvido (PK para Silver) |
| `data_hora` | DateTime | Não | Data + Hora do acidente (fusão de 2 colunas) |
| `dia_semana` | String | Não | segunda-feira..domingo |
| `uf` | String | Não | UF brasileiro (ex: SP, RJ, MG) |
| `br` | Float | Sim | Número da rodovia federal (ex: 101, 116) |
| `km` | Float | Sim | Quilometragem no trecho (0–2000 típico) |
| `municipio` | String | Não | Município do acidente |
| `causa_acidente` | String | Sim | Descrição da causa |
| `tipo_acidente` | String | Sim | Classificação (colisão, tombamento, etc.) |
| `classificacao_acidente` | String | Sim | Com Vítimas Fatais / Com Vítimas Feridas / Sem Vítimas |
| `fase_dia` | String | Sim | Amanhecer / Pleno dia / Anoitecer / Plena Noite |
| `sentido_via` | String | Sim | Crescente / Decrescente |
| `condicao_metereologica` | String | Sim | Tempo / Chuva / Neblina / etc |
| `tipo_pista` | String | Sim | Simples / Múltipla / Dupla |
| `tracado_via` | String | Sim | Reta / Curva / Aclive / Declive (composto possível) |
| `uso_solo` | String | Sim | Sim / Não |
| `id_veiculo` | Float | Sim | Identificador do veículo |
| `tipo_veiculo` | String | Sim | Carro / Caminhão / Moto / Ônibus / etc |
| `marca` | String | Sim | Marca do veículo (ex: Fiat, Chevrolet) |
| `ano_fabricacao_veiculo` | Int16 | Sim | Ano de fabricação (1900–2027) |
| `tipo_envolvido` | String | Sim | Condutor / Passageiro / Pedestre / Cavaleiro / Testemunha |
| `estado_fisico` | String | Sim | Ileso / Lesões Leves / Lesões Graves / Óbito |
| `idade` | Int16 | Sim | Idade (0–110) |
| `sexo` | String | Sim | Masculino / Feminino |
| `ilesos` / `feridos_leves` / `feridos_graves` / `mortos` | Int8 | Sim | Flag 0/1 |
| `latitude` / `longitude` | Float64 | Sim | Coordenadas WGS84 (-90..90 / -180..180) |
| `regional` / `delegacia` / `uop` | String | Sim | Localização administrativa |

---

## 🧪 Testes

### Etapa #1 (Atual)
Validação via logs do Pandera e relatório de resumo.

```bash
python src/extract/validate_schema.py
```

### Etapa #2 (Próximas)
Testes unitários para transformações:

```bash
pytest tests/test_transform.py -v
```

---

## 📝 Notas de Desenvolvimento

### Decisões de Design

1. **Arquitetura Medallion: Bronze/Silver/Gold:** Padrão corporativo para auditoria e rastreabilidade.
2. **Pandera** Pandera para validação de schema + dados.
3. **Parquet vs CSV:** Compressão, leitura columnar, suporte a tipos complexos.

### Aprendizados

- **Schema inconsistência:** Dados PRF têm formatação estável apenas a partir de 2017. Dados 2016 e anteriores requerem pipeline separada.
- **Tipos nullable:** Int8/Int16/Int32/Int64 do pandas evitam coerção para float ao processar pd.NA.
- **Encoding:** LATIN-1 é obrigatório; UTF-8 causa falhas silenciosas.

---

## ❓ FAQ

### P: Por que não versionar os CSVs no Git?

**R:** Os CSVs ocupam 2–5 GB (9 anos × 1.7M registros). Versionar dados brutos:
- ❌ Torna o repositório impraticável (clone/push lento)
- ❌ Viola boas práticas de engenharia de dados
- ❌ Dificulta atualizações (novo ano = adicionar GB ao repo)

**Solução:** Usar `download_prf_data.py` para baixar automaticamente. Se offline:
1. Baixe manualmente de https://www.prf.gov.br
2. Coloque em `data/raw/`
3. Rode scripts de limpeza/validação

### P: E se o site da PRF mudar ou cair?

**R:** Boas práticas:
1. **Cache local:** `download_prf_data.py` memoriza downloads bem-sucedidos em `.download_cache.json`
2. **Fallback manual:** Se URL quebrar, sempre pode baixar manualmente
3. **Documentação:** Manter log de URLs em `DOWNLOAD_URLS` (config no script)
4. **Versionamento de dados críticos:** Se precisar de snapshots históricos:
   - Use Git LFS (Large File Storage) ou
   - Salve em storage externo (S3, GCP, etc.) — implementado na Etapa #4

### P: O que é Git LFS?

**R:** Git LFS permite versionar arquivos grandes sem entupir o repositório:

```bash
# Instalar Git LFS
git lfs install

# Rastrear .parquets (caso queira versionar processados)
git lfs track "data/processed/*.parquet"

# Usar normalmente (Git LFS é transparente)
git add data/processed/
git commit -m "Add gold layer"
```

**Não recomendado para este projeto** porque os parquets são regeneráveis de CSVs brutos.

### P: Como atualizar com novos anos de dados?

**R:**
```bash
# Se novo ano é 2026:
python data/raw/download_prf_data.py --year 2026

# Ou atualizar range:
python data/raw/download_prf_data.py --year-range 2025 2026

# O script valida e move para Gold Layer automaticamente
```

### P: Posso rodar o pipeline sem internet?

**R:** **Sim**, com as seguintes condições:
- CSVs brutos já existem em `data/raw/`
- Pule o `download_prf_data.py` e rode direto:

```bash
python src/02_processing/silver_process.py      # Bronze → Silver
```

## 🔗 Recursos Externos

- [PRF Dataset](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf) — Dados públicos
- [pandas Documentation](https://pandas.pydata.org/)
- [Pandera Docs](https://pandera.readthedocs.io/)
- [Git LFS](https://git-lfs.github.com/) — Para arquivos grandes (opcional)

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