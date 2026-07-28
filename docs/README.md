# 📚 Documentação Oficial - Pipeline de Dados PRF

Este diretório centraliza toda a documentação técnica, regras de negócio e decisões de arquitetura do pipeline de dados de sinistros da Polícia Rodoviária Federal (PRF). 

Abaixo você encontra o índice dos documentos detalhados e a localização dos artefatos complementares.

---

## 📑 Índice de Documentos

* **[Dicionário de Dados](./dicionario_de_dados.md)**
  * Detalha as fontes oficiais e a extração dos dados brutos (Camada Bronze).
  * Mapeia o *schema* completo, os tipos de dados e os domínios das tabelas finais (Camadas Silver e Gold).
  * Documenta as regras universais de sanitização, padronização de bases históricas e o descarte de variáveis defasadas/redundantes para otimização de processamento.

* **[Arquitetura do Banco e Processo de Carga](./arquitetura_banco.md)**
  * Explica a modelagem dimensional (*Star Schema*) adotada no Data Warehouse (Camada Gold).
  * Justifica o uso de tipagem estrita (tipos `ENUM`) e regras restritivas (`UNIQUE`) para garantia de integridade referencial.
  * Detalha a estratégia de performance do script de carga (`load.py`), o processo de *upsert*, uso de *cache* em RAM e o sistema de observabilidade (geração de logs de rejeição para auditoria).

---

## 🗺️ Artefatos Complementares

A documentação escrita deste diretório trabalha em conjunto com diagramas e scripts localizados em outras pastas do repositório. Para um entendimento visual e estrutural completo, consulte:

* **Diagramas do Banco de Dados (`../imgs/`):**
  * `modelagem_conceitual.png`: Visão de alto nível das entidades e seus relacionamentos.
  * `modelagem_logica.png`: Visão aprofundada com tipos de dados, chaves primárias (PK) e estrangeiras (FK).

* **Modelagem e DDL (`../warehouse/`):**
  * `schema.sql`: Script responsável por erguer as tabelas, índices e tipos no PostgreSQL.
  * **Arquivos do brModelo:** Modelos originais editáveis (`.brM3`) localizados nos subdiretórios `modelagem_conceitual/` e `modelagem_logica/`.