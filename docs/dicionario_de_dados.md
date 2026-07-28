# Dicionário de Dados do Projeto

Este documento detalha o significado, a origem e os tratamentos aplicados a cada variável presente no pipeline de dados de acidentes da Polícia Rodoviária Federal (PRF).

---

# 1. Dados Brutos (Camada Bronze)

Os dados originais (formato `.csv`) são extraídos diretamente do portal de Dados Abertos da Polícia Rodoviária Federal (PRF).

A documentação oficial está disponível em:

🔗 **Dicionário de Acidentes - PRF (Portal Gov.br)**

## Dicionários oficiais disponibilizados pela PRF

- Dicionário de variáveis - acidentes agrupados por ocorrência (até 2016)
- Dicionário de variáveis - acidentes agrupados por pessoa (até 2016)
- Dicionário de variáveis - acidentes agrupados por pessoa (2017 em diante)
- Dicionário de variáveis - acidentes agrupados por pessoa com todas as causas e tipos de acidentes (2017 em diante)
- Dicionário de variáveis - acidentes agrupados por ocorrência (2017 em diante)

## Bases utilizadas neste projeto

O pipeline unifica todos os dados utilizando a granularidade de **acidentes agrupados por pessoa**.

Assim, os dicionários oficiais utilizados como referência para a camada **Bronze** são:

- **Dicionário de variáveis - acidentes agrupados por pessoa (até 2016)**
- **Dicionário de variáveis - acidentes agrupados por pessoa (2017 em diante)**

---

# 2. Dados Processados (Camadas Silver e Gold)

A tabela abaixo descreve o schema final após a execução das etapas de:

- sanitização;
- padronização entre bases legadas;
- validação de dados (`silver_process.py`).

O schema corresponde exatamente à estrutura:

- salva nos arquivos **Parquet (Silver)**;
- carregada no banco de dados relacional **(Gold)**.

| Coluna | Tipo | Descrição / Domínio | Regras aplicadas na camada Silver |
|---------|------|---------------------|-----------------------------------|
| `id` | Int32 | Identificador da ocorrência do acidente. | Convertido para numérico. Valores inválidos tornam-se `NULL`. |
| `pesid` | Int64 | Identificador único da pessoa envolvida. | Duplicatas removidas mantendo a primeira ocorrência. Chave primária do banco. |
| `data_hora` | DateTime | Data e horário do acidente. | Concatenação de `data_inversa` + `horario` e conversão para DateTime. |
| `dia_semana` | String | segunda-feira, terca-feira, quarta-feira, quinta-feira, sexta-feira, sabado, domingo | Padronização para caixa baixa, remoção de acentos e tradução do legado. |
| `uf` | String | Unidade Federativa. | Caixa alta (`upper()`), remoção de espaços e tratamento de `(null)`. |
| `br` | Int16 | Número da rodovia federal. | Valores ≤ 0 tornam-se `NULL`. |
| `km` | Float64 | Quilômetro da rodovia. | Troca vírgula por ponto. Valores ≤ 0 tornam-se `NULL`. |
| `municipio` | String | Município da ocorrência. | Caixa baixa, remoção de acentos, `strip()` e tratamento de nulos. |
| `causa_acidente` | String | Causa principal do acidente. | Caixa baixa, remoção de acentos, `strip()` e tratamento de nulos. |
| `tipo_acidente` | String | Tipo/Natureza do acidente. | Caixa baixa, remoção de acentos, `strip()` e tratamento de nulos. |
| `classificacao_acidente` | String | com vitimas fatais, com vitimas feridas, sem vitimas | Padronização do legado. Valores `"na"` tornam-se `NULL`. |
| `fase_dia` | String | amanhecer, pleno dia, anoitecer, plena noite | Caixa baixa, remoção de acentos e tratamento de nulos. |
| `sentido_via` | String | crescente, decrescente | Caixa baixa. `"nao informado"` torna-se `NULL`. |
| `condicao_meteorologica` | String | Condição climática no momento do acidente. | Corrige o nome legado `condicao_metereologica`. Valores ignorados tornam-se `NULL`. |
| `tipo_pista` | String | simples, dupla, multipla | Caixa baixa, remoção de acentos e tratamento de nulos. |
| `tracado_via` | String | Traçado da rodovia (reta, curva, etc.). | Caixa baixa, remoção de acentos e tratamento de nulos. |
| `uso_solo` | String | sim, nao | Caixa baixa, remoção de acentos e tratamento de nulos. 'sim' significa Urbano, 'nao' significa Rural |
| `id_veiculo` | Int32 | Identificador do veículo. | Conversão para numérico. Valores inválidos tornam-se `NULL`. |
| `tipo_veiculo` | String | Categoria do veículo. | Caixa baixa, remoção de acentos e tratamento de nulos. |
| `marca` | String | Marca ou modelo do veículo. | Valores compostos apenas por `*****` tornam-se `NULL`. |
| `ano_fabricacao_veiculo` | Int16 | Ano de fabricação (1900–2027). | Valores fora do intervalo tornam-se `NULL`. |
| `tipo_envolvido` | String | cavaleiro, condutor, passageiro, pedestre, testemunha | Valores fora do domínio permitido tornam-se `NULL`. |
| `estado_fisico` | String | ileso, lesoes leves, lesoes graves, obito | Padronização do legado. Valores fora do domínio tornam-se `NULL`. |
| `idade` | Int16 | Idade (0–110 anos). | Valores fora do intervalo tornam-se `NULL`. |
| `sexo` | String | masculino, feminino | Conversão do legado (`M`, `F`, `Inválido`). Valores inválidos tornam-se `NULL`. |
| `latitude` | Float64 | Latitude (-90 a 90). | Troca vírgula por ponto. Coordenadas fora dos limites do Brasil tornam-se `NULL`. |
| `longitude` | Float64 | Longitude (-180 a 180). | Troca vírgula por ponto. Coordenadas fora dos limites do Brasil tornam-se `NULL`. |
| `regional` | String | Superintendência Regional da PRF. | Caixa baixa, remoção de acentos. Para bases até 2016 o valor é sempre `NULL`. |
| `delegacia` | String | Delegacia responsável. | Caixa baixa, remoção de acentos. Para bases até 2016 o valor é sempre `NULL`. |
| `uop` | String | Unidade Operacional Policial. | Caixa baixa, remoção de acentos. Para bases até 2016 o valor é sempre `NULL`. |

---

# Regras Universais de Tratamento

Todas as colunas do tipo **String** passaram pelas seguintes etapas de padronização:

- remoção de espaços em branco (`strip()`);
- conversão para caixa baixa (quando aplicável);
- remoção de acentos;
- conversão da string literal `"null"` para valor nulo;
- conversão de strings vazias para `pd.NA`;
- conversão de valores `NaN` do pandas para `pd.NA`.

---

# Colunas Descontinuadas

As seguintes colunas existentes nas bases legadas foram removidas durante o pipeline por terem sido descontinuadas pela PRF a partir de 2017:

- `nacionalidade`
- `naturalidade`

As seguintes colunas foram removidas durante o processamento para a camada silver pois apresentavam informações redundantes que facilmente são obtidas através de simples agregações em linguagem `SQL`. A remoção foi feita pensando em otimização de espaço de armazenamento e capacidade de processamento: 
- `ilesos`
- `feridos_leves`
- `feridos_graves`
- `mortos`

Essas variáveis não fazem parte do schema final das camadas **Silver** e **Gold**.