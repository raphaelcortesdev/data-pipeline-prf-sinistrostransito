'''
Script unificado de limpeza e validação dos dados PRF para Silver Layer.

Objetivo: limpar, transformar, validar e armazenar os dados para o Data Warehouse.

Uso:
    python prf-sinistros-pipeline/src/processing/silver_process.py

O que faz:
    1. Estabelece um loop que captura qualquer arquivo .csv em data/bronze (Camada Bronze)
    2. Itera sobre cada arquivo
    3. Limpa, transforma e enriquece cada uma das colunas pré-definidas em def limpar_dados(df)
    4. Normaliza acentos em todas as colunas string (UTF-8 compatibility pra PostgreSQL)
    5. Valida o schema dos dados com Pandera
    6. Se passar na validação → salva em data/silver (Camada Silver)
    7. Se falhar na validação → registra erro no terminal e continua pro próximo arquivo
    8. Gera resumo final com sucessos e falhas
'''

import pandas as pd
import pandera as pa
from pathlib import Path
import warnings
import unicodedata

# Suprime o SettingWithCopyWarning
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# ============================================================================
# CONFIGURAÇÃO DE PATHS
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "01_bronze"
SILVER_DIR = BASE_DIR / "data" / "02_silver"

# Garante que as pastas existam antes de processar
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# DEFINIÇÃO DO SCHEMA DE VALIDAÇÃO
# ============================================================================

schema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(pa.Int32, nullable=False),
        "pesid": pa.Column(pa.Int64, nullable=False, unique=True),
        "data_hora": pa.Column(pa.DateTime, nullable=False),
        "dia_semana": pa.Column(pa.String, pa.Check.isin([
            "segunda-feira", "terca-feira", "quarta-feira", 
            "quinta-feira", "sexta-feira", "sabado", "domingo"
        ])),
        "uf": pa.Column(pa.String, nullable=False),
        "br": pa.Column(pa.Int16, pa.Check.ge(0), nullable=True),
        "km": pa.Column(float, pa.Check.ge(0), nullable=True),
        "municipio": pa.Column(pa.String, nullable=False),
        "causa_acidente": pa.Column(pa.String, nullable=True),
        "tipo_acidente": pa.Column(pa.String, nullable=True),
        "classificacao_acidente": pa.Column(pa.String, pa.Check.isin([
            "Com Vitimas Fatais", "Com Vitimas Feridas", "Sem Vitimas"
        ]), nullable=True),
        "fase_dia": pa.Column(pa.String, pa.Check.isin([
            "Amanhecer", "Pleno dia", "Anoitecer", "Plena Noite"
        ]), nullable=True),
        "sentido_via": pa.Column(pa.String, pa.Check.isin(["Crescente", "Decrescente"]), nullable=True),
        "condicao_meteorologica": pa.Column(pa.String, nullable=True),
        "tipo_pista": pa.Column(pa.String, pa.Check.isin(["Simples", "Multipla", "Dupla"]), nullable=True),
        "tracado_via": pa.Column(pa.String, nullable=True),
        "uso_solo": pa.Column(pa.String, pa.Check.isin(["Nao", "Sim"]), nullable=True),
        "id_veiculo": pa.Column(pa.Int32, nullable=True),
        "tipo_veiculo": pa.Column(pa.String, nullable=True),
        "marca": pa.Column(pa.String, nullable=True),
        "ano_fabricacao_veiculo": pa.Column(pa.Int16, pa.Check.in_range(1900, 2027), nullable=True),
        "tipo_envolvido": pa.Column(pa.String, pa.Check.isin([
            "Cavaleiro", "Condutor", "Passageiro", "Pedestre", "Testemunha"
        ]), nullable=True),
        "estado_fisico": pa.Column(pa.String, pa.Check.isin([
            "Ileso", "Lesoes Graves", "Lesoes Leves", "Obito"
        ]), nullable=True),
        "idade": pa.Column(pa.Int16, pa.Check.in_range(0, 110), nullable=True),
        "sexo": pa.Column(pa.String, pa.Check.isin(["Masculino", "Feminino"]), nullable=True),
        "ilesos": pa.Column(pa.Int8, pa.Check.isin([0, 1]), nullable=True),
        "feridos_leves": pa.Column(pa.Int8, pa.Check.isin([0, 1]), nullable=True),
        "feridos_graves": pa.Column(pa.Int8, pa.Check.isin([0, 1]), nullable=True),
        "mortos": pa.Column(pa.Int8, pa.Check.isin([0, 1]), nullable=True),
        "latitude": pa.Column(float, pa.Check.in_range(-90, 90), nullable=True),
        "longitude": pa.Column(float, pa.Check.in_range(-180, 180), nullable=True),
        "regional": pa.Column(pa.String, nullable=True),
        "delegacia": pa.Column(pa.String, nullable=True),
        "uop": pa.Column(pa.String, nullable=True)
    },
    coerce=False,
    strict=False
)

def remover_acentos(texto):
    '''
    Remove acentos e caracteres especiais de strings.
    Converte "Lesões Graves" -> "Lesoes Graves", "São Paulo" -> "Sao Paulo", etc.
    Necessário para compatibilidade UTF-8 com PostgreSQL no Windows.
    
    Args:
        texto: valor string (ou None/NaN)
        
    Returns:
        String sem acentos (ou None se entrada for nula)
    '''
    
    if pd.isna(texto):
        return texto
    if not isinstance(texto, str):
        return texto
    
    # unicodedata decompõe caracteres acentuados
    # NFD = decomposição normal (separa acentos dos caracteres)
    nfd = unicodedata.normalize('NFD', texto)
    
    # Remove diacríticos (categoria Mn = Nonspacing Mark)
    return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

# ============================================================================
# FUNÇÃO DE LIMPEZA
# ============================================================================

def limpar_dados(df):
    '''
    Limpa, transforma e enriquece os dados do DataFrame.
    
    Args:
        df: DataFrame com dados brutos do PRF
        
    Returns:
        DataFrame limpo e transformado
    '''
    
    df = df.copy()
    
    # 1) id
    # Coluna que contém o identificador único do acidente. Não pode ter nulos e deve ser int32. Pode repetir
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype("int32") #int32: não permite nulo e armazena valores de -2.147.483.648 a 2.147.483.647

    # 2) pesid
    # Coluna que contém o identificador único do envolvido no sinistro. Não pode ter nulos e deve ser int64. Não pode repetir
    df = df.drop_duplicates(subset=['pesid'], keep='first') # Remove duplicatas. o identificador é único
    df['pesid'] = pd.to_numeric(df['pesid'], errors='coerce').astype("int64") #int64: não permite nulo e armazena valores de -9.223.372.036.854.775.808 a 9.223.372.036.854.775.807

    # 3) data_inversa e horario
    # data no formato YYYY-MM-DD str. Não pode ter nulos e deve ser datetime64[ns]
    # horario no formato HH:MM:SS str
    # Concatena data_inversa e horario em uma nova coluna chamada data_hora, do tipo datetime64[ns]
    df['data_inversa'] = df['data_inversa'].str.strip() # Remove espaços em branco no início e no final da string
    df['horario'] = df['horario'].str.strip() # Remove espaços em branco no início e no final da string
    df['data_inversa'] = pd.to_datetime(df['data_inversa'], format='%Y-%m-%d') # Converte data_inversa para datetime e mantem formato YYYY-MM-DD
    df['horario'] = pd.to_timedelta(df['horario']) # Converte horario para datetime e mantem formato HH:MM:SS
    df['data_hora'] = df['data_inversa'] + df['horario'] # Concatena data_inversa e horario -> datetime + timedelta = datetime
    df = df.drop(columns=['data_inversa', 'horario']) # Remove colunas data_inversa e horario
    
    # 4) dia_semana
    # Nativamente object, valida como str
    df['dia_semana'] = df['dia_semana'].str.strip() # Remove espaços em branco no início e no final da string
    df['dia_semana'] = df['dia_semana'].apply(remover_acentos)  # Normaliza acentos em dia_semana ("terça-feira" → "terca-feira")

    # 5) uf
    # Nativamente object, valida como str
    df['uf'] = df['uf'].str.strip() # Remove espaços em branco no início e no final da string

    # 6) br
    # Armazena o número da rodovia federal. Deve ser Int16, aceita nulos e armazena valores de -32.768 a 32.767
    df['br'] = pd.to_numeric(df['br'], errors='coerce').astype("Int16") # Converte br para Int16, aceita nulos e armazena valores de -32.768 a 32.767
    df.loc[df['br'] < 0, 'br'] = pd.NA # Substitui valores negativos por nulos, caso existam
    df.loc[df['br'] == 0, 'br'] = pd.NA # Substitui valores 0 por nulos, caso existam

    # 7) km
    # Armazena o quilometragem. Deve ser float, aceita nulos e armazena valores de -1.797.693.134.862.315.7E+308 a 1.797.693.134.862.315.7E+308
    df['km'] = df['km'].str.replace(',', '.').astype(float) # substitui ',' por '.' para evitar erros de conversão, e converte para float

    # 8) municipio
    # Nativamente object, valida como str
    df['municipio'] = df['municipio'].str.strip() # Remove espaços em branco no início e no final da string
    df['municipio'] = df['municipio'].apply(remover_acentos) # Normaliza acentos em municipio

    # 9) causa_acidente
    # Nativamente object, valida como str
    df['causa_acidente'] = df['causa_acidente'].str.strip() # Remove espaços em branco no início e no final da string
    df['causa_acidente'] = df['causa_acidente'].apply(remover_acentos) # Normaliza acentos em causa_acidente

    # 10) tipo_acidente
    # Nativamente object, valida como str
    df['tipo_acidente'] = df['tipo_acidente'].str.strip() # Remove espaços em branco no início e no final da string
    df['tipo_acidente'] = df['tipo_acidente'].apply(remover_acentos) # Normaliza acentos em tipo_acidente

    # 11) classificacao_acidente
    # Nativamente object, valida como str
    df['classificacao_acidente'] = df['classificacao_acidente'].str.strip() # Remove espaços em branco no início e no final da string
    df['classificacao_acidente'] = df['classificacao_acidente'].replace('NA', pd.NA) # Substitui valores 'NA' por null
    df['classificacao_acidente'] = df['classificacao_acidente'].apply(remover_acentos) # Normaliza acentos em classificacao_acidente ("Vítimas" → "Vitimas")

    # 12) fase_dia
    # Nativamente object, valida como str
    df['fase_dia'] = df['fase_dia'].str.strip() # Remove espaços em branco no início e no final da string

    # 13) sentido_via
    # Nativamente object, valida como str
    df['sentido_via'] = df['sentido_via'].str.strip() # Remove espaços em branco no início e no final da string
    df['sentido_via'] = df['sentido_via'].replace('Não Informado', pd.NA) # Substitui valores 'Não Informado' por null
    df['sentido_via'] = df['sentido_via'].apply(remover_acentos) # Normaliza acentos em sentido_via ("Não" → "Nao")
    
    # 14) condicao_meteorologica
    # Nativamente object, valida como str
    #no csv original, o nome do campo esta como condicao_METEREOLOGICA
    df = df.rename(columns={'condicao_metereologica': 'condicao_meteorologica'}) # Renomeia
    df['condicao_meteorologica'] = df['condicao_meteorologica'].str.strip() # Remove espaços em branco no início e no final da string
    df['condicao_meteorologica'] = df['condicao_meteorologica'].replace('Ignorado', pd.NA) # Normaliza valores ausentes
    df['condicao_meteorologica'] = df['condicao_meteorologica'].apply(remover_acentos) # Normaliza acentos em condicao_metereologica

    # 15) tipo_pista
    # Nativamente object, valida como str
    df['tipo_pista'] = df['tipo_pista'].str.strip() # Remove espaços em branco no início e no final da string
    df['tipo_pista'] = df['tipo_pista'].apply(remover_acentos) # Normaliza acentos em tipo_pista ("Múltipla" → "Multipla")

    # 16) tracado_via
    # Nativamente object, valida como str
    df['tracado_via'] = df['tracado_via'].str.strip() # Remove espaços em branco no início e no final da string
    df['tracado_via'] = df['tracado_via'].apply(remover_acentos) # Normaliza acentos em tracado_via

    # 17) uso_solo
    # Nativamente object, valida como str
    df['uso_solo'] = df['uso_solo'].str.strip() # Remove espaços em branco no início e no final da string
    df['uso_solo'] = df['uso_solo'].apply(remover_acentos) # Normaliza acentos em uso_solo ("Não" → "Nao")

    # 18) id_veiculo
    df['id_veiculo'] = pd.to_numeric(df['id_veiculo'], errors='coerce').astype("Int32") # converte para Int32, que aceita nulos caso existam.

    # 19) tipo_veiculo
    # Nativamente object, valida como str
    df['tipo_veiculo'] = df['tipo_veiculo'].str.strip() # Remove espaços em branco no início e no final da string
    df['tipo_veiculo'] = df['tipo_veiculo'].apply(remover_acentos) # Normaliza acentos em tipo_veiculo

    # 20) marca
    df['marca'] = df['marca'].str.strip() # Remove espaços em branco no início e no final da string
    df['marca'] = df['marca'].apply(remover_acentos) # Normaliza acentos em marca

    # 21) ano_fabricacao_veiculo
    df['ano_fabricacao_veiculo'] = pd.to_numeric(df['ano_fabricacao_veiculo'], errors='coerce')
    df['ano_fabricacao_veiculo'] = df['ano_fabricacao_veiculo'].replace(0, pd.NA)
    df['ano_fabricacao_veiculo'] = df['ano_fabricacao_veiculo'].astype('Int16') # Converte para Int16 para aceitar possíveis valores nulos sem que o pandera converta todo o campo para float caso valores nulos existam.

    # 22) tipo_envolvido
    # Nativamente object, valida como str
    df['tipo_envolvido'] = df['tipo_envolvido'].str.strip() # Remove espaços em branco no início e no final da string
    df.loc[~df['tipo_envolvido'].isin(['Cavaleiro', 'Condutor', 'Passageiro', 'Pedestre', 'Testemunha']), 'tipo_envolvido'] = pd.NA

    # 23) estado_fisico
    # Nativamente object, valida como str
    df['estado_fisico'] = df['estado_fisico'].str.strip()
    df.loc[~df['estado_fisico'].isin(['Ileso', 'Lesões Graves', 'Lesões Leves', 'Óbito']), 'estado_fisico'] = pd.NA
    df['estado_fisico'] = df['estado_fisico'].apply(remover_acentos) # Normaliza acentos em estado_fisico ("Lesões Graves" → "Lesoes Graves", "Óbito" → "Obito")

    # 24) idade
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
    df.loc[df['idade'] > 110, 'idade'] = pd.NA # substitui idades maiores que 110 para null, se houver
    df.loc[df['idade'] < 0, 'idade'] = pd.NA # substitui idades menores que 0 para null, se houver
    df['idade'] = df['idade'].astype('Int16')

    # 25) sexo
    # Nativamente object, valida como str
    df['sexo'] = df['sexo'].str.strip() # Remove espaços em branco no início e no final da string
    df.loc[(df['sexo'] != 'Masculino') & (df['sexo'] != 'Feminino'), 'sexo'] = pd.NA # Qualquer valor que não Feminino ou Masculino vira null

    # 26) ilesos, 27) feridos_leves, 28) feridos_graves, 29) mortos
    # Colunas que recebem 0 ou 1
    # Converter para int8 após verificar que tenha apenas 0, 1, ou se o valor for diferente: null
    colunas_flags = ['ilesos', 'feridos_leves', 'feridos_graves', 'mortos']
    for col in colunas_flags:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[~df[col].isin([0, 1]), col] = pd.NA
        df[col] = df[col].astype('Int8')

    # 30) latitude, 31) longitude
    df['latitude'] = df['latitude'].astype(str).str.replace(',', '.').str.strip() # Converte para str e substitui ',' por '.' caso exista
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')  # Reconverte pra float
    df.loc[(df['latitude'] < -90) | (df['latitude'] > 90), 'latitude'] = pd.NA # Trata latitudes fora do limite (-90 a 90)
    df['latitude'] = df['latitude'].astype('Float64') # Converte para Float (F maiusculo) nativo do pandas, evita erros com pd.NA
    
    df['longitude'] = df['longitude'].astype(str).str.replace(',', '.').str.strip() # Converte para str e substitui ',' por '.' caso exista
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce') # Reconverte para float
    df.loc[(df['longitude'] < -180) | (df['longitude'] > 180), 'longitude'] = pd.NA # Trata longitudes fora do limite (-180 a 180)
    df['longitude'] = df['longitude'].astype('Float64') # Converte para Float (F maiusculo) nativo do pandas, evita erros com pd.NA

    # 32) regional, 33) delegacia, 34) uop
    df['regional'] = df['regional'].str.strip()
    # ALTERAÇÃO: Normalizar acentos em regional
    df['regional'] = df['regional'].apply(remover_acentos)
    
    df['delegacia'] = df['delegacia'].str.strip()
    # ALTERAÇÃO: Normalizar acentos em delegacia
    df['delegacia'] = df['delegacia'].apply(remover_acentos)
    
    df['uop'] = df['uop'].str.strip()
    # ALTERAÇÃO: Normalizar acentos em uop
    df['uop'] = df['uop'].apply(remover_acentos)
    
    return df

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    '''
    Processa todos os CSVs da camada Bronze, executa limpeza, normalização de acentos e validação,
    e salva os dados validados na Silver layer - data/02_silver .
    '''
    
    sucessos = 0
    falhas = 0
    arquivos_com_erro = []

    print(f"Iniciando processamento de arquivos da Bronze layer...\n")
    print("-" * 70)

    for arquivo_csv in sorted(BRONZE_DIR.glob("*.csv")):
        try:
            # Lê o CSV com os parâmetros corretos
            df = pd.read_csv(arquivo_csv, delimiter=';', encoding='latin1', low_memory=False)

            # 1. LIMPEZA DOS DADOS
            df_limpo = limpar_dados(df)

            # 2. VALIDAÇÃO COM PANDERA
            schema.validate(df_limpo)

            # 3. SALVA EM SILVER SE PASSOU NA VALIDAÇÃO
            output_path = SILVER_DIR / arquivo_csv.with_suffix('.parquet').name
            
            if output_path.exists():
                print(f"⚠️  {output_path.name} já existe em data/silver/, sobrescrevendo...")

            df_limpo.to_parquet(output_path, index=False)
            print(f"✅ {arquivo_csv.name} → Silver layer (validado com sucesso!)")
            
            sucessos += 1

        except Exception as e:
            print(f"❌ {arquivo_csv.name} → FALHOU na validação")
            print(f"   Erro: {str(e)[:100]}...")
            falhas += 1
            arquivos_com_erro.append(arquivo_csv.name)

    print("-" * 70)
    print("\n" + "=" * 70)
    print("📊 RESUMO DO PROCESSAMENTO DA SILVER LAYER")
    print("=" * 70)
    print(f"✅ Sucessos (Silver): {sucessos}")
    print(f"❌ Falhas: {falhas}")
    
    if arquivos_com_erro:
        print(f"\n⚠️  Arquivos com erro (não salvos em Silver):")
        for arquivo in arquivos_com_erro:
            print(f"   - {arquivo}")
    else:
        print(f"\n🎉 Todos os arquivos foram processados e salvos com sucesso!")

    print("=" * 70)

if __name__ == "__main__":
    main()