'''
Script unificado de limpeza e validação dos dados PRF para Silver Layer.
Objetivo: limpar, transformar, validar e armazenar os dados para o Data Warehouse.
'''

import pandas as pd
import pandera as pa
from pathlib import Path
import warnings
import unicodedata
import argparse

# Suprime o SettingWithCopyWarning
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# ============================================================================
# CONFIGURAÇÃO DE PATHS
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# DEFINIÇÃO DO SCHEMA DE VALIDAÇÃO
# ============================================================================
schema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(pa.Int32, nullable=False),
        "pesid": pa.Column(pa.Int64, nullable=False, unique=True),
        "data_hora": pa.Column(pa.DateTime, nullable=True),
        "dia_semana": pa.Column(pa.String, pa.Check.isin([
            "segunda-feira", "terca-feira", "quarta-feira", 
            "quinta-feira", "sexta-feira", "sabado", "domingo"
        ])),
        "uf": pa.Column(pa.String, nullable=True),
        "br": pa.Column(pa.Int16, pa.Check.ge(0), nullable=True),
        "km": pa.Column(float, pa.Check.ge(0), nullable=True),
        "municipio": pa.Column(pa.String, nullable=True),
        "causa_acidente": pa.Column(pa.String, nullable=True),
        "tipo_acidente": pa.Column(pa.String, nullable=True),
        "classificacao_acidente": pa.Column(pa.String, pa.Check.isin([
            "com vitimas fatais", "com vitimas feridas", "sem vitimas"
        ]), nullable=True),
        "fase_dia": pa.Column(pa.String, pa.Check.isin([
            "amanhecer", "pleno dia", "anoitecer", "plena noite"
        ]), nullable=True),
        "sentido_via": pa.Column(pa.String, pa.Check.isin(["crescente", "decrescente"]), nullable=True),
        "condicao_meteorologica": pa.Column(pa.String, nullable=True),
        "tipo_pista": pa.Column(pa.String, pa.Check.isin(["simples", "multipla", "dupla"]), nullable=True),
        "tracado_via": pa.Column(pa.String, nullable=True),
        "uso_solo": pa.Column(pa.String, pa.Check.isin(["nao", "sim", "urbano", "rural"]), nullable=True),
        "id_veiculo": pa.Column(pa.Int32, nullable=True),
        "tipo_veiculo": pa.Column(pa.String, nullable=True),
        "marca": pa.Column(pa.String, nullable=True),
        "ano_fabricacao_veiculo": pa.Column(pa.Int16, pa.Check.in_range(1900, 2027), nullable=True),
        "tipo_envolvido": pa.Column(pa.String, pa.Check.isin([
            "cavaleiro", "condutor", "passageiro", "pedestre", "testemunha"
        ]), nullable=True),
        "estado_fisico": pa.Column(pa.String, pa.Check.isin([
            "ileso", "lesoes graves", "lesoes leves", "obito"
        ]), nullable=True),
        "idade": pa.Column(pa.Int16, pa.Check.in_range(0, 110), nullable=True),
        "sexo": pa.Column(pa.String, pa.Check.isin(["masculino", "feminino"]), nullable=True),
        "latitude": pa.Column(float, pa.Check.in_range(-90, 90), nullable=True),
        "longitude": pa.Column(float, pa.Check.in_range(-180, 180), nullable=True),
        "regional": pa.Column(pa.String, nullable=True),
        "delegacia": pa.Column(pa.String, nullable=True),
        "uop": pa.Column(pa.String, nullable=True)
    },
    coerce=False,
    strict=False
)

# ============================================================================
# NORMALIZAÇÃO DE ACENTOS
# ============================================================================
def remover_acentos(texto):
    '''
        Função que normaliza qualquer acento ou caracter especial
    '''

    if pd.isna(texto) or not isinstance(texto, str):
        return texto
    nfd = unicodedata.normalize('NFD', texto)
    return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')

# ============================================================================
# EQUIVALÊNCIA DO LEGADO (2007-2016)
# ============================================================================
def aplicar_equivalencia(df):
    '''
    Trata exclusivamente os problemas e a formatação dos dados legado (2007-2016).
    O objetivo é deixar as colunas e domínios estruturalmente compatíveis com 2017+.
    '''
    df = df.copy()

    # ================ PROCEDIMENTOS TEXTUAIS ================

    # 1. Remove colunas obsoletas descontinuadas em 2017+
    df = df.drop(columns=['nacionalidade', 'naturalidade'], errors='ignore')

    # 2. Tratamento massivo da string literal '(null)' que infestava o legado
    df = df.replace('(null)', pd.NA)

    # 3. Dia da Semana: substitui o padrão legado pelo padrão adotado a partir de 2017
    if 'dia_semana' in df.columns:
        alt_dia = {
            'Domingo': 'domingo', 'Segunda': 'segunda-feira', 'Terca': 'terca-feira', 'Terça': 'terca-feira',
            'Quarta': 'quarta-feira', 'Quinta': 'quinta-feira', 'Sexta': 'sexta-feira', 'Sabado': 'sabado', 'Sábado': 'sabado'
        }
        df['dia_semana'] = df['dia_semana'].astype('string').str.strip().map(alt_dia).fillna(df['dia_semana'])

    # 4. Estado Físico: substitui o padrão legado pelo padrão adotado a partir de 2017
    if 'estado_fisico' in df.columns:
        alt_estado = {
            'Ferido Leve': 'Lesões Leves', 'Ferido Grave': 'Lesões Graves', 
            'Morto': 'Óbito', 'Ignorado': pd.NA
        }
        df['estado_fisico'] = df['estado_fisico'].astype('string').str.strip().map(alt_estado).fillna(df['estado_fisico'])

    # 5. Sexo: substitui o padrão legado pelo padrão adotado a partir de 2017
    if 'sexo' in df.columns:
        alt_sexo = {
            'M': 'Masculino', 'F': 'Feminino', 
            'Não Informado': pd.NA, 'Nao Informado': pd.NA, 
            'Inválido': pd.NA, 'Invalido': pd.NA, 'I': pd.NA
        }
        df['sexo'] = df['sexo'].astype('string').str.strip().map(alt_sexo).fillna(df['sexo'])
        
    # 6. Condição Meteorológica: substitui dados faltantes por nulos pandas
    if 'condicao_metereologica' in df.columns:
        df['condicao_metereologica'] = df['condicao_metereologica'].replace(['Ignorada', 'Ignorado'], pd.NA)

    # 7. Marca: substitui sujeiras (campos apenas com asteriscos) por nuloos pandas
    if 'marca' in df.columns:
        df['marca'] = df['marca'].astype('string').replace(r'^\*+$', pd.NA, regex=True)

    # 8. Classificação de Acidente: substitui o padrão legado pelo padrão adotado a partir de 2017
    if 'classificacao_acidente' in df.columns:
        df['classificacao_acidente'] = df['classificacao_acidente'].astype('string').str.strip()
        alt_classificacao = {
            'Com Vítimas Fatais': 'com vitimas fatais',
            'Com Vítimas Feridas': 'com vitimas feridas',
            'Sem Vítimas': 'sem vitimas',
            'Ignorado': pd.NA,
            '(null)': pd.NA
        }
        df['classificacao_acidente'] = df['classificacao_acidente'].map(alt_classificacao).fillna(pd.NA)

    # 9. uso_solo: padroniza coluna para formato 2017+: substitui 'Urbano' por 'sim', e 'Rural' por 'nao'
    df.loc[df['uso_solo'].str.strip().str.lower() == 'urbano', 'uso_solo'] = 'sim'
    df.loc[df['uso_solo'].str.strip().str.lower() == 'rural', 'uso_solo'] = 'nao'


    # ================ PROCEDIMENTOS NUMÉRICOS ================

    # 9. Horário: tratamento exclusivo da formatação de horário antigo
    if 'horario' in df.columns:
        df['horario'] = df['horario'].astype('string').str.strip()
        df['horario'] = df['horario'].apply(lambda x: f"{x}:00" if len(x) == 5 else x)
        
    # 10. Tratamento exclusivo da data legado (pode vir como DD/MM/YYYY ou DD/MM/YY)
    if 'data_inversa' in df.columns:
        # Em 2016 o campo 'ano' vem no formato YY, foi preciso adaptar para que o script aceite tambem esse formato e converta para o padrão 2017+
        df['data_inversa'] = pd.to_datetime(df['data_inversa'], dayfirst=True, errors='coerce')
        df['data_inversa'] = df['data_inversa'].dt.strftime('%Y-%m-%d')

    # 11. Cria campos 'latitude', 'longitude', 'regional', 'delegacia' e 'uop' com valores nulos, adotados a partir de 2017
    df['latitude'] = pd.NA
    df['latitude'] = df['latitude'].astype('Float64')
    df['longitude'] = pd.NA
    df['longitude'] = df['longitude'].astype('Float64')
    df['regional'] = pd.NA
    df['regional'] = df['regional'].astype('string')
    df['delegacia'] = pd.NA
    df['delegacia'] = df['delegacia'].astype('string')
    df['uop'] = pd.NA
    df['uop'] = df['uop'].astype('string')

    return df

# ============================================================================
# LIMPEZA DOS DADOS
# ============================================================================
def limpar_dados(df):
    '''
    Limpeza, formatação e validação técnica aplicada a TODOS os anos (2007-2025).
    '''
    df = df.copy()
    
    # ================ PROCEDIMENTOS TEXTUAIS ================

    # 1. Correção Universal de Nome de Coluna (Erro da PRF)
    df = df.rename(columns={'condicao_metereologica': 'condicao_meteorologica'})
    
    # 2. Processa dinamicamente todos os campos identificados como strings no schema de validação (schema.columns.itens())
    colunas_string = [nome for nome, coluna in schema.columns.items() if 'str' in str(coluna.dtype).lower()]
    
    for col in colunas_string:
        if col in df.columns:
            df[col] = df[col].astype('string').str.strip().replace('', pd.NA) # cast para string, remove espaços e troca vazios por Nulo
            df[col] = df[col].apply(remover_acentos) # remove acentos e caracteres especiais
            df[col] = df[col].replace('nan', pd.NA) # Trata possiveis 'nan' que surgirem apos cast para string e substitui por nulo pandas

            # Aplica upper() em UF e lower() em todos os outros campos
            if col == 'uf':
                df[col] = df[col].str.upper()
            else:
                df[col] = df[col].str.lower()
                
    # 3. limpezas finais baseadas em campos e valores específicos

    if 'sentido_via' in df.columns:
        df['sentido_via'] = df['sentido_via'].replace(['nao informado'], pd.NA)
        
    if 'classificacao_acidente' in df.columns:
        df['classificacao_acidente'] = df['classificacao_acidente'].replace('na', pd.NA)
        
    if 'tipo_envolvido' in df.columns:
        df.loc[~df['tipo_envolvido'].isin(['cavaleiro', 'condutor', 'passageiro', 'pedestre', 'testemunha']), 'tipo_envolvido'] = pd.NA
        
    if 'estado_fisico' in df.columns:
        # Tudo o que não for ['ileso', 'lesoes graves', 'lesoes leves', 'obito'] vira pd.NA em estado_fisico. Em dados legados há sujeiras que precisam ser tratadas
        # Os valores já estão sem acento devido à etapa 2
        df.loc[~df['estado_fisico'].isin(['ileso', 'lesoes graves', 'lesoes leves', 'obito']), 'estado_fisico'] = pd.NA 
        
    if 'sexo' in df.columns:
        # Tudo o que não for ['masculino', 'feminino'] vira pd.NA em sexo
        df.loc[~df['sexo'].isin(['masculino', 'feminino']), 'sexo'] = pd.NA

    # ================ PROCEDIMENTOS NUMÉRICOS ====================

    # 4. Tratamento Universal de Datas e horários
    df['data_inversa'] = pd.to_datetime(df['data_inversa'], format='%Y-%m-%d', errors='coerce')
    df['horario'] = pd.to_timedelta(df['horario'], errors='coerce')
    df['data_hora'] = df['data_inversa'] + df['horario'] # Concatena em um campo unico
    df = df.drop(columns=['data_inversa', 'horario'], errors='ignore') # Dropa colunas anteriores

    # 5. Formatações Numéricas gerais
    # Casta para o tipo específico (Int16, Int32, Int64) visando otimizar espaço de armazenamento
    # Faz validações para respeitar regra de negocio e eliminar sujeiras em parquet e no DW
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype("Int32")
    
    df = df.drop_duplicates(subset=['pesid'], keep='first')
    df['pesid'] = pd.to_numeric(df['pesid'], errors='coerce').astype("Int64")
    
    df['id_veiculo'] = pd.to_numeric(df['id_veiculo'], errors='coerce').astype("Int32")
    
    df['br'] = pd.to_numeric(df['br'], errors='coerce').astype("Int16")
    df.loc[df['br'] <= 0, 'br'] = pd.NA
    
    df['km'] = df['km'].astype('string').str.replace(',', '.').astype(float)
    df.loc[df['km'] <= 0, 'km'] = pd.NA
    
    df['ano_fabricacao_veiculo'] = pd.to_numeric(df['ano_fabricacao_veiculo'], errors='coerce')
    df.loc[(df['ano_fabricacao_veiculo'] < 1900) | (df['ano_fabricacao_veiculo'] > 2027), 'ano_fabricacao_veiculo'] = pd.NA
    df['ano_fabricacao_veiculo'] = df['ano_fabricacao_veiculo'].astype('Int16')
    
    df['idade'] = pd.to_numeric(df['idade'], errors='coerce')
    df.loc[(df['idade'] > 110) | (df['idade'] < 0), 'idade'] = pd.NA
    df['idade'] = df['idade'].astype('Int16')
    
    # Tratamento Geográfico (Float64 Pandas)
    for col in ['latitude', 'longitude']:
        if col in df.columns:
            df[col] = df[col].astype('string').str.replace(',', '.').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df.loc[(df['latitude'] < -90) | (df['latitude'] > 90), 'latitude'] = pd.NA
    df.loc[(df['longitude'] < -180) | (df['longitude'] > 180), 'longitude'] = pd.NA
    
    df['latitude'] = df['latitude'].astype('Float64')
    df['longitude'] = df['longitude'].astype('Float64')

    # 6. Remove colunas redundantes
    df = df.drop(columns=['ilesos', 'feridos_leves', 'feridos_graves', 'mortos'], errors='ignore')
    
    return df

# ============================================================================
# ARGUMENTOS
# ============================================================================
def obter_anos_filtrados():
    ''' 
        Define argumentos para realizar o processamento de apenas um ano ou um intervalo de anos, ao inves de carregar todos os anos.
        Ex: 
            python silver_process.py --anos 2008 -> processamento o carregamento apenas de 2008
            python silver_process.py --anos 2013-2016 -> Realiza o processamento apenas dos anos de 2013 a 2016
            retorna None se nenhum argumento for usado -> processa tudo       
    '''
    parser = argparse.ArgumentParser(description="Processador Silver PRF")
    parser.add_argument('--anos', type=str, help="Ano único (ex: 2016) ou intervalo (ex: 2016-2019)", default=None)
    args = parser.parse_args()
    
    if args.anos:
        if '-' in args.anos:
            inicio, fim = map(int, args.anos.split('-'))
            return list(range(inicio, fim + 1))
        else:
            return [int(args.anos)]
    return None

# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    anos_filtrados = obter_anos_filtrados()
    
    sucessos = 0
    falhas = 0
    arquivos_com_erro = []

    print(f"Iniciando processamento de arquivos da Bronze layer...\n")
    if anos_filtrados:
        print(f"🎯 Filtro ativado: Processando apenas os anos {anos_filtrados}\n")
    print("-" * 70)

    for arquivo_csv in sorted(BRONZE_DIR.glob("*.csv")):
        try:
            # 1. Extrai o ano do nome do arquivo (ex: 'acidentes2016.csv' -> 2016)
            ano = int(arquivo_csv.stem[-4:])
            
            if anos_filtrados and ano not in anos_filtrados:
                continue
            
            # 2. Define o delimitador com base no ano que acabou de extrair
            # Até 2015 é ',' / 2016 em diante é ';'
            sep = ',' if ano <= 2015 else ';'
            
            # 3. Lê o CSV usando o delimitador correto
            df = pd.read_csv(arquivo_csv, delimiter=sep, encoding='latin1', low_memory=False)

            # 4. Se for legado (<= 2016), aciona o tradutor para depois entrar na limpeza universal
            if ano <= 2016:
                df = aplicar_equivalencia(df)

            # 5. Executa a limpeza universal
            df_limpo = limpar_dados(df)

            # 6. Validação
            schema.validate(df_limpo)

            # 7. Salva em Parquet
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