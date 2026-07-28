import psycopg2
from psycopg2 import sql, extras
from pathlib import Path
import sys
import os
import time
from dotenv import load_dotenv
import pandas as pd
import argparse

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
LOGS_DIR = BASE_DIR / "data" / "logs"

load_dotenv()

# ============================================================================
# CONFIGURAÇÃO DE CONEXÃO
# ============================================================================
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'prf_dw')
}

# ============================================================================
# BATCH INSERTER
# ============================================================================
def execute_batch_insert(cursor, table, columns, data, conflict_keys):
    """Função genérica para inserir dados únicos em lotes, acelerando o processo.
       Ignorando conflitos de linhas já existentes (ON CONFLICT DO NOTHING).
    """
    if not data:
        return 0
    
    col_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_cols = ", ".join(conflict_keys)
    
    query = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO NOTHING
    """
    psycopg2.extras.execute_batch(cursor, query, data, page_size=5000)
    return len(data)

# ============================================================================
# CARREGAMENTO DAS TABELAS DIMENSÃO
# ============================================================================
def load_dimensions(conn, cursor, anos_filtrados):
    '''
    Função que valida e tranforma tipos pandas para tipos nativos de python e faz o insert em batch no Data Warehouse.
    É preciso limpar e validar novamente pois o psycopg2 tem problemas de compatibilidade com tipagem não nativa python.
    '''
    for parquet in sorted(SILVER_DIR.glob('*.parquet')):
        
        # --- NOVA REGRA DE FILTRO AQUI ---
        ano = int(parquet.stem.replace('acidentes', ''))
        if anos_filtrados and ano not in anos_filtrados:
            continue
        # ---------------------------------
        
        print(f"\n⌛ Processando dimensões do arquivo {parquet.name}...")
        start_time = time.time()
        
        df = pd.read_parquet(parquet)
        
        # 1. DIM_PESSOA
        df_pessoa = df[['id', 'pesid', 'idade', 'sexo', 'tipo_envolvido', 'estado_fisico']].dropna(subset=['id', 'pesid']).drop_duplicates(subset=['id', 'pesid'])
        dados_pessoa = df_pessoa.astype(object).where(pd.notnull(df_pessoa), None).values.tolist()
        execute_batch_insert(cursor, 'dim_pessoa', 
                             ['id_acidente_original', 'pesid_original', 'idade', 'sexo', 'tipo_envolvido', 'estado_fisico'], 
                             dados_pessoa, ['id_acidente_original', 'pesid_original'])

        # 2. DIM_TEMPO
        df_tempo = df[['data_hora', 'dia_semana', 'fase_dia']].dropna(subset=['data_hora']).drop_duplicates(subset=['data_hora'])
        dados_tempo = df_tempo.astype(object).where(pd.notnull(df_tempo), None).values.tolist()
        execute_batch_insert(cursor, 'dim_tempo', 
                             ['data_hora', 'dia_semana', 'fase_dia'], 
                             dados_tempo, ['data_hora'])

        # 3. DIM_LOCAL
        df_local = df[['uf', 'municipio', 'br', 'km', 'latitude', 'longitude', 'regional', 'delegacia', 'uop']].dropna(subset=['uf', 'municipio', 'br', 'km']).drop_duplicates(subset=['uf', 'municipio', 'br', 'km'])
        dados_local = df_local.astype(object).where(pd.notnull(df_local), None).values.tolist()
        execute_batch_insert(cursor, 'dim_local', 
                             ['uf', 'municipio', 'br', 'km', 'latitude', 'longitude', 'regional', 'delegacia', 'uop'], 
                             dados_local, ['uf', 'municipio', 'br', 'km'])

        # 4. DIM_PISTA
        df_pista = df[['tipo_pista', 'sentido_via', 'tracado_via', 'uso_solo']].dropna().drop_duplicates()
        dados_pista = df_pista.astype(object).where(pd.notnull(df_pista), None).values.tolist()
        execute_batch_insert(cursor, 'dim_pista', 
                             ['tipo_pista', 'sentido_via', 'tracado_via', 'uso_solo'], 
                             dados_pista, ['tipo_pista', 'sentido_via', 'tracado_via', 'uso_solo'])

        # 5. DIM_CLIMA
        df_clima = df[['condicao_meteorologica']].dropna().drop_duplicates()
        dados_clima = df_clima.astype(object).where(pd.notnull(df_clima), None).values.tolist()
        execute_batch_insert(cursor, 'dim_clima', 
                             ['condicao_meteorologica'], 
                             dados_clima, ['condicao_meteorologica'])

        # 6. DIM_CLASSIFICACAO
        df_classificacao = df[['tipo_acidente', 'causa_acidente', 'classificacao_acidente']].dropna().drop_duplicates()
        dados_classificacao = df_classificacao.astype(object).where(pd.notnull(df_classificacao), None).values.tolist()
        execute_batch_insert(cursor, 'dim_classificacao', 
                             ['tipo_acidente', 'causa_acidente', 'classificacao_acidente'], 
                             dados_classificacao, ['tipo_acidente', 'causa_acidente', 'classificacao_acidente'])

        # 7. DIM_VEICULO
        df_veiculo = df[['id', 'id_veiculo', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']].dropna(subset=['id_veiculo']).drop_duplicates(subset=['id', 'id_veiculo'])
        dados_veiculo = df_veiculo.astype(object).where(pd.notnull(df_veiculo), None).values.tolist()
        execute_batch_insert(cursor, 'dim_veiculo', 
                             ['id_acidente_original', 'id_veiculo_original', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo'], 
                             dados_veiculo, ['id_acidente_original', 'id_veiculo_original'])

        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"✅ Dimensões populadas em {elapsed_time:.2f} segundos para {parquet.name}!")

# ============================================================================
# CARREGAMENTO DA TABELA FATO
# ============================================================================
def insert_fato(conn, cursor, anos_filtrados):
    ''' Função que carrega dados na tabela fato.
        Transforma tabelas dim recem criadas por load_dimensions() em dict python para busca instantânea
    '''

    print("\n🚀 Carregando dimensões em memória (Cache) para a tabela Fato...")
    start_cache_time = time.time()
    
    cursor.execute("SELECT id_acidente_original, pesid_original, pk_pessoa FROM dim_pessoa")
    cache_pessoa = {(row[0], row[1]): row[2] for row in cursor.fetchall()}

    cursor.execute("SELECT data_hora, id_tempo FROM dim_tempo")
    cache_tempo = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT uf, municipio, br, km, id_local FROM dim_local")
    cache_local = {(row[0], row[1], row[2], row[3]): row[4] for row in cursor.fetchall()}

    cursor.execute("SELECT tipo_pista, sentido_via, tracado_via, uso_solo, id_estrada FROM dim_pista")
    cache_pista = {(row[0], row[1], row[2], row[3]): row[4] for row in cursor.fetchall()}

    cursor.execute("SELECT condicao_meteorologica, id_clima FROM dim_clima")
    cache_clima = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT tipo_acidente, causa_acidente, classificacao_acidente, id_classificacao FROM dim_classificacao")
    cache_classificacao = {(row[0], row[1], row[2]): row[3] for row in cursor.fetchall()}

    cursor.execute("SELECT id_acidente_original, id_veiculo_original, pk_veiculo FROM dim_veiculo")
    cache_veiculo = {(row[0], row[1]): row[2] for row in cursor.fetchall()}

    print(f"✅ Cache carregado em {time.time() - start_cache_time:.2f} segundos. Iniciando inserções...\n")

    total_fatos_inseridos = 0

    for parquet in sorted(SILVER_DIR.glob('*.parquet')):
        
        ano = int(parquet.stem.replace('acidentes', ''))
        if anos_filtrados and ano not in anos_filtrados:
            continue
        
        print(f"⌛ Populando tabela fato com os dados de {ano}...")
        start_time = time.time()
        
        df = pd.read_parquet(parquet)
        df = df.where(pd.notnull(df), None)
        
        lote_fatos = []
        linhas_ignoradas = 0

        # Lista para armazenar linhas rejeitadas para posterior salvamento em 'data/logs'
        dados_rejeitados = []
        
        for row in df.itertuples(index=False):
            data_hora = row.data_hora.to_pydatetime() if row.data_hora is not None else None
            
            fk_pesid = cache_pessoa.get((row.id, row.pesid))
            fk_tempo = cache_tempo.get(data_hora)
            fk_local = cache_local.get((row.uf, row.municipio, row.br, row.km))
            
            # Lógica para captura de linhas rejeitadas 
            if not fk_pesid or not fk_tempo or not fk_local:
                linhas_ignoradas += 1
                
                # Identifica qual dimensão falhou
                motivos = []
                if not fk_pesid: motivos.append("Pessoa (id/pesid não encontrados)")
                if not fk_tempo: motivos.append("Tempo (data nula ou não encontrada)")
                if not fk_local: motivos.append("Local (uf/municipio/br/km não encontrados)")
                
                # Converte a tupla para dicionário e adiciona o motivo da rejeição
                row_dict = row._asdict()
                row_dict['motivo_rejeicao'] = " | ".join(motivos)
                dados_rejeitados.append(row_dict)
                continue

            fk_estrada = cache_pista.get((row.tipo_pista, row.sentido_via, row.tracado_via, row.uso_solo))
            fk_clima = cache_clima.get(row.condicao_meteorologica)
            fk_classificacao = cache_classificacao.get((row.tipo_acidente, row.causa_acidente, row.classificacao_acidente))
            fk_veiculo = cache_veiculo.get((row.id, row.id_veiculo))

            lote_fatos.append((fk_pesid, fk_tempo, fk_local, fk_estrada, fk_clima, fk_classificacao, fk_veiculo))

        qtd_registros = len(lote_fatos)
        total_fatos_inseridos += qtd_registros

        if lote_fatos:
            query_insert = '''
                INSERT INTO fato (fk_pesid, fk_tempo, fk_local, fk_estrada, fk_clima, fk_classificacao, fk_veiculo)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fk_pesid, fk_tempo, fk_local) DO NOTHING
            '''
            psycopg2.extras.execute_batch(cursor, query_insert, lote_fatos, page_size=5000)
            conn.commit()
            
        elapsed_time = time.time() - start_time
        qtd_formatada = f"{qtd_registros:,}".replace(",", ".")
        print(f"✅ Tabela fato concluída para {ano}: {qtd_formatada} registros processados em {elapsed_time:.2f} segundos.")
        
        # Salva linhas rejeitadas em LOGS_DIR
        if dados_rejeitados:
            df_rejeitados = pd.DataFrame(dados_rejeitados)
            nome_arquivo_log = LOGS_DIR / f"rejeitados_{ano}.csv"
            df_rejeitados.to_csv(nome_arquivo_log, index=False, sep=';', encoding='utf-8-sig')
            print(f"⚠️  Aviso: {linhas_ignoradas} linhas puladas. Detalhes salvos em data/logs/{nome_arquivo_log.name} para auditoria.")

    return total_fatos_inseridos

# ============================================================================
# ARGUMENTOS
# ============================================================================
def obter_anos_filtrados():
    ''' Define argumentos para realizar o carregamento de apenas um ano ou um intervalo de anos, ao inves de carregar todos os anos.
        Ex: 
            python load.py --anos 2008 -> Realiza o carregamento apenas de 2008
            python load.py --anos 2013-2016 -> Realiza o carregamento apenas dos anos de 2013 a 2016
            retorna None se nenhum argumento for usado -> carrega tudo        
    '''
    parser = argparse.ArgumentParser(description="Carregador Data Warehouse PRF")
    parser.add_argument('--anos', type=str, help="Ano único (ex: 2016) ou intervalo (ex: 2016-2019)", default=None)
    args = parser.parse_args()
    
    if args.anos:
        if '-' in args.anos:
            inicio, fim = map(int, args.anos.split('-'))
            return list(range(inicio, fim + 1))
        else:
            return [int(args.anos)]
    return None

def main():

    # Identifica os argumentos, se houver
    anos_filtrados = obter_anos_filtrados()
    
    #Tenta realizar a conexão com o banco, se captura o erro e mostra em terminal
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
    except Exception as e:
        print (f'❌ Erro ao conectar ao banco de dados: {e}')
        sys.exit(1)

    # Tenta inserir os dados, se falhar, captura o erro e mostra em terminal
    try:
        print("▶️ Iniciando carga do Data Warehouse...")
        if anos_filtrados:
             print(f"🎯 Filtro ativado: Carregando apenas os anos {anos_filtrados}\n")
             
        global_start_time = time.time()
        
        load_dimensions(conn, cursor, anos_filtrados)
        total_fatos = insert_fato(conn, cursor, anos_filtrados)
        
        global_elapsed = time.time() - global_start_time
        total_fatos_formatado = f"{total_fatos:,}".replace(",", ".")
        
        print(f"\n🎉 Carga total concluída com sucesso!")
        print(f"📊 Resumo: {total_fatos_formatado} registros inseridos na tabela Fato em {global_elapsed:.2f} segundos.")
        
    except Exception as e:
        print(f'❌ Erro grave durante a carga: {e}')
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()