import psycopg2
from psycopg2 import sql, extras
from pathlib import Path
import sys
import os
import time
from dotenv import load_dotenv
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = BASE_DIR / "data" / "02_silver"

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'prf_dw')
}

def execute_batch_insert(cursor, table, columns, data, conflict_keys):
    """Função genérica para inserir dados únicos ignorando conflitos (ON CONFLICT DO NOTHING)."""
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

def load_dimensions(conn, cursor):
    '''
    Função que valida e tranforma tipos pandas para tipos nativos de python e faz o insert em batch no Data Warehouse.
    É preciso limpar e validar novamente pois o psycopg2 tem problemas de compatibilidade com tipagem não nativa python.
    '''
    for parquet in sorted(SILVER_DIR.glob('*.parquet')):
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
        df_local = df[['uf', 'municipio', 'br', 'km', 'latitude', 'longitude', 'regional', 'delegacia', 'uop']].dropna(subset=['uf', 'municipio']).drop_duplicates(subset=['uf', 'municipio'])
        dados_local = df_local.astype(object).where(pd.notnull(df_local), None).values.tolist()
        execute_batch_insert(cursor, 'dim_local', 
                             ['uf', 'municipio', 'br', 'km', 'latitude', 'longitude', 'regional', 'delegacia', 'uop'], 
                             dados_local, ['uf', 'municipio'])

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
        
def insert_fato(conn, cursor):
    print("\n🚀 Carregando dimensões em memória (Cache) para a tabela Fato...")
    start_cache_time = time.time()
    
    # Transforma as tabelas recém-criadas no banco em Dicionários Python para busca instantânea
    cursor.execute("SELECT id_acidente_original, pesid_original, pk_pessoa FROM dim_pessoa")
    cache_pessoa = {(row[0], row[1]): row[2] for row in cursor.fetchall()}

    cursor.execute("SELECT data_hora, id_tempo FROM dim_tempo")
    cache_tempo = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT uf, municipio, id_local FROM dim_local")
    cache_local = {(row[0], row[1]): row[2] for row in cursor.fetchall()}

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
        ano = parquet.stem.replace('acidentes', '')
        print(f"⌛ Populando tabela fato com os dados de {ano}...")
        start_time = time.time()
        
        df = pd.read_parquet(parquet)
        df = df.where(pd.notnull(df), None)
        
        lote_fatos = []
        linhas_ignoradas = 0
        
        for row in df.itertuples(index=False):
            data_hora = row.data_hora.to_pydatetime() if row.data_hora is not None else None
            
            fk_pesid = cache_pessoa.get((row.id, row.pesid))
            fk_tempo = cache_tempo.get(data_hora)
            fk_local = cache_local.get((row.uf, row.municipio))
            
            if not fk_pesid or not fk_tempo or not fk_local:
                linhas_ignoradas += 1
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
        
        # Formata o número de registros com separador de milhar (ponto) para ficar mais legível
        qtd_formatada = f"{qtd_registros:,}".replace(",", ".")
        
        print(f"✅ Tabela fato concluída para {ano}: {qtd_formatada} registros processados em {elapsed_time:.2f} segundos.")
        
        if linhas_ignoradas > 0:
            print(f"⚠️  Aviso: {linhas_ignoradas} linhas foram puladas por falta de integridade com as dimensões base.")

    return total_fatos_inseridos

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
    except Exception as e:
        print (f'❌ Erro ao conectar ao banco de dados: {e}')
        sys.exit(1)

    try:
        print("▶️ Iniciando carga do Data Warehouse...")
        global_start_time = time.time()
        
        load_dimensions(conn, cursor)
        total_fatos = insert_fato(conn, cursor)
        
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