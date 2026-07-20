import psycopg2
from psycopg2 import sql
from pathlib import Path
import sys
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações de conexão (do .env, não commitado no repositório por segurança)
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'prf_dw')
}

def dropar_banco():
    """Deleta o banco de dados se existir"""
    try:
        # Conectar ao PostgreSQL (banco padrão 'postgres')
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'  # Banco padrão
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Desconectar todas as sessões ativas
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{DB_CONFIG['database']}'
            AND pid <> pg_backend_pid();
        """)
        
        # Dropar banco
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_CONFIG['database']};")
        print(f"✅ Banco '{DB_CONFIG['database']}' deletado!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao deletar banco: {e}")
        sys.exit(1)

def criar_banco():
    """Cria o banco de dados"""
    try:
        # Conectar ao PostgreSQL (banco padrão 'postgres')
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'  # Banco padrão
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Criar banco
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']} ENCODING 'UTF8';")
        print(f"✅ Banco '{DB_CONFIG['database']}' criado com sucesso!")
        
        cursor.close()
        conn.close()
        
    except psycopg2.errors.DuplicateDatabase:
        print(f"ℹ️  Banco '{DB_CONFIG['database']}' já existe. Continuando...")
    except Exception as e:
        print(f"❌ Erro ao criar banco: {e}")
        sys.exit(1)

def executar_schema():
    """Executa o schema.sql no banco criado"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        schema_path = Path(__file__).parent / 'schema.sql'
        
        if not schema_path.exists():
            print(f"❌ Arquivo {schema_path} não encontrado!")
            sys.exit(1)
        
        with open(schema_path, 'r', encoding='latin-1') as f:
            schema_sql = f.read()
        
        try:
            cursor.execute(schema_sql)
            conn.commit()
            print("✅ Schema criado com sucesso no PostgreSQL!")
        except psycopg2.Error as e:
            # Ignorar erro se TYPE já existe
            if "já existe" in str(e) or "already exists" in str(e):
                print("⚠️  Alguns tipos já existem. Continuando...")
                conn.rollback()
            else:
                raise
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro ao executar schema: {e}")
        sys.exit(1)

def main():
    print("🚀 Iniciando setup do Data Warehouse PRF...")
    print("=" * 50)
    
    # Verificar se o argumento --fresh foi passado
    fresh_install = '--fresh' in sys.argv
    
    if fresh_install:
        print("⚠️  Modo FRESH: banco será recriado do zero")
        dropar_banco()
    else:
        print("ℹ️  Modo INCREMENTAL: banco será criado se não existir")
    
    criar_banco()
    executar_schema()
    
    print("=" * 50)
    print("✅ Setup concluído com sucesso!")
    print("\nDica: Use 'python warehouse/setup.py --fresh' para recriação completa")

if __name__ == "__main__":
    main()