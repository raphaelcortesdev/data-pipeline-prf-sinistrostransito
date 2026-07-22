import psycopg2
from pathlib import Path
import sys
import os
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

logger = logging.getLogger(__name__)

# Configurações de conexão
# Nota: 'postgres' é o hostname do container de banco no Docker Compose
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'database': os.getenv('DB_NAME', 'prf_dw')
}

def criar_banco():
    """Garante que o banco de dados exista no PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database='postgres'  # Conecta no banco padrão para criar o DW
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']} ENCODING 'UTF8';")
        logger.info(f"✅ Banco '{DB_CONFIG['database']}' criado com sucesso!")
        
        cursor.close()
        conn.close()
        
    except psycopg2.errors.DuplicateDatabase:
        logger.info(f"ℹ️  Banco '{DB_CONFIG['database']}' já existe. Continuando...")
    except Exception as e:
        logger.info(f"ℹ️  Verificação de banco: {e}")

def executar_schema():
    """Executa o schema.sql garantindo que tabelas e tipos existam."""
    schema_path = Path(__file__).parent / 'schema.sql'
    
    if not schema_path.exists():
        raise FileNotFoundError(f"❌ Arquivo {schema_path} não encontrado!")
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Separar instruções SQL para executar uma a uma isoladamente
    statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
    
    for stmt in statements:
        try:
            cursor.execute(stmt)
        except psycopg2.Error as e:
            # Ignora erros de objetos/tipos que já foram criados anteriormente
            if "already exists" in str(e) or "já existe" in str(e):
                continue
            else:
                logger.error(f"❌ Erro ao executar instrução SQL: {stmt[:60]}...")
                cursor.close()
                conn.close()
                raise e
    
    cursor.close()
    conn.close()
    logger.info("✅ Schema verificado/criado com sucesso no PostgreSQL!")

def main():
    logger.info("🚀 Iniciando setup/verificação do Data Warehouse PRF...")
    logger.info("=" * 50)
    
    criar_banco()
    executar_schema()
    
    logger.info("=" * 50)
    logger.info("✅ Setup do Data Warehouse concluído!")

if __name__ == "__main__":
    main()