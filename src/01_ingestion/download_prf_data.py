"""
Script de download de dados PRF (2017-2025).

Objetivo: Baixar, extrair e organizar dados de acidentes da PRF.

Uso:
    python config/download_prf_data.py              # Baixar todos os anos
    python config/download_prf_data.py --year 2024  # Apenas 2024
    python config/download_prf_data.py --year-range 2020 2024  # Range de anos

O que faz:
    1. Lê as URLs de config/prf_download_urls.json
    2. Converte URLs do Google Drive (view → download)
    3. Baixa os .zips
    4. Extrai os CSVs (mantém nomes originais: Acidentes[ano].csv)
    5. Limpa os .zips
    6. Mostra relatório detalhado
"""

import json
import requests
import zipfile
from pathlib import Path
import logging
import argparse
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Caminhos
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Sobe pra raiz
CONFIG_FILE = BASE_DIR / "config" / "prf_download_urls.json"
RAW_DIR = BASE_DIR / "data" / "01_bronze"

# Criar diretório se não existir
RAW_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    """Carrega as URLs do arquivo de config."""
    if not CONFIG_FILE.exists():
        logger.error(f"Arquivo de config não encontrado: {CONFIG_FILE}")
        raise FileNotFoundError(f"Configure {CONFIG_FILE} com as URLs")
    
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    return config['urls']

def convert_drive_url(view_url: str) -> str:
    """
    Converte URL de view para download do Google Drive.
    
    De: https://drive.google.com/file/d/{ID}/view
    Para: https://drive.google.com/uc?id={ID}&export=download
    """
    file_id = view_url.split('/d/')[1].split('/')[0]
    download_url = f"https://drive.google.com/uc?id={file_id}&export=download"
    
    return download_url

def download_and_extract(year: int, download_url: str) -> bool:
    """
    Baixa o arquivo .zip do Google Drive e extrai.
    
    Args:
        year: Ano a baixar (ex: 2024)
        download_url: URL de download convertida
    
    Returns:
        True se sucesso, False se falhou
    """
    
    zip_path = RAW_DIR / f"acidentes{year}.zip"
    
    try:
        # 1. Verificar se CSV já existe
        csv_path = RAW_DIR / f"acidentes{year}.csv"
        if csv_path.exists():
            logger.info(f"⏭️  {year}: CSV já existe, pulando...")
            return True
        
        # 2. Baixar arquivo
        logger.info(f"📥 {year}: Baixando...")
        
        response = requests.get(download_url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Salvar arquivo
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        percent = (downloaded / total_size) * 100
                        print(f"   {percent:5.1f}%", end='\r')
        
        size_mb = zip_path.stat().st_size / 1024 / 1024
        logger.info(f"   ✓ {size_mb:6.1f} MB baixado")
        
        # 3. Extrair ZIP
        logger.info(f"   📦 Extraindo...")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(RAW_DIR)
        
        logger.info(f"   ✓ Extração concluída")
        
        # 4. Verificar se CSV foi extraído
        if not csv_path.exists():
            logger.error(f"   ✗ CSV não encontrado após extração: {csv_path.name}")
            return False
        
        csv_size_mb = csv_path.stat().st_size / 1024 / 1024
        logger.info(f"   ✓ CSV: {csv_path.name} ({csv_size_mb:.1f} MB)")
        
        # 5. Limpar o ZIP
        zip_path.unlink()
        logger.info(f"   🗑️  ZIP removido")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"   ✗ Erro ao baixar: {e}")
        return False
    except zipfile.BadZipFile as e:
        logger.error(f"   ✗ ZIP corrompido: {e}")
        return False
    except Exception as e:
        logger.error(f"   ✗ Erro inesperado: {e}")
        return False

def main():
    """Executa o download."""
    
    # Parse argumentos
    parser = argparse.ArgumentParser(
        description="Baixa dados PRF do Google Drive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python config/download_prf_data.py                    # Todos os anos
  python config/download_prf_data.py --year 2024        # Apenas 2024
  python config/download_prf_data.py --year-range 2020 2024  # De 2020 a 2024
        """
    )
    
    parser.add_argument('--year', type=int, help='Ano específico a baixar')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('START', 'END'),
                        help='Range de anos (ex: 2020 2024)')
    
    args = parser.parse_args()
    
    # Determinar anos a baixar
    if args.year:
        years = [args.year]
    elif args.year_range:
        years = list(range(args.year_range[0], args.year_range[1] + 1))
    else:
        years = list(range(2017, 2026))  # 2017-2025
    
    logger.info("=" * 70)
    logger.info("📊 DOWNLOAD DE DADOS PRF")
    logger.info("=" * 70)
    logger.info(f"Configuração: {CONFIG_FILE}")
    logger.info(f"Destino: {RAW_DIR}")
    logger.info(f"Anos a baixar: {years}")
    logger.info("=" * 70 + "\n")
    
    # Carregar config
    try:
        urls = load_config()
    except Exception as e:
        logger.error(f"Erro ao carregar config: {e}")
        sys.exit(1)
    
    # Baixar cada ano
    results = {}
    for year in years:
        year_str = str(year)
        
        if year_str not in urls:
            logger.error(f"{year}: ✗ URL não encontrada no config")
            results[year] = False
            continue
        
        view_url = urls[year_str]
        download_url = convert_drive_url(view_url)
        
        success = download_and_extract(year, download_url)
        results[year] = success
        logger.info("")
    
    # Relatório final
    logger.info("=" * 70)
    logger.info("📋 RESUMO")
    logger.info("=" * 70)
    
    total = len(results)
    success = sum(1 for v in results.values() if v)
    failed = total - success
    
    logger.info(f"Total solicitado: {total}")
    logger.info(f"✅ Sucessos     : {success}")
    logger.info(f"❌ Falhas       : {failed}")
    
    if failed > 0:
        failed_years = [y for y, s in results.items() if not s]
        logger.warning(f"\nAnos com falha: {failed_years}")
    
    # Listar arquivos baixados
    logger.info(f"\nArquivos em {RAW_DIR}:")
    csv_files = sorted(RAW_DIR.glob("Acidentes*.csv"))
    for csv_file in csv_files:
        size_mb = csv_file.stat().st_size / 1024 / 1024
        logger.info(f"  ✓ {csv_file.name} ({size_mb:.1f} MB)")
    
    logger.info("=" * 70)
    
    # Exit code
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()