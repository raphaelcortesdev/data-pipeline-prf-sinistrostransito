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
RAW_DIR = BASE_DIR / "data" / "bronze"

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
    """Converte URL de view para download do Google Drive."""
    file_id = view_url.split('/d/')[1].split('/')[0]
    return f"https://drive.google.com/uc?id={file_id}&export=download"

def download_and_extract(year: int, download_url: str) -> bool:
    """Baixa o arquivo .zip do Google Drive e extrai."""
    zip_path = RAW_DIR / f"acidentes{year}.zip"
    
    try:
        csv_path = RAW_DIR / f"acidentes{year}.csv"
        if csv_path.exists():
            logger.info(f"⏭️  {year}: CSV já existe, pulando...")
            return True
        
        logger.info(f"📥 {year}: Baixando...")
        response = requests.get(download_url, timeout=120, stream=True)
        response.raise_for_status()
        
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
        
        logger.info(f"   📦 Extraindo...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(RAW_DIR)
        
        logger.info(f"   ✓ Extração concluída")
        
        if not csv_path.exists():
            logger.error(f"   ✗ CSV não encontrado após extração: {csv_path.name}")
            return False
        
        csv_size_mb = csv_path.stat().st_size / 1024 / 1024
        logger.info(f"   ✓ CSV: {csv_path.name} ({csv_size_mb:.1f} MB)")
        
        zip_path.unlink()
        logger.info(f"   🗑️  ZIP removido")
        return True
        
    except Exception as e:
        logger.error(f"   ✗ Erro: {e}")
        return False

def main(years=None):
    """
    Executa o download dos dados.
    Aceita 'years' como parâmetro para ser chamado diretamente pelo Airflow.
    """
    if years is None:
        years = list(range(2017, 2026))  # Padrão: 2017 a 2025
    
    logger.info("=" * 70)
    logger.info("📊 DOWNLOAD DE DADOS PRF")
    logger.info("=" * 70)
    logger.info(f"Configuração: {CONFIG_FILE}")
    logger.info(f"Destino: {RAW_DIR}")
    logger.info(f"Anos a baixar: {years}")
    logger.info("=" * 70 + "\n")
    
    urls = load_config()
    
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
    
    # Resumo
    total = len(results)
    success = sum(1 for v in results.values() if v)
    failed = total - success
    
    logger.info("=" * 70)
    logger.info(f"Total solicitado: {total} | ✅ Sucessos: {success} | ❌ Falhas: {failed}")
    logger.info("=" * 70)
    
    # Se houver falhas, lança uma Exceção para o Airflow marcar a task como FAILED
    if failed > 0:
        failed_years = [y for y, s in results.items() if not s]
        raise RuntimeError(f"Falha ao baixar os dados dos anos: {failed_years}")

if __name__ == "__main__":
    # Lógica CLI mantida apenas para quando rodar via Terminal
    parser = argparse.ArgumentParser(description="Baixa dados PRF do Google Drive")
    parser.add_argument('--year', type=int, help='Ano específico a baixar')
    parser.add_argument('--year-range', type=int, nargs=2, metavar=('START', 'END'), help='Range de anos')
    
    args = parser.parse_args()
    
    if args.year:
        selected_years = [args.year]
    elif args.year_range:
        selected_years = list(range(args.year_range[0], args.year_range[1] + 1))
    else:
        selected_years = None
        
    try:
        main(years=selected_years)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Erro na execução CLI: {e}")
        sys.exit(1)