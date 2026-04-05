"""
Script para extrair microdados PNAD COVID dos arquivos ZIP.

Requisito Tech Challenge: usar apenas 3 meses (Setembro, Outubro, Novembro/2020)
"""
import zipfile
from pathlib import Path

# Meses permitidos conforme requisito do Tech Challenge
ALLOWED_MONTHS = {"09", "10", "11"}


def extract_microdados(
    data_dir: Path,
    output_dir: Path,
    allowed_months: set[str] | None = None
) -> list[Path]:
    """
    Extrai CSVs dos arquivos ZIP de microdados.
    
    Args:
        data_dir: Diretório contendo os arquivos ZIP
        output_dir: Diretório de saída para os CSVs extraídos
        allowed_months: Conjunto de meses permitidos (ex: {"09", "10", "11"}).
                        Se None, usa ALLOWED_MONTHS padrão.
        
    Returns:
        Lista de caminhos dos arquivos extraídos
    """
    if allowed_months is None:
        allowed_months = ALLOWED_MONTHS
    
    zip_files = sorted(data_dir.glob("PNAD_COVID_*.zip"))
    extracted_files = []
    
    for zip_path in zip_files:
        # Extrair mês/ano do nome do arquivo (ex: PNAD_COVID_052020.zip)
        month_year = zip_path.stem.split("_")[-1]  # "052020"
        month = month_year[:2]  # "05"
        year = month_year[2:]   # "2020"
        
        # Filtrar apenas meses permitidos
        if month not in allowed_months:
            print(f"Ignorado (fora do período): {zip_path.name} (mês {month})")
            continue
        
        # Criar diretório de saída particionado
        partition_dir = output_dir / f"year={year}" / f"month={month}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Extrair conteúdo do ZIP
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file_info in zf.infolist():
                if file_info.filename.endswith('.csv'):
                    # Extrair mantendo apenas o nome do arquivo
                    file_info.filename = Path(file_info.filename).name
                    zf.extract(file_info, partition_dir)
                    extracted_path = partition_dir / file_info.filename
                    extracted_files.append(extracted_path)
                    print(f"Extraído: {extracted_path}")
    
    return extracted_files


if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    data_dir = project_root / "data" / "microdados" / "dados"
    output_dir = project_root / "data" / "bronze"
    
    # Criar diretório bronze local se não existir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Extraindo apenas meses: {sorted(ALLOWED_MONTHS)}")
    extracted = extract_microdados(data_dir, output_dir)
    print(f"\nTotal de arquivos extraídos: {len(extracted)}")