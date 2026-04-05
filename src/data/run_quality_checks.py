"""
Script para executar validações de qualidade dos dados.

Este script é integrado ao pipeline para garantir qualidade contínua.
Pode ser executado em modo local (sem AWS) ou completo (com Athena).
"""
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class QualityCheckResult:
    """Resultado de uma verificação de qualidade."""
    check_name: str
    passed: bool
    expected: Any
    actual: Any
    message: str


def generate_quality_report(results: list[QualityCheckResult]) -> str:
    """Gera relatório de qualidade em formato texto."""
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    
    report = [
        "=" * 60,
        "RELATÓRIO DE QUALIDADE DE DADOS",
        "=" * 60,
        f"Total de verificações: {total}",
        f"Aprovadas: {passed} ({passed/total*100:.1f}%)" if total > 0 else "Aprovadas: 0",
        f"Reprovadas: {total-passed}",
        "-" * 60,
    ]
    
    for r in results:
        status = "✓" if r.passed else "✗"
        report.append(f"{status} {r.check_name}: {r.message}")
    
    report.append("=" * 60)
    return "\n".join(report)


def run_local_checks() -> list[QualityCheckResult]:
    """Executa verificações locais (sem AWS)."""
    print("=" * 60)
    print("Validando arquivos locais...")
    print("=" * 60)
    
    results = []
    project_root = Path(__file__).parent.parent.parent
    
    # Verificar estrutura de diretórios
    required_dirs = ["data/bronze", "reports", "notebooks"]
    for dir_name in required_dirs:
        dir_path = project_root / dir_name
        results.append(QualityCheckResult(
            check_name=f"dir_{dir_name.replace('/', '_')}",
            passed=dir_path.exists(),
            expected="diretório existe",
            actual="existe" if dir_path.exists() else "não existe",
            message=f"Diretório {dir_name}: {'OK' if dir_path.exists() else 'NÃO ENCONTRADO'}"
        ))
    
    # Verificar arquivos de relatório (warnings se ausentes, não erros)
    reports_dir = project_root / "reports"
    expected_files = [
        ("relatorio_final.md", True),  # obrigatório
    ]
    
    # PNGs são gerados pelo EDA - verificar mas não falhar se ausentes
    png_files = [
        "01_evolucao_temporal.png",
        "02_sintomas_por_uf.png",
        "03_impacto_trabalho.png",
        "04_perfil_demografico.png",
        "05_taxa_positividade.png",
    ]
    
    for file_name, required in expected_files:
        file_path = reports_dir / file_name
        results.append(QualityCheckResult(
            check_name=f"report_{file_name.replace('.', '_')}",
            passed=file_path.exists(),
            expected="arquivo existe",
            actual="existe" if file_path.exists() else "não existe",
            message=f"Relatório {file_name}: {'OK' if file_path.exists() else 'NÃO ENCONTRADO'}"
        ))
    
    # PNGs - apenas informativo
    png_count = sum(1 for f in png_files if (reports_dir / f).exists())
    results.append(QualityCheckResult(
        check_name="report_pngs",
        passed=True,  # Não falha, apenas informa
        expected="5 PNGs",
        actual=f"{png_count} PNGs",
        message=f"Gráficos EDA: {png_count}/5 encontrados (execute EDA para gerar)"
    ))
    
    # Verificar scripts Glue
    glue_dir = project_root / "src" / "glue"
    glue_files = ["bronze_to_silver.py", "silver_to_gold.py", "create_bronze_table.json"]
    for file_name in glue_files:
        file_path = glue_dir / file_name
        results.append(QualityCheckResult(
            check_name=f"glue_{file_name.replace('.', '_')}",
            passed=file_path.exists(),
            expected="arquivo existe",
            actual="existe" if file_path.exists() else "não existe",
            message=f"Script Glue {file_name}: {'OK' if file_path.exists() else 'NÃO ENCONTRADO'}"
        ))
    
    # Verificar testes
    tests_dir = project_root / "tests"
    test_files = ["conftest.py", "test_extract_microdados.py", "test_quality_checks.py"]
    for file_name in test_files:
        file_path = tests_dir / file_name
        results.append(QualityCheckResult(
            check_name=f"test_{file_name.replace('.', '_')}",
            passed=file_path.exists(),
            expected="arquivo existe",
            actual="existe" if file_path.exists() else "não existe",
            message=f"Teste {file_name}: {'OK' if file_path.exists() else 'NÃO ENCONTRADO'}"
        ))
    
    return results


def main() -> int:
    """Executa verificações locais e retorna exit code."""
    all_results = []
    
    # Executar verificações locais
    all_results.extend(run_local_checks())
    
    # Gerar relatório
    report = generate_quality_report(all_results)
    print("\n" + report)
    
    # Salvar relatório
    reports_dir = Path(__file__).parent.parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    report_path = reports_dir / "quality_report.txt"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nRelatório salvo em: {report_path}")
    
    # Retornar exit code baseado nos resultados
    failed = sum(1 for r in all_results if not r.passed)
    if failed > 0:
        print(f"\n❌ {failed} verificações falharam!")
        return 1
    
    print(f"\n✅ Todas as {len(all_results)} verificações passaram!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
