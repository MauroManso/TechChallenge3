"""
Validações de qualidade de dados para o pipeline PNAD COVID.
"""
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class QualityCheckResult:
    """Resultado de uma verificação de qualidade."""
    check_name: str
    passed: bool
    expected: Any
    actual: Any
    message: str


def check_completeness(df: pd.DataFrame, required_columns: list[str]) -> list[QualityCheckResult]:
    """Verifica completude de colunas obrigatórias."""
    results = []
    total_rows = len(df)
    
    for col in required_columns:
        if col not in df.columns:
            results.append(QualityCheckResult(
                check_name=f"completeness_{col}",
                passed=False,
                expected="column exists",
                actual="column missing",
                message=f"Coluna {col} não encontrada"
            ))
            continue
            
        non_null = df[col].notna().sum()
        completeness = non_null / total_rows if total_rows > 0 else 0
        
        results.append(QualityCheckResult(
            check_name=f"completeness_{col}",
            passed=completeness >= 0.95,
            expected=">=95%",
            actual=f"{completeness*100:.1f}%",
            message=f"Completude de {col}: {completeness*100:.1f}%"
        ))
    
    return results


def check_uf_validity(df: pd.DataFrame, uf_column: str = "uf_codigo") -> QualityCheckResult:
    """Verifica se códigos de UF são válidos."""
    valid_ufs = {11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                 31, 32, 33, 35, 41, 42, 43, 50, 51, 52, 53}
    
    if uf_column not in df.columns:
        return QualityCheckResult(
            check_name="uf_validity",
            passed=False,
            expected="column exists",
            actual="column missing",
            message=f"Coluna {uf_column} não encontrada"
        )
    
    invalid_ufs = df[~df[uf_column].isin(valid_ufs)][uf_column].unique()
    
    return QualityCheckResult(
        check_name="uf_validity",
        passed=len(invalid_ufs) == 0,
        expected="all UFs valid",
        actual=f"{len(invalid_ufs)} invalid UFs",
        message=f"UFs inválidas encontradas: {list(invalid_ufs)[:5]}" if len(invalid_ufs) > 0 else "Todas UFs válidas"
    )


def check_duplicates(df: pd.DataFrame, key_columns: list[str]) -> QualityCheckResult:
    """Verifica duplicatas baseado em colunas-chave."""
    total = len(df)
    unique = df.drop_duplicates(subset=key_columns).shape[0]
    duplicates = total - unique
    
    return QualityCheckResult(
        check_name="no_duplicates",
        passed=duplicates == 0,
        expected="0 duplicates",
        actual=f"{duplicates} duplicates",
        message=f"Encontradas {duplicates} linhas duplicadas ({duplicates/total*100:.2f}%)"
    )


def check_value_range(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> QualityCheckResult:
    """Verifica se valores estão dentro de um range esperado."""
    if column not in df.columns:
        return QualityCheckResult(
            check_name=f"range_{column}",
            passed=False,
            expected="column exists",
            actual="column missing",
            message=f"Coluna {column} não encontrada"
        )
    
    out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
    
    return QualityCheckResult(
        check_name=f"range_{column}",
        passed=len(out_of_range) == 0,
        expected=f"[{min_val}, {max_val}]",
        actual=f"{len(out_of_range)} out of range",
        message=f"Valores fora do range: {len(out_of_range)}"
    )


def run_all_checks(df: pd.DataFrame) -> list[QualityCheckResult]:
    """Executa todas as verificações de qualidade."""
    results = []
    
    # Completude
    required = ["uf_codigo", "idade", "sexo", "peso_pos_estratificacao"]
    results.extend(check_completeness(df, required))
    
    # Validade de UF
    results.append(check_uf_validity(df))
    
    # Range de idade
    results.append(check_value_range(df, "idade", 0, 120))
    
    return results


def generate_quality_report(results: list[QualityCheckResult]) -> str:
    """Gera relatório de qualidade em formato texto."""
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    
    report = [
        "=" * 60,
        "RELATÓRIO DE QUALIDADE DE DADOS",
        "=" * 60,
        f"Total de verificações: {total}",
        f"Aprovadas: {passed} ({passed/total*100:.1f}%)",
        f"Reprovadas: {total-passed}",
        "-" * 60,
    ]
    
    for r in results:
        status = "✓" if r.passed else "✗"
        report.append(f"{status} {r.check_name}: {r.message}")
    
    report.append("=" * 60)
    return "\n".join(report)