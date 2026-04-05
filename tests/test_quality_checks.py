"""
Testes para o módulo de validações de qualidade.
"""
import pandas as pd
import pytest

from src.data.quality_checks import (
    check_completeness,
    check_duplicates,
    check_uf_validity,
    check_value_range,
    generate_quality_report,
    QualityCheckResult,
)


class TestQualityCheckResult:
    """Testes para a dataclass QualityCheckResult."""
    
    def test_create_passed_result(self):
        """Deve criar resultado de verificação que passou."""
        result = QualityCheckResult(
            check_name="test",
            passed=True,
            expected="expected",
            actual="actual",
            message="Test passed"
        )
        assert result.passed is True
    
    def test_create_failed_result(self):
        """Deve criar resultado de verificação que falhou."""
        result = QualityCheckResult(
            check_name="test",
            passed=False,
            expected="expected",
            actual="actual",
            message="Test failed"
        )
        assert result.passed is False


class TestCheckCompleteness:
    """Testes para verificação de completude."""
    
    def test_completeness_all_present(self):
        """Deve passar quando todas colunas existem e estão completas."""
        df = pd.DataFrame({
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"]
        })
        results = check_completeness(df, ["col1", "col2"])
        assert all(r.passed for r in results)
    
    def test_completeness_missing_column(self):
        """Deve falhar quando coluna está ausente."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        results = check_completeness(df, ["col1", "col2"])
        assert not results[1].passed
        assert "não encontrada" in results[1].message
    
    def test_completeness_with_nulls(self):
        """Deve reportar completude baixa com muitos nulos."""
        df = pd.DataFrame({
            "col1": [1, None, None, None, None]  # 20% completo
        })
        results = check_completeness(df, ["col1"])
        assert not results[0].passed  # Threshold é 95%


class TestCheckUfValidity:
    """Testes para verificação de códigos de UF."""
    
    def test_valid_ufs(self):
        """Deve passar com UFs válidas."""
        df = pd.DataFrame({"uf_codigo": [11, 35, 53]})  # RO, SP, DF
        result = check_uf_validity(df)
        assert result.passed
    
    def test_invalid_ufs(self):
        """Deve falhar com UFs inválidas."""
        df = pd.DataFrame({"uf_codigo": [99, 100]})  # Inválidos
        result = check_uf_validity(df)
        assert not result.passed


class TestCheckValueRange:
    """Testes para verificação de range de valores."""
    
    def test_values_in_range(self):
        """Deve passar quando valores estão no range."""
        df = pd.DataFrame({"idade": [25, 30, 45]})
        result = check_value_range(df, "idade", 0, 120)
        assert result.passed
    
    def test_values_out_of_range(self):
        """Deve falhar quando valores estão fora do range."""
        df = pd.DataFrame({"idade": [25, 150]})  # 150 é inválido
        result = check_value_range(df, "idade", 0, 120)
        assert not result.passed


class TestCheckDuplicates:
    """Testes para verificação de duplicatas."""
    
    def test_no_duplicates(self):
        """Deve passar sem duplicatas."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = check_duplicates(df, ["id"])
        assert result.passed
    
    def test_with_duplicates(self):
        """Deve falhar com duplicatas."""
        df = pd.DataFrame({"id": [1, 1, 2]})
        result = check_duplicates(df, ["id"])
        assert not result.passed


class TestGenerateQualityReport:
    """Testes para geração de relatório."""
    
    def test_report_format(self):
        """Deve gerar relatório formatado corretamente."""
        results = [
            QualityCheckResult("test1", True, "a", "a", "OK"),
            QualityCheckResult("test2", False, "b", "c", "Failed"),
        ]
        report = generate_quality_report(results)
        
        assert "RELATÓRIO DE QUALIDADE DE DADOS" in report
        assert "Total de verificações: 2" in report
        assert "Aprovadas: 1" in report
        assert "Reprovadas: 1" in report
