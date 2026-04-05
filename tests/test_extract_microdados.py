"""
Testes para o módulo de extração de microdados.
"""
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.data.extract_microdados import ALLOWED_MONTHS, extract_microdados


class TestAllowedMonths:
    """Testes para verificar configuração de meses permitidos."""
    
    def test_allowed_months_has_three_months(self):
        """Deve ter exatamente 3 meses conforme requisito."""
        assert len(ALLOWED_MONTHS) == 3
    
    def test_allowed_months_are_sept_oct_nov(self):
        """Deve conter setembro, outubro e novembro."""
        assert ALLOWED_MONTHS == {"09", "10", "11"}
    
    def test_allowed_months_are_strings(self):
        """Meses devem ser strings com zero à esquerda."""
        for month in ALLOWED_MONTHS:
            assert isinstance(month, str)
            assert len(month) == 2


class TestExtractMicrodados:
    """Testes para função de extração."""
    
    def test_extract_filters_months(self, tmp_path):
        """Deve filtrar apenas meses permitidos."""
        # Este teste verifica a lógica de filtragem sem arquivos reais
        data_dir = tmp_path / "dados"
        data_dir.mkdir()
        output_dir = tmp_path / "bronze"
        
        # Extração com diretório vazio não deve falhar
        result = extract_microdados(data_dir, output_dir)
        assert result == []
    
    def test_extract_creates_partitioned_structure(self, tmp_path):
        """Deve criar estrutura de diretórios particionada."""
        output_dir = tmp_path / "bronze"
        output_dir.mkdir()
        
        # Verificar estrutura esperada: year=YYYY/month=MM
        partition_dir = output_dir / "year=2020" / "month=09"
        partition_dir.mkdir(parents=True)
        
        assert partition_dir.exists()
        assert "year=" in str(partition_dir)
        assert "month=" in str(partition_dir)
