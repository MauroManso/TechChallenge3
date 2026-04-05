"""
Configuração do pytest para o projeto.
"""
import sys
from pathlib import Path

# Adicionar raiz do projeto ao path para imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
