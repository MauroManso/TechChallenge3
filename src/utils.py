"""
Utilitários para mapeamento de códigos geográficos IBGE.

Contém mapeamentos de UF e Região para uso nos scripts de transformação.
"""

# Mapeamento de códigos UF para nomes
UF_MAPPING: dict[int, str] = {
    11: "Rondônia",
    12: "Acre",
    13: "Amazonas",
    14: "Roraima",
    15: "Pará",
    16: "Amapá",
    17: "Tocantins",
    21: "Maranhão",
    22: "Piauí",
    23: "Ceará",
    24: "Rio Grande do Norte",
    25: "Paraíba",
    26: "Pernambuco",
    27: "Alagoas",
    28: "Sergipe",
    29: "Bahia",
    31: "Minas Gerais",
    32: "Espírito Santo",
    33: "Rio de Janeiro",
    35: "São Paulo",
    41: "Paraná",
    42: "Santa Catarina",
    43: "Rio Grande do Sul",
    50: "Mato Grosso do Sul",
    51: "Mato Grosso",
    52: "Goiás",
    53: "Distrito Federal",
}

# Mapeamento de faixas de UF para regiões
REGIAO_MAPPING: dict[tuple[int, int], str] = {
    (11, 17): "Norte",
    (21, 29): "Nordeste",
    (31, 35): "Sudeste",
    (41, 43): "Sul",
    (50, 53): "Centro-Oeste",
}


def get_uf_nome(uf_codigo: int) -> str:
    """Retorna o nome da UF com base no código IBGE."""
    return UF_MAPPING.get(uf_codigo, "Desconhecido")


def get_regiao(uf_codigo: int) -> str:
    """Retorna a região com base no código da UF."""
    for (inicio, fim), regiao in REGIAO_MAPPING.items():
        if inicio <= uf_codigo <= fim:
            return regiao
    return "Desconhecido"


def get_uf_codigo(uf_nome: str) -> int | None:
    """Retorna o código IBGE com base no nome da UF."""
    for codigo, nome in UF_MAPPING.items():
        if nome.lower() == uf_nome.lower():
            return codigo
    return None
