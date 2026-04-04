# Guia de Dados (PNAD COVID/IBGE)

Este projeto **não versiona dados reais** em `data/`. Apenas a estrutura de pastas é mantida no Git para padronizar o projeto.

## Fonte Oficial

Baixe os arquivos a partir de:

`https://www.ibge.gov.br/estatisticas/investigacoes-experimentais/estatisticas-experimentais/27947-divulgacao-mensal-pnadcovid2?t=downloads&utm_source=covid19&utm_medium=hotsite&utm_campaign=covid_19`

## Estrutura de Pastas Esperada

- `data/mensal/tabelas/`
- `data/microdados/dados/`
- `data/microdados/documentacao/`

A estrutura acima reflete o padrão disponibilizado pelo portal.

## Como Popular a Pasta Localmente

1. Faça download dos arquivos no site do IBGE.
2. Coloque microdados compactados em `data/microdados/dados/`.
3. Coloque documentação/dicionários em `data/microdados/documentacao/`.
4. Coloque tabelas mensais em `data/mensal/tabelas/`.

## Observações

- Não comite arquivos de dados (`zip`, `csv`, `xls`, `xlsx`, `parquet` etc.).
- Os padrões de ignore no `.gitignore` já bloqueiam o versionamento de dados reais.
- Use checksums locais, se necessário, para rastreabilidade dos downloads.
