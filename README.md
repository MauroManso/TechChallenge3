# PosTech Data Analytics - Tech Challenge 3

Repositório para desenvolvimento do **Tech Challenge - Fase 3** da pós-graduação, com base no enunciado em `docs/Postech - Tech Challenge - Fase 3.pdf` e no material de apoio em `docs/Aulas/`.

## Objetivo

Construir uma solução de análise de dados orientada ao desafio da Fase 3, aplicando os conceitos das aulas de Big Data e Bancos de Dados (SQL/NoSQL), com foco em:

- ingestão e organização dos dados;
- análise exploratória e estatística;
- modelagem/insights para responder às perguntas de negócio do desafio;
- entrega reprodutível com código testável.

## Estrutura do Repositório

- `notebooks/`: notebooks de exploração, prototipação e apresentações analíticas.
- `src/`: código Python reutilizável (pipelines, funções utilitárias, módulos de domínio).
- `tests/`: testes automatizados para módulos de `src/`.
- `data/`: estrutura local de dados brutos e derivados (sem versionar arquivos de dados reais).
- `docs/`: documentação do projeto, guias técnicos e material de referência.
- `reports/`: relatórios e saídas finais.

Guia de desenvolvimento: `docs/guide-coding-and-analytics.md`

## Dados

Os dados devem ser obtidos do portal do IBGE:

- `https://www.ibge.gov.br/estatisticas/investigacoes-experimentais/estatisticas-experimentais/27947-divulgacao-mensal-pnadcovid2?t=downloads&utm_source=covid19&utm_medium=hotsite&utm_campaign=covid_19`

A estrutura de pastas em `data/` foi mantida para espelhar a organização disponibilizada no site.

Instruções detalhadas: `data/README.md`

## Setup Rápido (Conda)

```bash
conda env create -f environment.yml
conda activate techchallenge3
```

Caso o ambiente já exista:

```bash
conda env update -f environment.yml --prune
```

## Boas Práticas

- Separar exploração (`notebooks/`) de código produtivo (`src/`).
- Escrever testes em `tests/` junto com a evolução dos módulos.
- Evitar caminhos absolutos; usar caminhos relativos ao projeto.
- Não versionar dados brutos, artefatos pesados e segredos.

## Referências Internas

- Desafio: `docs/Postech - Tech Challenge - Fase 3.pdf`
- Aulas: `docs/Aulas/`
- Guia de engenharia e analytics: `docs/guide-coding-and-analytics.md`
