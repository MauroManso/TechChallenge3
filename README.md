# PosTech Data Analytics - Tech Challenge 3

Repositório para desenvolvimento do **Tech Challenge - Fase 3** da pós-graduação

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
- `scripts/`: scripts PowerShell de orquestração (legado, ver seção Pipeline abaixo).

Guia de desenvolvimento: `docs/guide-coding-and-analytics.md`

## Dados

Os dados devem ser obtidos do portal do IBGE:

- `https://www.ibge.gov.br/estatisticas/investigacoes-experimentais/estatisticas-experimentais/27947-divulgacao-mensal-pnadcovid2?t=downloads&utm_source=covid19&utm_medium=hotsite&utm_campaign=covid_19`

A estrutura de pastas em `data/` foi mantida para espelhar a organização disponibilizada no site.

Instruções detalhadas: `data/README.md`

## Setup Rápido (Conda)

```bash
conda env create -f environment.ymlconda activate techchallenge3
```

Caso o ambiente já exista:

```bash
conda env update -f environment.yml --prune
```

## Pipeline de Dados (Python)

O pipeline de dados é orquestrado pelo módulo Python em `src/pipeline/`, que substitui os scripts PowerShell legados.

### Executando o Pipeline

```bash
# Executa todos os 19 passos (requer confirmação)
python -m src.pipeline

# Execução automática sem confirmação
python -m src.pipeline -y

# Executa apenas passos 1-10
python -m src.pipeline --stop-at 10

# Continua a partir do passo 5
python -m src.pipeline --skip-to 5

# Modo dry-run (mostra o que seria feito sem executar)
python -m src.pipeline --dry-run

# Combinação de opções
python -m src.pipeline --skip-to 5 --stop-at 15 --dry-run
```

### Passos do Pipeline

| Passo | Descrição |
| --- | --- |
| 01 | Verificar configuração AWS |
| 02 | Criar bucket S3 |
| 03 | Criar database Glue |
| 04 | Criar role IAM |
| 05 | Extrair dados locais |
| 06 | Upload bronze para S3 |
| 07 | Criar tabela bronze |
| 08 | Adicionar partições bronze |
| 09 | Upload scripts Glue |
| 10 | Criar job bronze-to-silver |
| 11 | Executar job bronze-to-silver |
| 12 | Criar crawler silver |
| 13 | Criar job silver-to-gold |
| 14 | Executar job silver-to-gold |
| 15 | Criar crawler gold |
| 16 | Criar workgroup Athena |
| 17 | Executar queries Athena |
| 18 | Executar notebook EDA |
| 19 | Executar verificações de qualidade |

### Configuração

O pipeline usa as seguintes variáveis de ambiente (com valores padrão):

- `AWS_REGION`: Região AWS (padrão: `us-east-1`)
- `PNAD_BUCKET_NAME` ou `S3_BUCKET`: Nome do bucket S3 (padrão: `pnad-covid-techchallenge3`)
- `GLUE_DATABASE`: Nome do database Glue (padrão: `pnad_covid_db`)
- `GLUE_ROLE`: Nome da role IAM (padrão: `GlueServiceRole-TechChallenge3`)
- `ATHENA_WORKGROUP`: Nome do workgroup Athena (padrão: `techchallenge3`)

### Scripts PowerShell (Legado)

Os scripts PowerShell em `scripts/` permanecem funcionais para compatibilidade:

```powershell
# Pipeline completo
.\scripts\run-all.ps1

# Com opções
.\scripts\run-all.ps1 -SkipTo 5 -StopAt 15 -DryRun
```

## Testes

```bash
# Executar todos os testes
python -m pytest tests/ -v

# Apenas testes do pipeline
python -m pytest tests/test_pipeline.py -v
```
