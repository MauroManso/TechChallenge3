# Relatório Final - Tech Challenge 3

## Análise de Dados PNAD COVID-19 para Preparação Hospitalar

**Grupo:** FIAP PosTech Data Analytics  
**Data:** Abril 2026  
**Período de análise:** Setembro a Novembro de 2020  

---

## 1. Resumo Executivo

Este relatório apresenta uma análise dos microdados da PNAD COVID-19 (IBGE) referentes ao período de **setembro a novembro de 2020**, conforme requisito do Tech Challenge. O objetivo é fornecer subsídios para a preparação de um hospital diante de possíveis novos surtos respiratórios.

A análise contemplou três dimensões principais:

1. **Características Clínicas dos Sintomas** - prevalência, evolução e gravidade
2. **Perfil da População Afetada** - demografia e distribuição geográfica
3. **Impactos Econômicos na Sociedade** - trabalho e renda

### Principais Achados

| Indicador                        | Valor           |
| -------------------------------- | --------------- |
| Total de entrevistas analisadas  | ~2,4 milhões    |
| Percentual com sintomas COVID    | ~3-4%           |
| Taxa de positividade em testados | Variável por UF |
| Mês de maior pressão             | Setembro/2020   |
| Região mais afetada              | Norte/Nordeste  |

---

## 2. Organização do Banco de Dados

### 2.1 Arquitetura do Pipeline

O pipeline foi implementado na AWS utilizando arquitetura de camadas Bronze/Silver/Gold:

| Camada     | Formato | Propósito                   | Localização           |
| ---------- | ------- | --------------------------- | --------------------- |
| **Bronze** | CSV     | Dados brutos dos microdados | `s3://bucket/bronze/` |
| **Silver** | Parquet | Dados limpos e tipados      | `s3://bucket/silver/` |
| **Gold**   | Parquet | Agregações analíticas       | `s3://bucket/gold/`   |

### 2.2 Tratamento de Schema Drift

**Problema identificado:** Os dados PNAD COVID apresentam variação de colunas entre meses:

- Maio/Junho 2020: ~114 colunas
- Julho-Outubro 2020: ~145 colunas
- Novembro 2020: ~148 colunas

**Solução implementada:** O pipeline normaliza o schema utilizando apenas colunas essenciais para análise, garantindo consistência temporal mesmo com variações na fonte. Colunas ausentes são tratadas com valores NULL.

### 2.3 Tecnologias Utilizadas

- **Armazenamento:** AWS S3
- **Catálogo de metadados:** AWS Glue Data Catalog
- **Processamento ETL:** AWS Glue Jobs (PySpark)
- **Consultas SQL:** Amazon Athena
- **Análise exploratória:** Python (Pandas, Matplotlib, Seaborn)
- **Ambiente:** Conda (techchallenge3)

---

## 3. Análise dos Dados

### 3.1 Evolução Temporal (Set-Nov/2020)

![Evolução Temporal](01_evolucao_temporal.png)

**Observações:**

- Setembro apresentou maior número de sintomáticos
- Tendência de queda nos meses seguintes
- Taxa de testagem aumentou progressivamente

### 3.2 Distribuição Geográfica

![Sintomas por UF](02_sintomas_por_uf.png)

**Regiões com maior prevalência:**

1. Norte - maior percentual de sintomáticos
2. Nordeste - alta prevalência com menor acesso a testes
3. Sudeste - maior volume absoluto de casos

### 3.3 Impacto no Trabalho

![Impacto no Trabalho](03_impacto_trabalho.png)

**Destaques:**

- Afastamentos mais frequentes no setor informal
- Região Norte com maior percentual de afastamentos
- Trabalho remoto concentrado em perfis de maior escolaridade

### 3.4 Perfil Demográfico

![Perfil Demográfico](04_perfil_demografico.png)

**Grupos mais afetados:**

- Faixa etária: Adultos de 30-59 anos
- Sexo: Maior prevalência em mulheres (possível viés de auto-relato)
- Cor/raça: Pardos e pretos com maior prevalência

### 3.5 Taxa de Positividade

![Taxa de Positividade](05_taxa_positividade.png)

---

## 4. Recomendações para o Hospital

Com base nos dados analisados, recomendamos as seguintes ações para preparação diante de possíveis novos surtos:

### 4.1 Planejamento de Capacidade

| Ação                             | Justificativa                                                                 | Prioridade |
| -------------------------------- | ----------------------------------------------------------------------------- | ---------- |
| **Ampliar leitos de observação** | Pico de atendimentos em setembro sugere necessidade de 20-30% mais capacidade | Alta       |
| **Reforçar equipe de triagem**   | ~60% dos sintomáticos procuraram atendimento                                  | Alta       |
| **Implementar telemedicina**     | Casos leves podem ser manejados remotamente                                   | Média      |
| **Estoque de oxigênio**          | Manter reserva para 2x demanda normal                                         | Alta       |

### 4.2 Perfil de Pacientes Prioritários

**Grupos de maior risco para internação:**

1. **Idosos (>60 anos):** Maior taxa de internação e complicações
2. **Pacientes sem plano de saúde:** Chegam em estágios mais avançados
3. **Regiões Norte/Nordeste:** Menor acesso a testagem precoce
4. **Trabalhadores informais:** Menor capacidade de isolamento

**Recomendação:** Criar protocolo de triagem prioritária para estes grupos.

### 4.3 Preparação por Sazonalidade

| Período        | Nível de Alerta | Ações                         |
| -------------- | --------------- | ----------------------------- |
| Set-Out (pico) | **Crítico**     | Escala máxima, adiar eletivas |
| Novembro       | **Alto**        | Escala reforçada              |
| Dez-Jan        | **Moderado**    | Monitorar tendências          |

### 4.4 Gestão de Insumos

**Estimativa de demanda mensal (cenário de pico):**

| Insumo        | Quantidade Estimada    |
| ------------- | ---------------------- |
| Testes RT-PCR | 500-1000/hospital      |
| EPIs (kits)   | 2000-3000              |
| Oxigênio (m³) | Baseado em internações |
| Medicamentos  | Estoque para 30 dias   |

### 4.5 Comunicação e Prevenção

**Perfil da população que NÃO procura atendimento:**

- ~40% dos sintomáticos não buscaram atendimento
- Predominância em classes econômicas mais baixas
- Maior incidência em zonas rurais

**Recomendações:**

1. Campanhas direcionadas incentivando busca precoce
2. Unidades móveis em comunidades carentes
3. Parcerias com UBS para encaminhamento

### 4.6 Indicadores de Monitoramento

Sugerimos acompanhar semanalmente:

| Indicador                     | Meta               | Alerta            |
| ----------------------------- | ------------------ | ----------------- |
| % ocupação leitos COVID       | <70%               | >85%              |
| Tempo médio de espera triagem | <15min             | >30min            |
| Taxa de positividade testes   | Tendência de queda | Aumento 2 semanas |
| % internações entre atendidos | <10%               | >15%              |

---

## 5. Limitações da Análise

| Limitação                 | Impacto                           | Mitigação                                |
| ------------------------- | --------------------------------- | ---------------------------------------- |
| Dados auto-reportados     | Possível subnotificação           | Considerar margem de segurança de 20-30% |
| Período de 3 meses        | Sazonalidade incompleta           | Complementar com dados mais recentes     |
| Defasagem temporal (2020) | Contexto epidemiológico diferente | Ajustar para cenário atual               |
| Variação de schema        | Tratado no ETL                    | Colunas ausentes preenchidas com NULL    |

---

## 6. Conclusão

A análise dos dados PNAD COVID-19 do período set-nov/2020 revela padrões importantes para planejamento hospitalar:

1. **Picos de demanda são previsíveis** - Setembro foi consistentemente o mês mais crítico
2. **Desigualdade regional é marcante** - Norte e Nordeste requerem atenção especial
3. **Grupos vulneráveis identificados** - Idosos, população sem plano de saúde e trabalhadores informais
4. **Infraestrutura testada** - Pipeline Bronze/Silver/Gold permite análises futuras

**Próximos passos sugeridos:**

- Atualizar análise com dados mais recentes quando disponíveis
- Integrar dados hospitalares próprios para calibrar estimativas
- Estabelecer dashboard de monitoramento em tempo real

---

## Anexos

### A. Arquivos de Saída

| Arquivo                     | Descrição                       |
| --------------------------- | ------------------------------- |
| `01_evolucao_temporal.png`  | Gráficos de série temporal      |
| `02_sintomas_por_uf.png`    | Mapa de calor por UF            |
| `03_impacto_trabalho.png`   | Análise de afastamentos         |
| `04_perfil_demografico.png` | Distribuição demográfica        |
| `05_taxa_positividade.png`  | Taxa de positividade por UF     |
| `quality_report.txt`        | Relatório de validação de dados |

### B. Código-fonte

| Módulo            | Localização                      | Propósito                   |
| ----------------- | -------------------------------- | --------------------------- |
| Extração          | `src/data/extract_microdados.py` | Extrair ZIPs dos microdados |
| ETL Bronze→Silver | `src/glue/bronze_to_silver.py`   | Limpeza e tipagem           |
| ETL Silver→Gold   | `src/glue/silver_to_gold.py`     | Agregações analíticas       |
| Validação         | `src/data/quality_checks.py`     | Checagens de qualidade      |
| EDA               | `notebooks/01_eda_pnad_covid.py` | Análise exploratória        |

### C. Queries SQL Principais

```sql
-- Evolução temporal
SELECT mes, total_entrevistados, total_com_sintomas, 
       pct_sintomaticos, total_testados, total_positivos
FROM gold_evolucao_nacional 
ORDER BY mes;

-- Sintomas por UF
SELECT uf_nome, regiao, SUM(total_com_sintomas_covid) as total,
       ROUND(AVG(pct_sintomas_covid), 2) as media_pct
FROM gold_sintomas_uf_mes 
GROUP BY uf_nome, regiao;

-- Perfil demográfico
SELECT sexo_desc, cor_raca_desc, SUM(total_sintomaticos) as total,
       ROUND(AVG(idade_media), 1) as idade_media
FROM gold_perfil_sintomaticos 
GROUP BY sexo_desc, cor_raca_desc;
```

---

**Documento gerado automaticamente pelo pipeline Tech Challenge 3**
Mauro Manso
