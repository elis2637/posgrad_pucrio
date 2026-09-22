# MVP - Engenharia de Dados: Análise de Egressos e Conversão para Pós-Graduação

**Aluna:** Elisandra André Maranhe  
**RA:** 4052026000054  
**Sprint:** Engenharia de Dados  
**Instituição:** PUC-Rio  
**Repositório GitHub:** [posgrad_pucrio](https://github.com/elis2637/posgrad_pucrio/tree/main/sprint_engenharia_dados)

---

## 📋 Sumário
1. [Contexto de Negócio e Perguntas](#-contexto-de-negócio-e-perguntas)
2. [Datasets Utilizados e LGPD](#-datasets-utilizados-e-lgpd)
3. [Arquitetura de Dados e Ingestão (Camada Bronze)](#-arquitetura-de-dados-e-ingestão-camada-bronze)
4. [Modelagem Dimensional e Catálogo de Dados (Camadas Silver e Gold)](#-modelagem-dimensional-e-catálogo-de-dados-camadas-silver-e-gold)
5. [Pipeline ETL e Tratamento de Qualidade dos Dados](#-pipeline-etl-e-tratamento-de-qualidade-dos-dados)
6. [Data Marts de KPIs e Otimização](#-data-marts-de-kpis-e-otimização)
7. [Análise de Dados](#-análise-de-dados)
8. [Estrutura do Repositório](#-estrutura-do-repositório)
9. [Autoavaliação e Trabalhos Futuros](#-autoavaliação-e-trabalhos-futuros)

---

## 🎯 Contexto de Negócio e Perguntas

### Contexto
O projeto foi desenvolvido no âmbito de uma instituição educacional de ensino superior em processo de reestruturação das ofertas de cursos de pós-graduação. Historicamente, o desenvolvimento de novos cursos baseou-se em intuição ou tendências genéricas de mercado.

**Problema Central:** Desconexão entre a gestão do ciclo de vida do aluno (*Lifetime Value* – LTV) e a previsibilidade de receita na conversão de egressos para a pós-graduação. O objetivo principal é evoluir de uma análise diagnóstica para a alocação eficiente de esforços de marketing e vendas (Up-sell) através de segmentação e propensão preditiva do Alumni.

```
                             [ PROBLEMA CENTRAL ]
           Ineficiência na Conversão e Previsibilidade do LTV do Egresso
                                       |
    +------------------+---------------+---------------+-------------------+
    |                  |                               |                   |
    v                  v                               v                   v
Timing &          Capacidade                      Engajamento &       Atratividade
Propensão         Financeira                      Lealdade            Geográfica
(Conversão)       (Socioeconômico)                (Alumni Score)      (Mercado)
```

---

### Perguntas de Negócio por Categoria

#### 1. Timing e Propensão (Potencial de conversão e captação)
* **Setores envolvidos:** Marketing e Financeiro
* Qual é a taxa de conversão (`potencial_matricula_pos`) por curso (`nome_curso`) e por área do conhecimento?
* Qual o tempo ideal após a graduação (`meses_desde_formacao`) em que os egressos demonstram maior intenção de matrícula?
* Ex-bolsistas de graduação (`bolsista_graduacao`) possuem maior probabilidade de continuar os estudos em comparação aos não bolsistas?

#### 2. Capacidade Financeira (Empregabilidade e perfil socioeconômico)
* **Setores envolvidos:** Marketing e Financeiro
* Qual é a renda média e a distribuição de cargos (`nivel_cargo`) por curso e por modalidade (`modalidade_graduacao`)?
* Existe correlação entre o valor da mensalidade da graduação (`mensalidade_base`) e o retorno financeiro atual do egresso (`renda_mensal_estimada`)?
* Quais cursos geram maior inserção na iniciativa privada vs. setor público vs. profissionais autônomos?

#### 3. Engajamento e Lealdade (Satisfação e Engajamento da Comunidade Alumni)
* **Setores envolvidos:** Acadêmico e Marketing
* Como o NPS da graduação (`satisfacao_graduacao_nps`) se relaciona com a pontuação de engajamento do ex-aluno (`engajamento_alumni_score`)?
* Promotores do curso (NPS 9-10) apresentam maior propensão a se matricular na pós-graduação do que detratores?
* A modalidade de ensino (EAD vs. Presencial) impacta o nível de engajamento pós-formação?

#### 4. Atratividade Geográfica (Geografia e Oportunidades Regionais)
* **Setores envolvidos:** Acadêmico e Marketing
* Quais estados (`uf_residencia`) concentram os egressos com maior potencial de matrícula para campanhas regionalizadas de marketing?
* Qual a distribuição espacial dos profissionais por área de atuação e renda média?

---

## 📁 Datasets Utilizados e LGPD

Os dados foram extraídos do ERP da instituição em formato bruto CSV:
1. **`dataset_egressos_posgrad_erp.csv`**: 15 atributos e 11.660 linhas.
2. **`dataset_cursos.csv`**: 5 atributos e 16 linhas.

Os arquivos estão disponíveis publicamente no repositório GitHub em: [`/dataset`](https://github.com/elis2637/posgrad_pucrio/tree/main/sprint_engenharia_dados/dataset).

### Conformidade com a LGPD
Para assegurar a conformidade com a LGPD (Lei Geral de Proteção de Dados), os IDs reais e os nomes dos egressos foram eliminados. Foi gerada uma chave sequencial anônima de 1 a 11.660 (`id_egresso`) antes de subir os dados para o ambiente público.

---

## 🏗 Arquitetura de Dados e Ingestão (Camada Bronze)

A plataforma utiliza a **Arquitetura Medallion** (Bronze, Silver e Gold) sobre o ecossistema Databricks Delta Lake com Unity Catalog.

### Ingestão dos Dados Brutos (Volume do Unity Catalog)
Os arquivos CSV foram carregados manualmente para o Volume do Unity Catalog no caminho `/Volumes/mvp_eng_dados/bronze/base_dados`.

```python
# Notebook: 01_camada_bronze
from pyspark.sql.functions import current_timestamp, lit

volume_path = "/Volumes/mvp_eng_dados/bronze/base_dados"

# 1. Ingestão do Dataset Egressos direto do CSV no Volume
df_egressos_bronze_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{volume_path}/dataset_egressos_posgrad_erp.csv")
    .withColumn("_data_ingestao", current_timestamp())
    .withColumn("_arquivo_origem", lit(f"{volume_path}/dataset_egressos_posgrad_erp.csv"))
)

# 2. Ingestão do Dataset Cursos direto do CSV no Volume
df_cursos_bronze_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{volume_path}/dataset_cursos.csv")
    .withColumn("_data_ingestao", current_timestamp())
    .withColumn("_arquivo_origem", lit(f"{volume_path}/dataset_cursos.csv"))
)

# Criar Schema Bronze
spark.sql("CREATE SCHEMA IF NOT EXISTS mvp_eng_dados.bronze")

# Gravação nas tabelas Delta Bronze
df_egressos_bronze_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mvp_eng_dados.bronze.dataset_egressos_bruto")
df_cursos_bronze_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mvp_eng_dados.bronze.dataset_cursos_bruto")
```

---

## 📐 Modelagem Dimensional e Catálogo de Dados (Camadas Silver e Gold)

A camada Gold implementa a **Modelagem Dimensional em Esquema Estrela (Star Schema)**.

### Diagrama Entidade-Relacionamento (DER) - Modelo Estrela
```
         +-----------------------+
         |      dim_egresso      |
         +-----------------------+
         | PK | id_egresso       |
         |    | idade_egresso    |
         |    | uf_residencia    |
         |    | bolsista_graduacao|
         +-----------+-----------+
                     |
                     | 1:N
                     v
+--------------------+--------------------+         +-----------------------+
|                   fato_egressos         |         |       dim_curso       |
+-----------------------------------------+         +-----------------------+
| FK | id_egresso                         |<---+1:N-| PK | id_curso         |
| FK | id_curso                           |         |    | nome_curso       |
| FK | id_emprego                         |         |    | area_conhecimento|
|    | dt_ultimo_contato                  |         |    | duracao_semestres|
|    | renda_mensal_estimada              |         |    | mensalidade_base |
|    | satisfacao_graduacao_nps           |         |    | modalidade_grad...|
|    | categoria_nps                      |         +-----------------------+
|    | engajamento_alumni_score           |
|    | potencial_matricula_pos            |         +-----------------------+
|    | meses_desde_formacao               |         |      dim_emprego      |
|    | faixa_meses_formacao               |         +-----------------------+
+--------------------+--------------------+<---+1:N-| PK | id_emprego (Hash)|
                     |                              |    | nivel_cargo      |
                     +------------------------------|    | tipo_empresa     |
                                                    +-----------------------+
```

### Catálogo de Tabelas da Camada Gold (`mvp_eng_dados.gold`)

A camada final conta com 9 tabelas (1 Fato, 3 Dimensões e 5 Data Marts de KPIs):

#### 1. Tabela Fato: `fato_egressos`
* **Descrição:** Registro individual de acompanhamento do egresso e métricas agregadas.
* **Atributos:**
  * `id_egresso` (INT, FK): Identificador único do egresso.
  * `id_curso` (INT, FK): Identificador do curso de formação.
  * `id_emprego` (STRING, FK): Hash MD5 para o perfil de emprego.
  * `dt_ultimo_contato` (TIMESTAMP): Data do último contato registrado.
  * `renda_mensal_estimada` (DECIMAL(10,2)): Valor estimado da renda mensal do egresso.
  * `satisfacao_graduacao_nps` (INT): Pontuação NPS de satisfação (0 a 10).
  * `categoria_nps` (STRING): Classificação em Promotor, Neutro ou Detrator.
  * `engajamento_alumni_score` (INT): Pontuação de engajamento do ex-aluno.
  * `potencial_matricula_pos` (INT): Indicador binário/potencial de pós-graduação.
  * `meses_desde_formacao` (INT): Meses decorridos desde a formatura.
  * `faixa_meses_formacao` (STRING): Agrupamento temporal do tempo pós-formado.

#### 2. Dimensão: `dim_egresso`
* **Descrição:** Perfil demográfico do egresso.
* **Atributos:** `id_egresso` (INT, PK), `idade_egresso` (INT), `uf_residencia` (STRING), `bolsista_graduacao` (INT).

#### 3. Dimensão: `dim_curso`
* **Descrição:** Dados acadêmicos e financeiros dos cursos.
* **Atributos:** `id_curso` (INT, PK), `nome_curso` (STRING), `area_conhecimento` (STRING), `duracao_semestres` (INT), `mensalidade_base` (DECIMAL(10,2)), `modalidade_graduacao` (STRING).

#### 4. Dimensão: `dim_emprego`
* **Descrição:** Classificação de mercado do egresso (gerada via Surrogate Key Hash MD5 dos campos de cargo e empresa).
* **Atributos:** `id_emprego` (STRING, PK), `nivel_cargo` (STRING), `tipo_empresa` (STRING).

#### 5. Data Marts de KPIs (`mvp_eng_dados.gold`)
* **`kpi_conversao_tempo_bolsa`**: Análise de conversão por faixa de tempo de formação e condição de bolsista (`faixa_meses_formacao`, `bolsista_graduacao`, `total_egressos`, `total_potencial_pos`, `taxa_potencial_pos_pct`).
* **`kpi_empregabilidade_roi_curso`**: Empregabilidade e ROI do curso (`nome_curso`, `modalidade_graduacao`, `nivel_cargo`, `tipo_empresa`, `mensalidade_base`, `qtd_egressos`, `renda_media`, `razao_roi_renda_mensalidad`).
* **`kpi_nps_engajamento_alumni`**: NPS e engajamento Alumni (`categoria_nps`, `satisfacao_graduacao_nps`, `modalidade_graduacao`, `total_egressos`, `media_engajamento_alumni`, `total_potencial_pos`, `taxa_potencial_pos_pct`).
* **`kpi_perfil_profissional_uf`**: Distribuição geográfica e perfil profissional (`uf_residencia`, `area_atuacao`, `nivel_cargo`, `tipo_empresa`, `qtd_egressos`, `total_potencial_pos`, `renda_media`, `media_meses_formado`, `taxa_potencial_pos_pct`).
* **`kpi_potencial_pos_graduacao`**: Potencial de pós-graduação por curso e área (`nome_curso`, `area_conhecimento`, `modalidade_graduacao`, `total_egressos`, `total_potencial_pos`, `media_nps`, `taxa_potencial_pos_pct`).

---

## 🧹 Pipeline ETL e Tratamento de Qualidade dos Dados

No notebook [`02_camada_silver.ipynb`](https://github.com/elis2637/posgrad_pucrio/blob/main/sprint_engenharia_dados/notebooks/02_camada_silver.ipynb), foram aplicadas regras estritas de sanidade de dados e limpeza:

### Tratamentos Aplicados no Dataset de Egressos
1. **Deduplicação:** Remoção de duplicatas exatas utilizando `.dropDuplicates()` no conjunto explícito de colunas de negócio.
2. **Sanitização de Limites Biológicos e Financeiros:**
   * `idade_egresso`: Validada no intervalo pragmático entre 16 e 100 anos. Fora da faixa, convertida para `NULL`.
   * `renda_mensal_estimada`: Valores `< 0` foram invalidados para `NULL`; mantidos formatados em `decimal(10,2)`.
   * `meses_desde_formacao`: Valores `< 0` convertidos para `NULL`.
   * `satisfacao_graduacao_nps`: Validada no intervalo de 0 a 10; valores fora do limite convertidos para `NULL`.
3. **Padronização de Texto:** Aplicação de `UPPER(TRIM(...))` nos campos de texto (`uf_residencia`, `nivel_cargo`, `area_atuacao`, `modalidade_graduacao`, `tipo_empresa`).
4. **Tipagem Temporal:** Conversão de `dt_ultimo_contato` para `TIMESTAMP` via `to_timestamp()`.
5. **Feature Engineering:** Criação da flag `is_empregado` (atribuindo `0` se o tipo da empresa for `"DESEMPREGADO"`, e `1` nos demais casos).

### Tratamentos Aplicados no Dataset de Cursos
1. **Filtragem e Chaves:** Remoção de `id_curso` nulos e deduplicação pela chave primária.
2. **Validação Financeira e Carga Horária:** `duracao_semestres` precisa ser `> 0` e `mensalidade_base` `>= 0` (convertida em `decimal(10,2)`).
3. **Padronização:** Aplicação de `UPPER(TRIM(...))` em `nome_curso` e `area_conhecimento`.

### Estratégias de Persistência e Rastreabilidade
* **Auditoria:** Inclusão da coluna `_data_processamento_silver` com `current_timestamp()` em ambas as tabelas.
* **Persistência do Dataset de Egressos:** Ingestão no modo `overwrite` com substituição de schema (`overwriteSchema=true`).
* **Persistência do Dataset de Cursos:** Implementação de carga idempotente via **UPSERT (MERGE)** no Delta Lake baseando-se na PK `id_curso` (`whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()`).

---

## ⚡ Data Marts de KPIs e Otimização

Para assegurar alta performance nas consultas de Business Intelligence sem exigir *data scanning* extensivo das tabelas completas, aplicou-se a otimização **Z-Ordering** nas tabelas Delta físicas da camada Gold. 

Essa abordagem garante uma arquitetura de **Fonte Única da Verdade (Single Source of Truth - SSOT)**, unindo a governança na ingestão bruta com a velocidade nas análises agregadas executivas.

---

## 📊 Análise de Dados

A análise técnica completa e a resposta individual às 11 perguntas de negócio foram consolidadas no notebook dedicado [`05_analise_dados_output.ipynb`](https://github.com/elis2637/posgrad_pucrio/blob/main/sprint_engenharia_dados/notebooks/05_analise_dados_output.ipynb).

---

## 📂 Estrutura do Repositório

```text
.
├── dataset/
│   ├── dataset_cursos.csv                      # Dataset bruto de cursos (16 linhas)
│   └── dataset_egressos_posgrad_erp.csv       # Dataset bruto de egressos (11.660 linhas)
│
├── notebooks/
│   ├── 01_camada_bronze.ipynb                 # Ingestão bruta para Delta Lake (Bronze)
│   ├── 02_camada_silver.ipynb                 # Limpeza, qualidade e sanitização (Silver)
│   ├── 03_camada_gold.ipynb                   # Modelagem Estrela e Data Marts (Gold)
│   ├── 04_constraints_gold_output.ipynb       # Mapeamento DDL de PK/FK no Unity Catalog
│   ├── 05_analise_dados_output.ipynb          # Respostas técnicas às 11 perguntas de negócio
│   └── 06_dashboard_dados_egressos.pdf        # Protótipo do Dashboard construído no Databricks
│
└── README.md                                  # Documentação completa do projeto MVP
```

---

## 📝 Autoavaliação e Trabalhos Futuros

### Atingimento dos Objetivos Delineados
* **Diagnóstico e Mapeamento de Negócio:** Formulação bem-sucedida das 11 perguntas em 4 categorias operacionais.
* **Privacidade e LGPD:** Anonymização completa dos datasets antes da publicação pública.
* **Governança no Databricks:** Implementação bem-sucedida da Arquitetura Medallion via Unity Catalog e Modelagem Estrela na camada Gold.
* **Sanidade de Dados:** Eliminação eficaz de inconsistências, outliers e ruídos.

### Dificuldades Encontradas
* **Ingestão Manual:** Ausência de conector automático de ETL entre o ERP institucional e a nuvem nesta fase de POC, demandando ingestão via Volumes do Unity Catalog.
* **Qualidade da Base Bruta:** Alta incidência de ruídos, duplicidades e ausência de padronização nos registros brutos oriundos de migrações de sistemas anteriores.
* **Restrições Relacionais no Delta Lake:** Como o Delta Lake gerencia restrições relacionais de forma informativa, exigiu-se a construção de rotinas de DDL para assegurar a consistência do Diagrama Entidade-Relacionamento.

### Trabalhos Futuros
1. **Automação do Pipeline de Dados:**
   * Orquestração automatizada utilizando **Databricks Workflows / Jobs** ou **Apache Airflow** conectando via API.
   * Ingestão incremental streaming / micro-batch via **Databricks Auto Loader**.
2. **Modelagem Preditiva (Machine Learning):**
   * Construção de modelos de classificação e propensão de matrícula na pós-graduação.
   * Algoritmos de *Clustering* para segmentação preditiva do Alumni e estimação de *Lifetime Value* (LTV).
3. **Dashboards Dinâmicos:**
   * Lapidação e integração dos Data Marts de KPIs com ferramentas de BI como Power BI ou Databricks Dashboards nativos.
4. **CI/CD e Qualidade Automatizada:**
   * Implementação de testes automatizados de qualidade de dados (Great Expectations ou Delta Live Tables) e rastreabilidade de experimentos via MLflow no GitHub.