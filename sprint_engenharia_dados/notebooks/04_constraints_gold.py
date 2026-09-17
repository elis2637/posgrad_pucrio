# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,DDL - Primary Keys e Foreign Keys (Camada Gold)
# MAGIC %md
# MAGIC # DDL Constraints - Chaves Primárias e Estrangeiras
# MAGIC
# MAGIC Este notebook documenta os comandos DDL para criação de **Primary Keys (PK)** e **Foreign Keys (FK)** nas tabelas da camada Gold do modelo estrela.
# MAGIC
# MAGIC ## Modelo de dados
# MAGIC
# MAGIC ```
# MAGIC   dim_curso (pk_curso)       dim_egresso (pk_egresso)       dim_emprego (pk_emprego)
# MAGIC        │                          │                              │
# MAGIC        └──────────┬───────────────┘                              │
# MAGIC                   ▼                                              │
# MAGIC               fato_egressos ───────────────────────────────────┘
# MAGIC                   (3 FKs)
# MAGIC ```
# MAGIC
# MAGIC ## Regras
# MAGIC - As PKs devem ser criadas **antes** das FKs que as referenciam
# MAGIC - Colunas de PK devem ser `NOT NULL`
# MAGIC - As constraints são **informativas** (não há enforced pelo Databricks)
# MAGIC - O diagrama ERD é visível no Catalog Explorer após a criação das FKs

# COMMAND ----------

# DBTITLE 1,1. Primary Keys
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 1. PRIMARY KEYS - Tabelas de Dimensão
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- dim_curso: chave primária em id_curso
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_curso
# MAGIC   ALTER COLUMN id_curso SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_curso
# MAGIC   DROP CONSTRAINT IF EXISTS pk_curso;
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_curso
# MAGIC   ADD CONSTRAINT pk_curso PRIMARY KEY (id_curso);
# MAGIC
# MAGIC -- dim_egresso: chave primária em id_egresso
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_egresso
# MAGIC   ALTER COLUMN id_egresso SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_egresso
# MAGIC   DROP CONSTRAINT IF EXISTS pk_egresso;
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_egresso
# MAGIC   ADD CONSTRAINT pk_egresso PRIMARY KEY (id_egresso);
# MAGIC
# MAGIC -- dim_emprego: chave primária em id_emprego
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_emprego
# MAGIC   ALTER COLUMN id_emprego SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_emprego
# MAGIC   DROP CONSTRAINT IF EXISTS pk_emprego;
# MAGIC ALTER TABLE mvp_eng_dados.gold.dim_emprego
# MAGIC   ADD CONSTRAINT pk_emprego PRIMARY KEY (id_emprego);

# COMMAND ----------

# DBTITLE 1,2. Foreign Keys
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 2. FOREIGN KEYS - Tabela de Fatos (fato_egressos)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- FK: fato_egressos.id_curso -> dim_curso.id_curso
# MAGIC ALTER TABLE mvp_eng_dados.gold.fato_egressos
# MAGIC   ADD CONSTRAINT fk_egressos_curso
# MAGIC   FOREIGN KEY (id_curso) REFERENCES mvp_eng_dados.gold.dim_curso;
# MAGIC
# MAGIC -- FK: fato_egressos.id_egresso -> dim_egresso.id_egresso
# MAGIC ALTER TABLE mvp_eng_dados.gold.fato_egressos
# MAGIC   ADD CONSTRAINT fk_egressos_egresso
# MAGIC   FOREIGN KEY (id_egresso) REFERENCES mvp_eng_dados.gold.dim_egresso;
# MAGIC
# MAGIC -- FK: fato_egressos.id_emprego -> dim_emprego.id_emprego
# MAGIC ALTER TABLE mvp_eng_dados.gold.fato_egressos
# MAGIC   ADD CONSTRAINT fk_egressos_emprego
# MAGIC   FOREIGN KEY (id_emprego) REFERENCES mvp_eng_dados.gold.dim_emprego;

# COMMAND ----------

# DBTITLE 1,3. Verificação das Constraints
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- 3. VERIFICAÇÃO - Listar constraints criadas
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Verificar constraints da tabela de fatos
# MAGIC DESCRIBE TABLE EXTENDED mvp_eng_dados.gold.fato_egressos;
# MAGIC
# MAGIC -- Verificar constraints das dimensões
# MAGIC DESCRIBE TABLE EXTENDED mvp_eng_dados.gold.dim_curso;
# MAGIC DESCRIBE TABLE EXTENDED mvp_eng_dados.gold.dim_egresso;
# MAGIC DESCRIBE TABLE EXTENDED mvp_eng_dados.gold.dim_emprego;