# Predição de Matrículas em Pós-Graduação (Alumni Analytics)

![Status](https://img.shields.io/badge/Status-Conclu%C3%ADdo-success)
![Python](https://img.shields.io/badge/Python-3.9+-blue)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-Pipeline-orange)

## 📌 Visão Geral do Projeto
Este repositório contém o modelo preditivo (`modelo_final.pkl`) e os artefatos de análise desenvolvidos para prever a probabilidade de conversão (matrícula) de egressos (alumni) em nossos cursos de Pós-Graduação. 

O foco principal deste projeto de Machine Learning (ML) foi responder a uma pergunta crítica de negócio: **"Quais áreas ou perfis de egressos com alto poder aquisitivo estão recusando nossas ofertas, e por quê?"**

## 🎯 Objetivos de Negócio
1. **Aumentar a Assertividade Comercial:** Substituir campanhas de marketing em massa por abordagens direcionadas baseadas em probabilidade (Propensity Score).
2. **Identificar Atritos na Conversão:** Compreender os motivos pelos quais perfis de alta renda e alto engajamento não retornam para a Pós-Graduação.
3. **Embasar Decisões Estratégicas:** Fornecer dados para o Conselho Diretor justificando a necessidade de atualização das matrizes curriculares.

## 📊 Dados e Features
O modelo utiliza um *Pipeline* customizado do Scikit-Learn integrando pré-processamento avançado. As principais features numéricas extraídas e tratadas incluem:
- `meses_desde_formacao` (Tempo desde a graduação)
- `idade_egresso` (Idade do ex-aluno)
- `renda_mensal_estimada` (Poder aquisitivo)
- `satisfacao_graduacao_nps` (NPS dado ao final da graduação)
- `engajamento_alumni_score` (Métrica de engajamento contínuo com a instituição)

Features categóricas (como *Área de Formação* e *Histórico Profissional*) foram tratadas com `OneHotEncoder` nativo no pipeline.

## 🚀 Como Utilizar o Modelo
O arquivo `modelo_final.pkl` é um pipeline completo (`ColumnTransformer` + Estimador).

```python
import pandas as pd
import joblib

# 1. Carregar o modelo
modelo = joblib.load('modelo_final.pkl')

# 2. Carregar novos dados (exemplo)
novos_dados = pd.read_csv('novos_leads_alumni.csv')

# 3. Prever probabilidade de conversão
probabilidades = modelo.predict_proba(novos_dados)[:, 1]

# 4. Filtrar leads de alta probabilidade para a equipe comercial
novos_dados['Score_Conversao'] = probabilidades
leads_prioritarios = novos_dados[novos_dados['Score_Conversao'] > 0.75]