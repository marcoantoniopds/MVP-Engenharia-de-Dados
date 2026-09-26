# MVP — Engenharia de Dados

## Análise de Despesas Eleitorais — Eleições 2026

Repositório dedicado ao projeto de MVP desenvolvido na disciplina de Engenharia de Dados da pós-graduação em Ciência de Dados & Analytics da PUC-Rio.

## Contextualização e objetivo

Com a proximidade das eleições de 2026, o projeto busca estruturar informações públicas sobre candidaturas, despesas eleitorais e eleitorado, permitindo analisar de forma mais clara a distribuição dos custos contratados durante o processo eleitoral.

O objetivo é consolidar esses dados por meio de um pipeline de Engenharia de Dados e responder questões relacionadas à distribuição das despesas por estado, candidato, categoria de despesa e eleitorado.

## Tecnologias

- Databricks Community Edition
- Apache Spark / PySpark
- Spark SQL
- GitHub

## Pipeline

Fonte TSE → Bronze → Silver → Gold — Esquema Estrela → Gold Flat → Análises SQL

## Arquivos do projeto

- [Notebook principal do projeto](./MVP%20Engenharia%20de%20Dados.ipynb)
- [Notebook completo com outputs](./MVP%20Engenharia%20de%20Dados%20-%20Com%20Outputs.ipynb)
- [Relatório final em PDF](./Relatório%20MVP%20Engenharia%20de%20Dados.pdf)
- [Imagens do catálogo](./Imagens%20de%20Catálogos/)

### Sobre os notebooks

O projeto possui duas versões do notebook:

- **Notebook principal:** versão integrada diretamente ao repositório Git pelo Databricks, contendo a estrutura completa de código e documentação do projeto.
- **Notebook com outputs:** versão exportada do Databricks após a execução do projeto, preservando os resultados das consultas e demais outputs gerados durante o processamento.

A diferença entre os arquivos ocorre porque, no fluxo de integração direta do notebook com o repositório Git, os resultados de execução não são mantidos no arquivo versionado. Por isso, foi disponibilizada também a versão exportada com os outputs preservados, permitindo a visualização dos resultados diretamente no GitHub.

## Autor

Nome: Marco Antonio Pereira da Silva

Contato: marcoantoniopds@icloud.com
