# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import col, trim, upper, to_date, regexp_replace
from pyspark.sql.functions import sum as spark_sum

# COMMAND ----------

# MAGIC %md
# MAGIC ### Objetivo do Projeto
# MAGIC Com a proximidade das eleições de 2026, a disponibilidade de informações sobre candidaturas, despesas eleitorais e eleitorado - temos informações suficientes analisar de forma mais estruturada e clara os custos envolvidos no processo eleitoral.
# MAGIC
# MAGIC Dessa forma, o trabalho busca consolidar e tornar mais objetivas informações públicas, permitindo observar os custos associados às candidaturas e sua distribuição por estados, categorias de despesas e eleitorado, contribuindo para uma análise mais clara dos dados disponibilizados à população.
# MAGIC
# MAGIC ### Perguntas de Negócio
# MAGIC
# MAGIC Com base nesse contexto, o objetivo deste trabalho é responder os seguintes questionamentos:
# MAGIC
# MAGIC **1.** Quais são os 5 estados com maior valor total de despesas eleitorais contratadas em 2026?
# MAGIC
# MAGIC **1.1.** Quais são os 3 candidatos com maior valor total de despesas contratadas em cada um desses 5 estados?
# MAGIC
# MAGIC **2.** Quais são as 3 principais origens das despesas eleitorais contratadas em 2026?
# MAGIC
# MAGIC **2.1.** Quais são as 3 principais descrições de despesas dentro de cada uma das 3 principais origens?
# MAGIC
# MAGIC **3.** Qual é o custo das despesas eleitorais contratadas por eleitor em cada estado brasileiro em 2026?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Licença e uso dos dados
# MAGIC
# MAGIC As bases utilizadas neste projeto são disponibilizadas publicamente pelo Tribunal Superior Eleitoral (TSE) por meio de seu Portal de Dados Abertos e estão identificadas no portal sob a licença **Creative Commons Atribuição (CC BY)**. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Coleta e carregamento de dados
# MAGIC Foram utilizadas três bases disponibilizadas pelo portal de dados abertos do TSE:
# MAGIC
# MAGIC - Base de candidaturas - https://dadosabertos.tse.jus.br/dataset/candidatos-2026#
# MAGIC - Base de despesas eleitorais contratadas (prestação de contas até o dia 24/09/2026) - https://dadosabertos.tse.jus.br/dataset/prestacao-de-contas-eleitorais-2026#
# MAGIC - Base de perfil do eleitorado - https://dadosabertos.tse.jus.br/dataset/eleitorado-atual#
# MAGIC
# MAGIC Os três arquivos foram armazenados manualmente no volume do Databricks abaixo, tendo em vista o tamanho das tabelas e o objetivo principal para responder os questionamentos acima. Também estarão disponíveis no diretório Github onde se ecnontra este notebook.
# MAGIC
# MAGIC `/Volumes/workspace/default/MVP_Engenharia_de_Dados`
# MAGIC
# MAGIC Todos os três resultados representam o snapshot dos arquivos disponíveis na data de execução do projeto

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga e pipeline
# MAGIC O pipeline segue o fluxo:
# MAGIC
# MAGIC `Fonte TSE → Bronze → Silver → Gold → SQL / Análise`
# MAGIC
# MAGIC A camada Bronze preserva os dados recebidos da fonte. A camada Silver realiza padronização, conversão de tipos e preparação dos dados. A camada Gold consolida estruturas analíticas utilizadas nas perguntas de negócio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão e tratamento da Tabela de Candidatos
# MAGIC Leitura do arquivo original e posterior transformação para a camada Silver.

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper

caminho = "/Volumes/workspace/default/MVP_Engenharia_de_Dados"

df_candidatos = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("quote", '"')
    .option("encoding", "UTF-8")
    .csv(f"{caminho}/consulta_cand_2026_BRASIL.csv")
)

display(df_candidatos.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento da Tabela de Candidatos (Silver)
# MAGIC
# MAGIC Após a ingestão dos dados na camada Bronze, foram realizados os seguintes processos de padronização:
# MAGIC
# MAGIC - Conversão do identificador do candidato para o tipo numérico inteiro.
# MAGIC - Remoção de espaços excedentes nos campos textuais.
# MAGIC - Padronização dos campos de partido e estado para letras maiúsculas.
# MAGIC - Renomeação dos campos para uma nomenclatura padronizada na camada Silver.
# MAGIC - Estruturação dos dados com uma linha por candidato, utilizando `ID_CANDIDATO` como identificador da candidatura.

# COMMAND ----------

df_candidatos_silver = df_candidatos.select(
    col("SQ_CANDIDATO").cast("long").alias("ID_CANDIDATO"),
    trim(col("NM_CANDIDATO")).alias("CANDIDATO"),
    upper(trim(col("SG_PARTIDO"))).alias("PARTIDO"),
    trim(col("DS_CARGO")).alias("CARGO"),
    upper(trim(col("SG_UF"))).alias("ESTADO")
)

display(df_candidatos_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão e tratamento da Tabela de Despesas
# MAGIC Leitura do arquivo original e posterior transformação para a camada Silver, incluindo tratamento dos valores monetários e datas.

# COMMAND ----------

df_despesas = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("quote", '"')
    .option("encoding", "UTF-8")
    .csv(f"{caminho}/despesas_contratadas_candidatos_2026_BRASIL.csv")
)

display(df_despesas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento da Tabela de Despesas (Silver)
# MAGIC
# MAGIC Após a ingestão dos dados na camada Bronze, foram realizados os seguintes processos de padronização:
# MAGIC
# MAGIC - Conversão dos identificadores de despesa e candidato para o tipo numérico inteiro.
# MAGIC - Padronização do estado e remoção de espaços excedentes nos campos textuais.
# MAGIC - Conversão dos valores de despesas para o formato numérico decimal.
# MAGIC - Conversão da data da despesa para o formato de data.
# MAGIC - Renomeação e padronização dos campos para a estrutura da `FACT_DESPESA`.
# MAGIC - Manutenção dos registros da fonte, sem exclusão de dados durante a transformação para a camada Silver.

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, to_date, regexp_replace

df_despesas_silver = df_despesas.select(

    col("SQ_DESPESA")
        .cast("long")
        .alias("ID_DESPESA"),

    col("SQ_CANDIDATO")
        .cast("long")
        .alias("ID_CANDIDATO"),

    upper(trim(col("SG_UF")))
        .alias("ESTADO"),

    trim(col("DS_ORIGEM_DESPESA"))
        .alias("CATEGORIA_DESPESA"),

    trim(col("DS_DESPESA"))
        .alias("DESPESA_DETALHADA"),

    regexp_replace(
        regexp_replace(
            trim(col("VR_DESPESA_CONTRATADA")),
            "\\.",
            ""
        ),
        ",",
        "."
    )
    .cast("decimal(18,2)")
    .alias("VALOR_DESPESA"),

    to_date(
        trim(col("DT_DESPESA")),
        "dd/MM/yyyy"
    )
    .alias("DATA_DESPESA")

)

display(df_despesas_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão e tratamento da Tabela de Eleitorado
# MAGIC Leitura do arquivo original e posterior transformação para a camada Silver.

# COMMAND ----------

caminho = "/Volumes/workspace/default/MVP_Engenharia_de_Dados"

df_eleitorado = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("quote", '"')
    .option("encoding", "ISO-8859-1")
    .csv(f"{caminho}/perfil_eleitorado_ATUAL.csv")
)

display(df_eleitorado.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratamento da Tabela de Eleitorado (Silver)
# MAGIC
# MAGIC Após a ingestão dos dados na camada Bronze, foram realizados os seguintes processos de padronização:
# MAGIC
# MAGIC - Conversão dos códigos de município e zona eleitoral para tipos numéricos.
# MAGIC - Padronização do estado para letras maiúsculas e remoção de espaços excedentes.
# MAGIC - Conversão da data de geração para o formato de data.
# MAGIC - Remoção de espaços excedentes nos campos textuais relacionados ao perfil do eleitorado.
# MAGIC - Conversão das quantidades de eleitores para tipos numéricos inteiros.
# MAGIC - Renomeação dos campos para uma nomenclatura padronizada na camada Silver.

# COMMAND ----------

df_eleitorado_silver = df_eleitorado.select(

    to_date(col("DT_GERACAO"), "dd/MM/yyyy").alias("DATA_GERACAO"),

    upper(trim(col("SG_UF"))).alias("ESTADO"),

    col("CD_MUNICIPIO").cast("int").alias("CODIGO_MUNICIPIO"),

    trim(col("NM_MUNICIPIO")).alias("MUNICIPIO"),

    col("NR_ZONA").cast("int").alias("ZONA"),

    trim(col("DS_GENERO")).alias("GENERO"),

    trim(col("DS_ESTADO_CIVIL")).alias("ESTADO_CIVIL"),

    trim(col("DS_FAIXA_ETARIA")).alias("FAIXA_ETARIA"),

    trim(col("DS_GRAU_INSTRUCAO")).alias("GRAU_INSTRUCAO"),

    trim(col("DS_COR_RACA")).alias("COR_RACA"),

    trim(col("TP_OBRIGATORIEDADE_VOTO")).alias("OBRIGATORIEDADE_VOTO"),

    col("QT_ELEITORES").cast("long").alias("ELEITORES"),

    col("QT_ELEITORES_BIOMETRIA").cast("long").alias("ELEITORES_BIOMETRIA"),

    col("QT_ELEITORES_DEFICIENCIA").cast("long").alias("ELEITORES_DEFICIENCIA"),

    col("QT_ELEITORES_NOME_SOCIAL").cast("long").alias("ELEITORES_NOME_SOCIAL")

)

display(df_eleitorado_silver.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modelagem de dados
# MAGIC O projeto foi estruturado utilizando um **esquema estrela**, com a `FACT_DESPESA` como tabela fato central e dimensões relacionadas às principais análises do projeto.
# MAGIC
# MAGIC **FACT_DESPESA**  
# MAGIC Tabela fato central, com uma linha por registro de despesa disponibilizado pela fonte. Contém as medidas de valor e as chaves utilizadas para relacionamento com as dimensões.
# MAGIC
# MAGIC **DIM_CANDIDATO**  
# MAGIC Uma linha por candidato, com `ID_CANDIDATO` como chave primária. Armazena os atributos descritivos utilizados nas análises de candidatos.
# MAGIC
# MAGIC **DIM_ESTADO**  
# MAGIC Uma linha por estado. Além da identificação da unidade federativa, contém o total de eleitores obtido pela agregação da base de eleitorado por estado.
# MAGIC
# MAGIC **DIM_CATEGORIA_DESPESA**  
# MAGIC Uma linha por combinação de categoria e descrição de despesa, concentrando os atributos descritivos utilizados na análise das origens e tipos de gastos.
# MAGIC
# MAGIC **DIM_DATA**  
# MAGIC Uma linha por data de despesa, utilizada para organizar e filtrar as informações temporais da `FACT_DESPESA`.
# MAGIC
# MAGIC **Relacionamentos:** `FACT_DESPESA` se relaciona com `DIM_CANDIDATO`, `DIM_ESTADO`, `DIM_CATEGORIA_DESPESA` e `DIM_DATA`, formando o esquema estrela utilizado na camada analítica.
# MAGIC
# MAGIC **Tratamento do eleitorado:** a tabela de eleitorado permanece como fonte Silver para a modelagem. Seus registros são agregados por `ESTADO` para formar a `DIM_ESTADO`, evitando o relacionamento direto entre o nível detalhado do eleitorado e o nível de detalhe das despesas.
# MAGIC
# MAGIC **Diagrama do modelo:** Abaixo está a representação do esquema estrela utilizado no projeto.
# MAGIC
# MAGIC ![Diagrama Estrela...png](./Diagrama Estrela...png "Diagrama Estrela...png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de dados
# MAGIC
# MAGIC O catálogo de dados abaixo apresenta os principais atributos das tabelas utilizadas no modelo dimensional, relacionando os campos originais às estruturas modeladas durante a camada Silver - com seus tipos, regras de transformação e respectivas funções no modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM_CANDIDATO

# COMMAND ----------

# MAGIC %md
# MAGIC ![DIM_CANDIDATO-.png](./DIM_CANDIDATO-.png "DIM_CANDIDATO-.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FACT_DESPESA

# COMMAND ----------

# MAGIC %md
# MAGIC ![FACT_DESPESA-.png](./FACT_DESPESA-.png "FACT_DESPESA-.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM_ESTADO

# COMMAND ----------

# MAGIC %md
# MAGIC ![DIM_ESTADO-.png](./DIM_ESTADO-.png "DIM_ESTADO-.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM_CATEGORIA_DESPESA

# COMMAND ----------

# MAGIC %md
# MAGIC ![DIM_CATEGORIA_DESPESA..png](./DIM_CATEGORIA_DESPESA..png "DIM_CATEGORIA_DESPESA..png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIM_DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ![DIM DATA..png](./DIM DATA..png "DIM DATA..png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação das camadas
# MAGIC Conferência da quantidade de registros entre Bronze e Silver.

# COMMAND ----------

print("Candidatos Bronze:", df_candidatos.count())
print("Candidatos Silver:", df_candidatos_silver.count())

print("Despesas Bronze:", df_despesas.count())
print("Despesas Silver:", df_despesas_silver.count())

print("Eleitorado Bronze:", df_eleitorado.count())
print("Eleitorado Silver:", df_eleitorado_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação do valor total de despesas
# MAGIC Conferência do valor monetário após a transformação para decimal.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

df_despesas_silver.select(
    spark_sum("VALOR_DESPESA").alias("TOTAL_DESPESAS")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Views de apoio
# MAGIC Criação das views temporárias utilizadas nas análises SQL.

# COMMAND ----------

df_despesas_silver.createOrReplaceTempView("despesas_silver")
df_candidatos_silver.createOrReplaceTempView("candidatos_silver")
df_eleitorado_silver.createOrReplaceTempView("eleitorado_silver")
df_eleitorado.createOrReplaceTempView("eleitorado_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualidade de dados
# MAGIC Aqui foram realizados testes de completude, unicidade, consistência, integridade referencial e identificação de anomalias nas bases utilizadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualidade da tabela de despesas (Silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volume e unicidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS,
# MAGIC     COUNT(DISTINCT ID_DESPESA) AS IDS_DISTINTOS,
# MAGIC     COUNT(*) - COUNT(DISTINCT ID_DESPESA) AS POSSIVEIS_DUPLICIDADES
# MAGIC FROM despesas_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Completude

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS,
# MAGIC
# MAGIC     SUM(CASE WHEN ID_DESPESA IS NULL THEN 1 ELSE 0 END) AS NULOS_ID_DESPESA,
# MAGIC     SUM(CASE WHEN ID_CANDIDATO IS NULL THEN 1 ELSE 0 END) AS NULOS_ID_CANDIDATO,
# MAGIC     SUM(CASE WHEN ESTADO IS NULL THEN 1 ELSE 0 END) AS NULOS_ESTADO,
# MAGIC     SUM(CASE WHEN CATEGORIA_DESPESA IS NULL THEN 1 ELSE 0 END) AS NULOS_CATEGORIA,
# MAGIC     SUM(CASE WHEN DESPESA_DETALHADA IS NULL THEN 1 ELSE 0 END) AS NULOS_DESPESA,
# MAGIC     SUM(CASE WHEN VALOR_DESPESA IS NULL THEN 1 ELSE 0 END) AS NULOS_VALOR,
# MAGIC     SUM(CASE WHEN DATA_DESPESA IS NULL THEN 1 ELSE 0 END) AS NULOS_DATA
# MAGIC
# MAGIC FROM despesas_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Registros anômalos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ID_DESPESA,
# MAGIC     DATA_DESPESA,
# MAGIC     VALOR_DESPESA,
# MAGIC     DESPESA_DETALHADA
# MAGIC FROM despesas_silver
# MAGIC WHERE ID_DESPESA = -1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quantidade de registros anômalos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS QTD_REGISTROS_ANOMALOS
# MAGIC FROM despesas_silver
# MAGIC WHERE ID_DESPESA = -1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Integridade referencial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS DESPESAS_SEM_CANDIDATO
# MAGIC FROM despesas_silver d
# MAGIC LEFT JOIN candidatos_silver c
# MAGIC     ON d.ID_CANDIDATO = c.ID_CANDIDATO
# MAGIC WHERE c.ID_CANDIDATO IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC #### IDs repetidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ID_DESPESA,
# MAGIC     COUNT(*) AS QTD_REGISTROS
# MAGIC FROM despesas_silver
# MAGIC WHERE ID_DESPESA <> -1
# MAGIC GROUP BY ID_DESPESA
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY QTD_REGISTROS DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Quantidade de IDs repetidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS IDS_DUPLICADOS
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         ID_DESPESA
# MAGIC     FROM despesas_silver
# MAGIC     WHERE ID_DESPESA <> -1
# MAGIC     GROUP BY ID_DESPESA
# MAGIC     HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### Investigação de IDs repetidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ID_DESPESA,
# MAGIC     COUNT(*) AS QTD_REGISTROS
# MAGIC FROM despesas_silver
# MAGIC WHERE ID_DESPESA <> -1
# MAGIC GROUP BY ID_DESPESA
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY QTD_REGISTROS DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exemplo de ocorrência

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM despesas_silver
# MAGIC WHERE ID_DESPESA = 73302255;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Registro sem candidato

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.ID_DESPESA,
# MAGIC     d.ID_CANDIDATO,
# MAGIC     d.ESTADO,
# MAGIC     d.CATEGORIA_DESPESA,
# MAGIC     d.DESPESA_DETALHADA,
# MAGIC     d.VALOR_DESPESA,
# MAGIC     d.DATA_DESPESA
# MAGIC FROM despesas_silver d
# MAGIC LEFT JOIN candidatos_silver c
# MAGIC     ON d.ID_CANDIDATO = c.ID_CANDIDATO
# MAGIC WHERE c.ID_CANDIDATO IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão — Despesas Eleitorais
# MAGIC
# MAGIC A tabela de despesas na camada Silver possui **767.497 registros**. Foram identificados **4.967 registros anômalos** com `ID_DESPESA = -1`, data nula, valor `0,00` e descrição `#NULO`. Esses registros foram preservados na Silver, mantendo a rastreabilidade dos dados da fonte.
# MAGIC
# MAGIC Também foram identificados **37.772 IDs de despesa com múltiplas ocorrências**, desconsiderando `ID_DESPESA = -1`. A análise das repetições demonstra que um mesmo identificador pode estar associado a diferentes registros, descrições e valores. Por esse motivo, `ID_DESPESA` não foi considerado uma chave primária simples na modelagem.
# MAGIC
# MAGIC Na análise de integridade referencial, foram identificados **168 registros de despesas sem correspondência na tabela de candidatos**. Esses registros foram mantidos na Silver e documentados como uma limitação de integridade das fontes de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualidade da tabela de candidatos

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volume e unicidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS,
# MAGIC     COUNT(DISTINCT ID_CANDIDATO) AS IDS_DISTINTOS,
# MAGIC     COUNT(*) - COUNT(DISTINCT ID_CANDIDATO) AS POSSIVEIS_DUPLICIDADES
# MAGIC FROM candidatos_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Completude

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS,
# MAGIC
# MAGIC     SUM(CASE WHEN ID_CANDIDATO IS NULL THEN 1 ELSE 0 END) AS NULOS_ID_CANDIDATO,
# MAGIC     SUM(CASE WHEN CANDIDATO IS NULL THEN 1 ELSE 0 END) AS NULOS_CANDIDATO,
# MAGIC     SUM(CASE WHEN PARTIDO IS NULL THEN 1 ELSE 0 END) AS NULOS_PARTIDO,
# MAGIC     SUM(CASE WHEN CARGO IS NULL THEN 1 ELSE 0 END) AS NULOS_CARGO,
# MAGIC     SUM(CASE WHEN ESTADO IS NULL THEN 1 ELSE 0 END) AS NULOS_ESTADO
# MAGIC
# MAGIC FROM candidatos_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Duplicidade de ID_CANDIDATO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ID_CANDIDATO,
# MAGIC     COUNT(*) AS QTD
# MAGIC FROM candidatos_silver
# MAGIC GROUP BY ID_CANDIDATO
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY QTD DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conclusão — candidatos
# MAGIC A tabela de candidatos na camada Silver possui **20.836 registros**, com **20.836 `ID_CANDIDATO` distintos** e sem nulos nos campos analisados. Não foram identificadas duplicidades da chave primária.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualidade da tabela de eleitorado (Silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS
# MAGIC FROM eleitorado_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Completude

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TOTAL_REGISTROS,
# MAGIC     SUM(CASE WHEN DATA_GERACAO IS NULL THEN 1 ELSE 0 END) AS NULOS_DATA_GERACAO,
# MAGIC     SUM(CASE WHEN ESTADO IS NULL THEN 1 ELSE 0 END) AS NULOS_ESTADO,
# MAGIC     SUM(CASE WHEN CODIGO_MUNICIPIO IS NULL THEN 1 ELSE 0 END) AS NULOS_CODIGO_MUNICIPIO,
# MAGIC     SUM(CASE WHEN MUNICIPIO IS NULL THEN 1 ELSE 0 END) AS NULOS_MUNICIPIO,
# MAGIC     SUM(CASE WHEN ZONA IS NULL THEN 1 ELSE 0 END) AS NULOS_ZONA,
# MAGIC     SUM(CASE WHEN GENERO IS NULL THEN 1 ELSE 0 END) AS NULOS_GENERO,
# MAGIC     SUM(CASE WHEN ESTADO_CIVIL IS NULL THEN 1 ELSE 0 END) AS NULOS_ESTADO_CIVIL,
# MAGIC     SUM(CASE WHEN FAIXA_ETARIA IS NULL THEN 1 ELSE 0 END) AS NULOS_FAIXA_ETARIA,
# MAGIC     SUM(CASE WHEN GRAU_INSTRUCAO IS NULL THEN 1 ELSE 0 END) AS NULOS_GRAU_INSTRUCAO,
# MAGIC     SUM(CASE WHEN COR_RACA IS NULL THEN 1 ELSE 0 END) AS NULOS_COR_RACA,
# MAGIC     SUM(CASE WHEN OBRIGATORIEDADE_VOTO IS NULL THEN 1 ELSE 0 END) AS NULOS_OBRIGATORIEDADE_VOTO,
# MAGIC     SUM(CASE WHEN ELEITORES IS NULL THEN 1 ELSE 0 END) AS NULOS_ELEITORES,
# MAGIC     SUM(CASE WHEN ELEITORES_BIOMETRIA IS NULL THEN 1 ELSE 0 END) AS NULOS_ELEITORES_BIOMETRIA,
# MAGIC     SUM(CASE WHEN ELEITORES_DEFICIENCIA IS NULL THEN 1 ELSE 0 END) AS NULOS_ELEITORES_DEFICIENCIA,
# MAGIC     SUM(CASE WHEN ELEITORES_NOME_SOCIAL IS NULL THEN 1 ELSE 0 END) AS NULOS_ELEITORES_NOME_SOCIAL
# MAGIC FROM eleitorado_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Granularidade — teste exploratório

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATA_GERACAO,
# MAGIC     ESTADO,
# MAGIC     CODIGO_MUNICIPIO,
# MAGIC     ZONA,
# MAGIC     GENERO,
# MAGIC     ESTADO_CIVIL,
# MAGIC     FAIXA_ETARIA,
# MAGIC     GRAU_INSTRUCAO,
# MAGIC     COR_RACA,
# MAGIC     OBRIGATORIEDADE_VOTO,
# MAGIC     COUNT(*) AS QTD
# MAGIC FROM eleitorado_silver
# MAGIC GROUP BY
# MAGIC     DATA_GERACAO,
# MAGIC     ESTADO,
# MAGIC     CODIGO_MUNICIPIO,
# MAGIC     ZONA,
# MAGIC     GENERO,
# MAGIC     ESTADO_CIVIL,
# MAGIC     FAIXA_ETARIA,
# MAGIC     GRAU_INSTRUCAO,
# MAGIC     COR_RACA,
# MAGIC     OBRIGATORIEDADE_VOTO
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY QTD DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consulta à Bronze para investigação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM eleitorado_bronze
# MAGIC WHERE SG_UF = 'AC'
# MAGIC   AND CD_MUNICIPIO = 1007
# MAGIC   AND NR_ZONA = 9
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valores mínimos das medidas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(ELEITORES) AS MIN_ELEITORES,
# MAGIC     MIN(ELEITORES_BIOMETRIA) AS MIN_ELEITORES_BIOMETRIA,
# MAGIC     MIN(ELEITORES_DEFICIENCIA) AS MIN_ELEITORES_DEFICIENCIA,
# MAGIC     MIN(ELEITORES_NOME_SOCIAL) AS MIN_ELEITORES_NOME_SOCIAL
# MAGIC FROM eleitorado_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Valores negativos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS REGISTROS_COM_VALOR_NEGATIVO
# MAGIC FROM eleitorado_silver
# MAGIC WHERE ELEITORES < 0
# MAGIC    OR ELEITORES_BIOMETRIA < 0
# MAGIC    OR ELEITORES_DEFICIENCIA < 0
# MAGIC    OR ELEITORES_NOME_SOCIAL < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Conclusão — eleitorado
# MAGIC A tabela de eleitorado na camada Silver possui **1.105.719 registros**, sem nulos nos campos analisados e sem valores negativos nas medidas.
# MAGIC
# MAGIC A base apresenta **granularidade multidimensional**, combinando atributos geográficos e demográficos, não sendo identificada uma chave primária simples. Os registros foram preservados na Silver e utilizados para compor a `DIM_ESTADO`, por meio da agregação por `ESTADO`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construção da camada Gold — esquema estrela
# MAGIC A partir das tabelas Silver, são construídas as dimensões e a tabela fato que formam o esquema estrela da camada Gold.
# MAGIC
# MAGIC **DIM_CANDIDATO:** concentra os atributos descritivos das candidaturas.
# MAGIC
# MAGIC **DIM_ESTADO:** contém uma linha por estado e o total de eleitores obtido pela agregação da base de eleitorado.
# MAGIC
# MAGIC **DIM_CATEGORIA_DESPESA:** concentra as categorias e descrições das despesas.
# MAGIC
# MAGIC **DIM_DATA:** contém uma linha por data de despesa, com atributos temporais derivados.
# MAGIC
# MAGIC **FACT_DESPESA:** contém os registros de despesas e a medida `VALOR_DESPESA`, relacionando-se às quatro dimensões.

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, row_number, lit, coalesce, year, month, dayofmonth, quarter
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### DIM_CANDIDATO

# COMMAND ----------

df_dim_candidato = (
    df_candidatos_silver
    .select("ID_CANDIDATO", "CANDIDATO", "PARTIDO", "CARGO", "ESTADO")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DIM_ESTADO
# MAGIC Agregação do eleitorado por estado para incorporar o total de eleitores à dimensão.

# COMMAND ----------

df_eleitorado_estado = (
    df_eleitorado_silver
    .groupBy("ESTADO")
    .agg(spark_sum("ELEITORES").alias("TOTAL_ELEITORES"))
)

estados_fact = df_despesas_silver.select("ESTADO").distinct()

w_estado = Window.orderBy("ESTADO")

df_dim_estado = (
    estados_fact
    .join(df_eleitorado_estado, on="ESTADO", how="left")
    .withColumn("TOTAL_ELEITORES", coalesce(col("TOTAL_ELEITORES"), lit(0).cast("long")))
    .withColumn("ID_ESTADO", row_number().over(w_estado))
    .select("ID_ESTADO", "ESTADO", "TOTAL_ELEITORES")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DIM_CATEGORIA_DESPESA
# MAGIC Uma linha por combinação de categoria e descrição de despesa.

# COMMAND ----------

w_categoria = Window.orderBy("CATEGORIA_DESPESA", "DESPESA_DETALHADA")

df_dim_categoria_despesa = (
    df_despesas_silver
    .select("CATEGORIA_DESPESA", "DESPESA_DETALHADA")
    .dropDuplicates()
    .withColumn("ID_CATEGORIA_DESPESA", row_number().over(w_categoria))
    .select(
        "ID_CATEGORIA_DESPESA",
        "CATEGORIA_DESPESA",
        "DESPESA_DETALHADA"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DIM_DATA
# MAGIC Uma linha por data de despesa, com atributos temporais derivados.

# COMMAND ----------

from pyspark.sql.functions import col, date_format, year, month, dayofmonth, quarter
from pyspark.sql.types import StructType, StructField, LongType, DateType, IntegerType

df_dim_data = (
    df_despesas_silver
    .select("DATA_DESPESA")
    .where(col("DATA_DESPESA").isNotNull())
    .dropDuplicates()
    .withColumn(
        "ID_DATA",
        date_format("DATA_DESPESA", "yyyyMMdd").cast("long")
    )
    .withColumn("ANO", year("DATA_DESPESA"))
    .withColumn("MES", month("DATA_DESPESA"))
    .withColumn("DIA", dayofmonth("DATA_DESPESA"))
    .withColumn("TRIMESTRE", quarter("DATA_DESPESA"))
    .select(
        "ID_DATA",
        "DATA_DESPESA",
        "ANO",
        "MES",
        "DIA",
        "TRIMESTRE"
    )
)

schema_dim_data = StructType([
    StructField("ID_DATA", LongType(), False),
    StructField("DATA_DESPESA", DateType(), True),
    StructField("ANO", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("DIA", IntegerType(), True),
    StructField("TRIMESTRE", IntegerType(), True)
])

df_data_desconhecida = spark.createDataFrame(
    [(0, None, None, None, None, None)],
    schema_dim_data
)

df_dim_data = df_data_desconhecida.unionByName(df_dim_data)

display(df_dim_data.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### FACT_DESPESA
# MAGIC A tabela fato recebe as chaves das dimensões por meio dos relacionamentos com as tabelas dimensionais.

# COMMAND ----------

# Dimensão candidata com registro desconhecido para preservar o registro órfão identificado na Silver.
df_candidato_desconhecido = spark.createDataFrame(
    [(-1, "CANDIDATO NÃO IDENTIFICADO", None, None, None)],
    schema=df_dim_candidato.schema
)

df_dim_candidato = (
    df_dim_candidato
    .unionByName(df_candidato_desconhecido)
)

df_fact_despesa = (
    df_despesas_silver.alias("f")
    .join(
        df_dim_estado.alias("e"),
        col("f.ESTADO") == col("e.ESTADO"),
        "left"
    )
    .join(
        df_dim_categoria_despesa.alias("d"),
        (col("f.CATEGORIA_DESPESA") == col("d.CATEGORIA_DESPESA")) &
        (col("f.DESPESA_DETALHADA") == col("d.DESPESA_DETALHADA")),
        "left"
    )
    .join(
        df_dim_data.alias("dt"),
        col("f.DATA_DESPESA") == col("dt.DATA_DESPESA"),
        "left"
    )
    .join(
        df_dim_candidato.alias("c"),
        col("f.ID_CANDIDATO") == col("c.ID_CANDIDATO"),
        "left"
    )
    .select(
        col("f.ID_DESPESA"),
        coalesce(col("c.ID_CANDIDATO"), lit(-1)).alias("ID_CANDIDATO"),
        col("e.ID_ESTADO"),
        col("d.ID_CATEGORIA_DESPESA"),
        coalesce(col("dt.ID_DATA"), lit(0)).alias("ID_DATA"),
        col("f.VALOR_DESPESA")
    )
)

display(df_fact_despesa.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validação das Estruturas Gold

# COMMAND ----------

display(df_dim_candidato.limit(10))
display(df_dim_estado.orderBy("ESTADO"))
display(df_dim_categoria_despesa.limit(10))
display(df_dim_data.limit(10))
display(df_fact_despesa.limit(10))

# COMMAND ----------

df_dim_candidato.createOrReplaceTempView("dim_candidato")
df_dim_estado.createOrReplaceTempView("dim_estado")
df_dim_categoria_despesa.createOrReplaceTempView("dim_categoria_despesa")
df_dim_data.createOrReplaceTempView("dim_data")
df_fact_despesa.createOrReplaceTempView("fact_despesa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparação da Gold Estadual
# MAGIC Para as análises estaduais, a `FACT_DESPESA` é agregada por `DIM_ESTADO`, utilizando o total de eleitores da dimensão.

# COMMAND ----------

df_gold_estado = (
    df_fact_despesa.alias("f")
    .join(
        df_dim_estado.alias("e"),
        col("f.ID_ESTADO") == col("e.ID_ESTADO"),
        "left"
    )
    .filter(col("e.ESTADO") != "BR")
    .groupBy(
        col("e.ID_ESTADO"),
        col("e.ESTADO"),
        col("e.TOTAL_ELEITORES")
    )
    .agg(
        spark_sum("f.VALOR_DESPESA").alias("TOTAL_DESPESAS")
    )
)

# COMMAND ----------

display(
    df_gold_estado
    .filter(col("ESTADO") != "BR")
    .orderBy(col("TOTAL_DESPESAS").desc())
)

# COMMAND ----------

df_gold_estado.createOrReplaceTempView("gold_estado")

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabela Gold Analítica (Flat)
# MAGIC
# MAGIC Após a construção do esquema estrela, foi criada uma tabela analítica desnormalizada a partir da `FACT_DESPESA` e de suas dimensões. A estrutura reúne em uma única tabela as informações de candidato, estado, categoria de despesa, data, eleitorado e valor da despesa, facilitando o consumo dos dados nas consultas SQL.

# COMMAND ----------

df_gold_flat = (
    df_fact_despesa.alias("f")
    .join(
        df_dim_candidato.alias("c"),
        col("f.ID_CANDIDATO") == col("c.ID_CANDIDATO"),
        "left"
    )
    .join(
        df_dim_estado.alias("e"),
        col("f.ID_ESTADO") == col("e.ID_ESTADO"),
        "left"
    )
    .join(
        df_dim_categoria_despesa.alias("d"),
        col("f.ID_CATEGORIA_DESPESA") == col("d.ID_CATEGORIA_DESPESA"),
        "left"
    )
    .join(
        df_dim_data.alias("dt"),
        col("f.ID_DATA") == col("dt.ID_DATA"),
        "left"
    )
    .select(
        col("f.ID_DESPESA"),
        col("f.ID_CANDIDATO"),
        col("c.CANDIDATO"),
        col("c.PARTIDO"),
        col("c.CARGO"),
        col("e.ID_ESTADO"),
        col("e.ESTADO"),
        col("e.TOTAL_ELEITORES"),
        col("d.ID_CATEGORIA_DESPESA"),
        col("d.CATEGORIA_DESPESA"),
        col("d.DESPESA_DETALHADA"),
        col("dt.ID_DATA"),
        col("dt.DATA_DESPESA"),
        col("dt.ANO"),
        col("dt.MES"),
        col("dt.DIA"),
        col("dt.TRIMESTRE"),
        col("f.VALOR_DESPESA")
    )
)

display(df_gold_flat.limit(10))

# COMMAND ----------

df_gold_flat.createOrReplaceTempView("gold_despesa_analitica")

# COMMAND ----------

# DBTITLE 1,Solução dos problemas propostos
# MAGIC %md
# MAGIC ### Solução dos problemas propostos
# MAGIC
# MAGIC A partir do processamento e da modelagem das bases de candidaturas, despesas eleitorais e eleitorado, foi construída uma estrutura analítica em esquema estrela, permitindo consolidar as informações em diferentes dimensões e facilitar sua consulta.
# MAGIC
# MAGIC Com os dados preparados na camada Gold, torna-se possível realizar análises em SQL sobre a distribuição das despesas eleitorais, os candidatos envolvidos, as categorias de gastos e a relação entre despesas e eleitorado. A tabela analítica `gold_despesa_analitica` foi criada para reunir essas informações em uma única estrutura e passou a ser utilizada diretamente nas perguntas de negócio, facilitando o consumo e a comparação dos resultados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Quais são os 5 estados com maior valor total de despesas eleitorais contratadas em 2026?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ESTADO,
# MAGIC     SUM(VALOR_DESPESA) AS TOTAL_DESPESAS
# MAGIC FROM gold_despesa_analitica
# MAGIC WHERE ESTADO <> 'BR'
# MAGIC GROUP BY ESTADO
# MAGIC ORDER BY TOTAL_DESPESAS DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise
# MAGIC
# MAGIC Os resultados mostram que **São Paulo apresenta o maior valor total de despesas eleitorais contratadas, com R$ 505,97 milhões**, seguido por **Rio de Janeiro (R$ 278,53 milhões)**, **Minas Gerais (R$ 266,35 milhões)**, **Bahia (R$ 215,21 milhões)** e **Paraná (R$ 182,01 milhões)**.
# MAGIC
# MAGIC Considerando apenas os cinco estados apresentados, o total de despesas é de aproximadamente **R$ 1,45 bilhão**. São Paulo concentra cerca de **34,9% desse valor** e registra um volume aproximadamente **1,8 vez maior que o Rio de Janeiro**, segundo colocado. Os resultados evidenciam uma concentração relevante das despesas entre as maiores unidades federativas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Quais são os 3 candidatos com maior valor total de despesas contratadas em cada um desses 5 estados?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH top5_estados AS (
# MAGIC     SELECT
# MAGIC         ESTADO
# MAGIC     FROM gold_despesa_analitica
# MAGIC     WHERE ESTADO <> 'BR'
# MAGIC     GROUP BY ESTADO
# MAGIC     ORDER BY SUM(VALOR_DESPESA) DESC
# MAGIC     LIMIT 5
# MAGIC ),
# MAGIC
# MAGIC despesas_por_candidato AS (
# MAGIC     SELECT
# MAGIC         g.ESTADO,
# MAGIC         g.ID_CANDIDATO,
# MAGIC         g.CANDIDATO,
# MAGIC         g.PARTIDO,
# MAGIC         g.CARGO,
# MAGIC         SUM(g.VALOR_DESPESA) AS TOTAL_DESPESAS_CANDIDATO
# MAGIC     FROM gold_despesa_analitica g
# MAGIC     INNER JOIN top5_estados t
# MAGIC         ON g.ESTADO = t.ESTADO
# MAGIC     GROUP BY
# MAGIC         g.ESTADO,
# MAGIC         g.ID_CANDIDATO,
# MAGIC         g.CANDIDATO,
# MAGIC         g.PARTIDO,
# MAGIC         g.CARGO
# MAGIC ),
# MAGIC
# MAGIC ranking AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY ESTADO
# MAGIC             ORDER BY TOTAL_DESPESAS_CANDIDATO DESC
# MAGIC         ) AS RANK
# MAGIC     FROM despesas_por_candidato
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ESTADO,
# MAGIC     RANK,
# MAGIC     CANDIDATO,
# MAGIC     PARTIDO,
# MAGIC     CARGO,
# MAGIC     TOTAL_DESPESAS_CANDIDATO
# MAGIC FROM ranking
# MAGIC WHERE RANK <= 3
# MAGIC ORDER BY ESTADO, RANK;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise
# MAGIC
# MAGIC Os resultados mostram diferenças na distribuição das despesas entre os três candidatos com maiores valores em cada estado. Em **São Paulo**, o primeiro colocado concentra aproximadamente **59,3%** do valor total dos três candidatos analisados, apresentando uma diferença significativa em relação ao segundo e ao terceiro colocados. Em **Bahia e Rio de Janeiro**, o primeiro candidato também representa cerca de metade das despesas dos três primeiros.
# MAGIC
# MAGIC Já em **Paraná**, os valores dos dois primeiros candidatos são bastante próximos, enquanto em **Minas Gerais** observa-se uma distribuição mais equilibrada entre os três primeiros colocados. A análise permite, assim, observar como as despesas contratadas se distribuem entre os principais candidatos de cada estado, além de evidenciar diferentes níveis de concentração entre essas candidaturas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2.** Quais são as 3 principais origens das despesas eleitorais contratadas em 2026?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CATEGORIA_DESPESA,
# MAGIC     SUM(VALOR_DESPESA) AS TOTAL_DESPESAS
# MAGIC FROM gold_despesa_analitica
# MAGIC GROUP BY CATEGORIA_DESPESA
# MAGIC ORDER BY TOTAL_DESPESAS DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise
# MAGIC
# MAGIC Entre as três principais origens de despesas, **Publicidade por materiais impressos** apresenta o maior valor, com aproximadamente **R$ 652,76 milhões**, seguida por **Serviços prestados por terceiros**, com **R$ 600,40 milhões**. Os valores das duas categorias são próximos, com uma diferença de aproximadamente **R$ 52,36 milhões**.
# MAGIC
# MAGIC Já **Atividades de militância e mobilização de rua** totalizam aproximadamente **R$ 46,69 milhões**, valor consideravelmente inferior aos dois primeiros. Somadas, as duas principais categorias representam cerca de **96,4% do total das três maiores origens**, indicando forte concentração das despesas nessas duas categorias de propaganda direta.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Quais são as 3 principais descrições de despesas dentro de cada uma das 3 principais origens?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH top3_origens AS (
# MAGIC     SELECT
# MAGIC         CATEGORIA_DESPESA
# MAGIC     FROM gold_despesa_analitica
# MAGIC     GROUP BY CATEGORIA_DESPESA
# MAGIC     ORDER BY SUM(VALOR_DESPESA) DESC
# MAGIC     LIMIT 3
# MAGIC ),
# MAGIC
# MAGIC despesas_por_origem AS (
# MAGIC     SELECT
# MAGIC         g.CATEGORIA_DESPESA,
# MAGIC         g.DESPESA_DETALHADA,
# MAGIC         SUM(g.VALOR_DESPESA) AS TOTAL_DESPESAS
# MAGIC     FROM gold_despesa_analitica g
# MAGIC     INNER JOIN top3_origens t
# MAGIC         ON g.CATEGORIA_DESPESA = t.CATEGORIA_DESPESA
# MAGIC     GROUP BY
# MAGIC         g.CATEGORIA_DESPESA,
# MAGIC         g.DESPESA_DETALHADA
# MAGIC ),
# MAGIC
# MAGIC ranking AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY CATEGORIA_DESPESA
# MAGIC             ORDER BY TOTAL_DESPESAS DESC
# MAGIC         ) AS RANK
# MAGIC     FROM despesas_por_origem
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     CATEGORIA_DESPESA,
# MAGIC     RANK,
# MAGIC     DESPESA_DETALHADA,
# MAGIC     TOTAL_DESPESAS
# MAGIC FROM ranking
# MAGIC WHERE RANK <= 3
# MAGIC ORDER BY CATEGORIA_DESPESA, RANK;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise
# MAGIC
# MAGIC Dentro das três principais origens de despesas, observa-se que cada categoria apresenta uma concentração em determinados tipos de gasto. Em **Atividades de militância e mobilização de rua**, a maior despesa é **Cabo Eleitoral**, com aproximadamente **R$ 19,54 milhões**, seguida por **Panfletagem (R$ 7,73 milhões)** e **Militância (R$ 7,35 milhões)**.
# MAGIC
# MAGIC Em **Publicidade por materiais impressos**, destacam-se **Santinhos**, com aproximadamente **R$ 44,69 milhões**, seguidos por **Wind Banner (R$ 2,95 milhões)** e **Windbanner (R$ 1,95 milhão)**. Já em **Serviços prestados por terceiros**, os maiores valores estão concentrados em **Serviços especializados de comunicação e marketing (R$ 31,0 milhões)**, **Serviços de planejamento e marketing político (R$ 13,1 milhões)** e **Serviços de Marketing Digital (R$ 10,0 milhões)**. Os resultados mostram quais tipos específicos de despesas possuem maior participação dentro de cada uma das principais categorias.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Qual é o custo das despesas eleitorais contratadas por eleitor em cada estado brasileiro em 2026?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH estado AS (
# MAGIC     SELECT
# MAGIC         ESTADO,
# MAGIC         SUM(VALOR_DESPESA) AS TOTAL_DESPESAS,
# MAGIC         MAX(TOTAL_ELEITORES) AS TOTAL_ELEITORES
# MAGIC     FROM gold_despesa_analitica
# MAGIC     WHERE ESTADO <> 'BR'
# MAGIC     GROUP BY ESTADO
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     ESTADO,
# MAGIC     TOTAL_DESPESAS,
# MAGIC     TOTAL_ELEITORES,
# MAGIC     ROUND(
# MAGIC         TOTAL_DESPESAS / NULLIF(TOTAL_ELEITORES, 0),
# MAGIC         2
# MAGIC     ) AS CUSTO_POR_ELEITOR
# MAGIC FROM estado
# MAGIC ORDER BY CUSTO_POR_ELEITOR DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise
# MAGIC
# MAGIC Os resultados mostram diferenças relevantes no custo das despesas eleitorais contratadas por eleitor entre os estados. **Roraima (RR)** apresenta o maior valor, com **R$ 172,49 por eleitor**, seguido por **Amapá (R$ 110,31)** e **Acre (R$ 69,68)**. Na sequência aparecem **Rondônia (R$ 65,74)** e **Tocantins (R$ 59,77)**.
# MAGIC
# MAGIC Essa diferença mostra que, em alguns estados, há uma **maior intensidade de recursos contratados em relação ao tamanho do eleitorado**. Roraima, por exemplo, apresenta um custo por eleitor aproximadamente **56% superior ao Amapá**. Esse indicador pode servir como ponto de partida para investigar por que determinadas unidades federativas concentram mais recursos por eleitor, embora, isoladamente, não permita concluir que maiores gastos resultem em maior alcance ou impacto sobre os eleitores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Considerações Finais

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise consolidada dos problemas levantados
# MAGIC
# MAGIC A análise conjunta das perguntas de negócio evidencia que o principal aspecto observado não está apenas no volume das despesas eleitorais, mas na forma como esses recursos se distribuem entre estados, candidatos e categorias de gasto. Os resultados demonstram diferenças relevantes no volume total contratado e, principalmente, no custo das despesas em relação ao tamanho do eleitorado de cada estado.
# MAGIC
# MAGIC Esse cenário revela uma forte assimetria na aplicação dos recursos, com concentração das despesas em determinadas unidades federativas, candidatos e categorias. Embora os dados não permitam avaliar a efetividade ou o impacto desses gastos sobre os eleitores, as diferenças observadas indicam um ponto relevante para investigação: compreender quais fatores explicam essas disparidades e como o volume de recursos contratados se relaciona com a escala de cada eleitorado.
# MAGIC
# MAGIC Esse levantamento também abre espaço para questionamentos relacionados ao acompanhamento, à transparência e à distribuição dos recursos empregados no processo eleitoral, especialmente diante das diferenças observadas entre as unidades federativas e entre as categorias de despesas.
# MAGIC
# MAGIC De forma geral, o trabalho permitiu responder aos questionamentos definidos no início do projeto, consolidando as informações em uma estrutura analítica que facilita a consulta e a interpretação dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pontos de Melhoria
# MAGIC
# MAGIC Uma das principais oportunidades de evolução do projeto seria o tratamento mais aprofundado dos registros incompletos ou anômalos identificados nas bases de origem. Embora esses registros tenham sido preservados na camada Silver para manter a fidelidade aos dados disponibilizados pelo TSE, uma etapa adicional de tratamento poderia classificar, corrigir ou excluir registros inadequados antes da consolidação da camada Gold, aumentando a precisão das métricas e análises realizadas.
# MAGIC
# MAGIC Outra melhoria seria a automatização da atualização das bases. Devido ao volume dos arquivos e às limitações do ambiente Databricks utilizado no projeto, a coleta e o processamento foram realizados manualmente diretamente no Databricks, com os códigos disponibilizados no GitHub. Em uma implementação produtiva, seria desejável automatizar a ingestão e o processamento periódico dos dados, garantindo maior atualização e reprodutibilidade do pipeline, principalmente em períodos eleitorais, nos quais informações atualizadas podem contribuir para análises e acompanhamento do processo eleitoral.
# MAGIC
# MAGIC Por fim, poderia ser implementado um processo de monitoramento da qualidade dos dados a cada nova carga. Indicadores como quantidade de nulos, registros anômalos, chaves sem correspondência, duplicidades e variações no volume das bases poderiam ser verificados automaticamente, permitindo identificar alterações ou problemas na fonte antes que afetem as análises.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoavaliação
# MAGIC
# MAGIC Em um âmbito pessoal, este foi um projeto extremamente enriquecedor e prazeroso de realizar. O módulo foi uma introdução a um nível de programação com o qual eu ainda não estava acostumado, mas fiquei muito feliz por ter tido a oportunidade de praticar, aprender e desenvolver análises que, no início do módulo, eu não imaginava ser capaz de construir.
# MAGIC
# MAGIC Independentemente do resultado final, estou muito feliz com a minha entrega e, principalmente, com o meu desenvolvimento ao longo do projeto. 
# MAGIC
# MAGIC Consegui escolher um tema às vésperas de um momento importante, trabalhar com dados reais e construir uma análise que pode ser útil para pessoas interessadas no assunto. Provavelmente em alguns meses, vou enxergar diversas coisas que poderiam ser melhoradas, mas acredito que isso faz parte do processo de adaptação a uma nova área. 
# MAGIC
# MAGIC Saio deste módulo mais confiante nos meus conhecimentos e ainda mais entusiasmado para continuar aprendendo e me desenvolvendo com um próximo desafio.