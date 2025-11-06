# Leitura das Tabelas Bronze no Spark

Para manipular os dados no PySpark, carregamos cada tabela Delta da camada Bronze em DataFrames:

```python
df_amsterdam  = spark.read.format("delta").table("bronze.amsterdam")
df_athens     = spark.read.format("delta").table("bronze.athens")
df_barcelona  = spark.read.format("delta").table("bronze.barcelona")
df_berlin     = spark.read.format("delta").table("bronze.berlin")
df_lisbon     = spark.read.format("delta").table("bronze.lisbon")
df_london     = spark.read.format("delta").table("bronze.london")
df_paris      = spark.read.format("delta").table("bronze.paris")
df_rome       = spark.read.format("delta").table("bronze.rome")
```

# Adição de Colunas de Auditoria na Camada Silver

Nesta etapa, incluímos colunas de auditoria em cada DataFrame da camada Silver:

```python
from pyspark.sql.functions import current_timestamp, lit

df_amsterdam  = df_amsterdam.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("amsterdam"))
df_athens     = df_athens.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("athens"))
df_barcelona  = df_barcelona.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("barcelona"))
df_berlin     = df_berlin.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("berlin"))
df_lisbon     = df_lisbon.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("lisbon"))
df_london     = df_london.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("london"))
df_paris      = df_paris.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("paris"))
df_rome       = df_rome.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("rome"))
```

data_hora_silver: Marca o timestamp de quando o registro foi processado para a camada Silver.

nome_tabela: Identifica a cidade correspondente à tabela, útil para rastreabilidade e integração futura.

Objetivo: Garantir auditoria e rastreabilidade dos dados transformados antes de serem persistidos na camada Silver.


# Tratando e organizando dados

O bloco de codigo abaixo realiza as seguintes operações

- **1. Leitura da tabela Bronze como DataFrame.**
- **2. Renomeação de colunas seguindo o padrão PT-BR.**
- **3. Inclusão das colunas de auditoria:**
    - `DATA_HORA_BRONZE`: indica a origem do registro.
     - `DATA_ARQUIVO_SILVER`: timestamp do processamento para Silver.
- **4. Salvamento da tabela na camada Silver usando Delta Lake, com merge de schema.**
    
```
from pyspark.sql import functions as F

def _apply_name_rules_ptbr(colname: str) -> str:
    
    col_map = {
        "_C0": "CODIGO",
        "REALSUM": "PRECO_TOTAL_DIARIO",
        "ROOM_TYPE": "TIPO_QUARTO",
        "ROOM_SHARED": "QUARTO_COMPARTILHADO",
        "ROOM_PRIVATE": "QUARTO_PRIVADO",
        "PERSON_CAPACITY": "CAPACIDADE_MAXIMA",
        "HOST_IS_SUPERHOST": "ANFITRIAO_SUPERHOST",
        "MULTI": "IMOVEIS_ANFITRIAO",
        "BIZ": "HOSPEDAGEM_CORPORATIVA",
        "CLEANLINESS_RATING": "NOTA_LIMPEZA",
        "GUEST_SATISFACTION_OVERALL": "NOTA_SATISFACAO_HOSPEDES",
        "BEDROOMS": "NUMERO_QUARTOS",
        "DIST": "DISTANCIA_CENTRO_KM",
        "METRO_DIST": "DISTANCIA_METRO_KM",
        "ATTR_INDEX": "INDICE_ATRACOES",
        "ATTR_INDEX_NORM": "INDICE_ATRACOES_NORMALIZADO",
        "REST_INDEX": "INDICE_RESTAURANTES",
        "REST_INDEX_NORM": "INDICE_RESTAURANTES_NORMALIZADO",
        "LNG": "LONGITUDE",
        "LAT": "LATITUDE",
    }
    return col_map.get(colname.upper(), colname.upper())


def renomear_colunas_cidades(src_fqn: str, dest_fqn: str = None):
   
    dest_fqn = dest_fqn or src_fqn

    df = spark.read.format("delta").table(src_fqn)

    new_cols = [_apply_name_rules_ptbr(c) for c in df.columns]
    df = df.toDF(*new_cols)

    df = (
        df
        .withColumn("DATA_HORA_BRONZE", F.lit(src_fqn))  # origem rastreável
        .withColumn("DATA_ARQUIVO_SILVER", F.current_timestamp())
    )

    df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(dest_fqn)


    return dest_fqn
```

# Execução da Função de Renomeação para Todas as Cidades

Os comandos abaixo aplicam a função `renomear_colunas_cidades` para cada cidade, transformando os dados da camada Bronze para a camada Silver:

```
python
renomear_colunas_cidades("bronze.rome", "silver.rome")
renomear_colunas_cidades("bronze.amsterdam", "silver.amsterdam")
renomear_colunas_cidades("bronze.athens", "silver.athens")
renomear_colunas_cidades("bronze.barcelona", "silver.barcelona")
renomear_colunas_cidades("bronze.berlin", "silver.berlin")
renomear_colunas_cidades("bronze.lisbon", "silver.lisbon")
renomear_colunas_cidades("bronze.paris", "silver.paris")
renomear_colunas_cidades("bronze.london", "silver.london")
```
