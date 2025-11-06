# Listagem de Arquivos na Camada Landing

Este comando exibe os arquivos presentes no **volume de dados brutos** do schema `landing`, permitindo verificar quais arquivos foram carregados para processamento no pipeline.

```
display(dbutils.fs.ls('/Volumes/workspace/landing/dados/'))
```

# Leitura de Arquivos CSV da Camada Landing

O código abaixo realiza a leitura dos arquivos CSV brutos de cada cidade, presentes no volume `landing/dados`, e os transforma em DataFrames Spark para posterior processamento no pipeline.
Cada DataFrame criado (`df_amsterdam`, `df_athens`, etc.) corresponde a uma cidade específica, permitindo que o pipeline processe os dados de forma independente por cidade.

```
caminho_landing = '/Volumes/workspace/landing/dados'

df_amsterdam  = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/amsterdam_weekdays.csv")
df_athens     = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/athens_weekdays.csv")
df_barcelona  = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/barcelona_weekdays.csv")
df_berlin     = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/berlin_weekdays.csv")
df_lisbon     = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/lisbon_weekdays.csv")
df_london     = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/london_weekdays.csv")
df_paris      = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/paris_weekdays.csv")
df_rome       = spark.read.option("inferschema", "true").option("header", "true").csv(f"{caminho_landing}/rome_weekdays.csv")
```

# Adição de Colunas de Auditoria na Camada Bronze

Neste passo, adicionamos duas colunas de auditoria a cada DataFrame da camada Bronze:

- **data_hora_bronze** → registra o timestamp de quando o arquivo foi carregado no pipeline.
- **nome_arquivo** → armazena o nome do arquivo CSV de origem para rastreabilidade.

```
from pyspark.sql.functions import current_timestamp, lit

df_amsterdam  = df_amsterdam.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("amsterdam_weekdays.csv"))
df_athens     = df_athens.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("athens_weekdays.csv"))
df_barcelona  = df_barcelona.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("barcelona_weekdays.csv"))
df_berlin     = df_berlin.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("berlin_weekdays.csv"))
df_lisbon     = df_lisbon.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("lisbon_weekdays.csv"))
df_london     = df_london.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("london_weekdays.csv"))
df_paris      = df_paris.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("paris_weekdays.csv"))
df_rome       = df_rome.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("rome_weekdays.csv"))
```

# Persistência dos DataFrames na Camada Bronze

Após a preparação dos DataFrames com as colunas de auditoria, cada DataFrame é salvo como uma tabela Delta na camada Bronze.

```
df_amsterdam.write.format('delta').mode("overwrite").saveAsTable("bronze.amsterdam")
df_athens.write.format('delta').mode("overwrite").saveAsTable("bronze.athens")
df_barcelona.write.format('delta').mode("overwrite").saveAsTable("bronze.barcelona")
df_berlin.write.format('delta').mode("overwrite").saveAsTable("bronze.berlin")
df_lisbon.write.format('delta').mode("overwrite").saveAsTable("bronze.lisbon")
df_london.write.format('delta').mode("overwrite").saveAsTable("bronze.london")
df_paris.write.format('delta').mode("overwrite").saveAsTable("bronze.paris")
df_rome.write.format('delta').mode("overwrite").saveAsTable("bronze.rome")
```

# Listagem de Tabelas na Camada Bronze

Após a criação das tabelas Delta para cada cidade, é possível verificar todas as tabelas existentes no schema `bronze` com o comando SQL:

```
%sql
SHOW TABLES IN bronze
```
