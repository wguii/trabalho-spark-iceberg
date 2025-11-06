# Leitura das Tabelas Silver para a Camada Gold

Os comandos abaixo carregam as tabelas da camada Silver, preparando os DataFrames para transformação e modelagem dimensional na camada Gold:

```
df_rome        = spark.read.format("delta").table("silver.rome")
df_london      = spark.read.format("delta").table("silver.london")
df_amsterdam   = spark.read.format("delta").table("silver.amsterdam")
df_athens      = spark.read.format("delta").table("silver.athens")
df_barcelona   = spark.read.format("delta").table("silver.barcelona")
df_berlin      = spark.read.format("delta").table("silver.berlin")
df_lisbon      = spark.read.format("delta").table("silver.lisbon")
df_paris       = spark.read.format("delta").table("silver.paris")
```

# Funções de Transformação para a Camada Gold

O trecho de código abaixo define funções de transformação aplicadas aos DataFrames Silver para gerar a camada Gold.

---

### **`arredondar_dist_tamanho_indices(df)`**
Arredonda colunas de interesse para facilitar análise:
- distâncias e metros
- índices normalizados
- preço total diário

---

### **`converter_para_booleano(df, colunas)`**
Converte colunas 0/1 em booleano, facilitando análise e agregações.

---

### **`remover_colunas(df)`**
Remove colunas de auditoria e detalhes redundantes:
- Quartos privados/compartilhados
- Dados de arquivo e timestamp da camada Silver/Bronze
- Índices brutos (não normalizados) que não são necessários na camada Gold

---

### **`gerar_gold(df, cidade)`**
Prepara DataFrame para a camada Gold:

1. Remove colunas de auditoria e detalhes de quarto

2. Converte colunas 0/1 em booleano

3. Arredonda colunas de distâncias, capacidade, índices e preço total

4. Cria coluna **`AVALIACAO_MEDIA`** a partir das notas de limpeza e satisfação do hóspede:
   - Calcula a média entre **`NOTA_LIMPEZA`** e **`NOTA_SATISFACAO_HOSPEDES`**
   - Remove as colunas originais após criar a média para simplificar a tabela

5. Adiciona coluna **`CIDADE`** com o nome da cidade

    


```
from pyspark.sql import functions as F

def arredondar_dist_tamanho_indices(df):
   
    for c in df.columns:
        lower_c = c.lower()
        if any(k in lower_c for k in ["dist", "metro", "capacit", "bedrooms", "normalizado"]):
            df = df.withColumn(c, F.round(F.col(c), 4))
        elif "preco_total_diario" == lower_c:
            df = df.withColumn(c, F.round(F.col(c), 2))
    return df


def converter_para_booleano(df, colunas):
    for c in colunas:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("boolean"))
    return df

def remover_colunas(df):
    colunas_para_remover = [
        "QUARTO_PRIVADO", "QUARTO_COMPARTILHADO",
        "DATA_ARQUIVO_SILVER", "NOME_ARQUIVO_BRONZE",
        "DATA_HORA_BRONZE", "NOME_ARQUIVO",
        "INDICE_ATRACOES", "INDICE_RESTAURANTES"
    ]
    existing = [c for c in colunas_para_remover if c in df.columns]
    return df.drop(*existing)

def gerar_gold(df, cidade):
    
    df = remover_colunas(df)
    df = converter_para_booleano(df, ["IMOVEIS_ANFITRIAO", "HOSPEDAGEM_CORPORATIVA"])
    df = arredondar_dist_tamanho_indices(df)

    if "NOTA_LIMPEZA" in df.columns and "NOTA_SATISFACAO_HOSPEDES" in df.columns:
        df = df.withColumn(
            "AVALIACAO_MEDIA",
            F.round((F.col("NOTA_LIMPEZA") + F.col("NOTA_SATISFACAO_HOSPEDES")) / 2, 2)
        )
        df = df.drop("NOTA_LIMPEZA", "NOTA_SATISFACAO_HOSPEDES")

    df_gold = df.withColumn("CIDADE", F.lit(cidade))
    return df_gold
```

## Dicionário de DataFrames por Cidade

O dicionário `cidades_dfs` organiza os DataFrames Silver por cidade, facilitando iterações para transformações e geração da camada Gold.

```
cidades_dfs = {
    "rome": df_rome,
    "london": df_london,
    "amsterdam": df_amsterdam,
    "athens": df_athens,
    "barcelona": df_barcelona,
    "berlin": df_berlin,
    "lisbon": df_lisbon,
    "paris": df_paris
}
```

## Geração da Camada Gold para Todas as Cidades

O loop abaixo percorre o dicionário `cidades_dfs` e aplica a função `gerar_gold` em cada DataFrame Silver, gerando e salvando os DataFrames Gold correspondentes.

```
for cidade, df in cidades_dfs.items():
    df_gold = gerar_gold(df, cidade)
    
    # Converte colunas específicas para boolean antes de salvar
    df_gold = converter_para_booleano(df_gold, ["IMOVEIS_ANFITRIAO", "HOSPEDAGEM_CORPORATIVA"])
    
    # Salva cada DataFrame Gold como tabela Delta no schema 'gold'
    df_gold.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"gold.{cidade}")
```


