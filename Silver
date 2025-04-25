from pyspark.sql.functions import col, to_date, date_format, when, month

# Definir os caminhos
bronze_path = "Files/Bronze/Fashion_Sales"
silver_path = "Files/Silver/Fashion_Sales"

# Ler os dados da camada Bronze
df_silver = spark.read.format("parquet").load(bronze_path)

# Realizar as transformações de colunas, preenchendo valores nulos com valores padrão
df_silver = df_silver.withColumn("valor_compra", col("valor_compra").cast("double")) \
    .withColumn("avaliacao", col("avaliacao").cast("int")) \
    .withColumn("data_compra", to_date(col("data_compra"), "dd-MM-yyyy"))

# Adiciona o número do mês
df_silver = df_silver.withColumn("mes", month(col("data_compra")))

# Criar uma nova coluna com o nome do mês
df_silver = df_silver.withColumn("mes_nome", when(col("mes") == 1, "Janeiro")
                .when(col("mes") == 2, "Fevereiro")
                .when(col("mes") == 3, "Março")
                .when(col("mes") == 4, "Abril")
                .when(col("mes") == 5, "Maio")
                .when(col("mes") == 6, "Junho")
                .when(col("mes") == 7, "Julho")
                .when(col("mes") == 8, "Agosto")
                .when(col("mes") == 9, "Setembro")
                .when(col("mes") == 10, "Outubro")
                .when(col("mes") == 11, "Novembro")
                .when(col("mes") == 12, "Dezembro")
                .otherwise("Desconhecido"))

# Preencher valores nulos com valores padrão
df_silver = df_silver.fillna({"valor_compra": 0.0, "avaliacao": 0, "data_compra": "1900-01-01"})

# Salvar os dados na camada Silver
df_silver.write.format("parquet").mode("overwrite").save(silver_path)

# Exibir as 5 primeiras linhas
df_silver.show(5)
