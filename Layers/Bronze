# Caminho do arquivo
arquivo = "Files/Landing/Fashion_Retail_Sales.csv"

# Leitura do CSV
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(arquivo)

# Visualização dos dados
display(df)

# Leitura do arquivo CSV
bronze_path = "Files/Bronze/Fashion_Sales"

df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(arquivo)

# Renomear colunas para nomes amigáveis e traduzidos
df_bronze = df_bronze.withColumnRenamed("Customer Reference ID", "id_cliente") \
    .withColumnRenamed("Item Purchased", "item_comprado") \
    .withColumnRenamed("Purchase Amount (USD)", "valor_compra") \
    .withColumnRenamed("Date Purchase", "data_compra") \
    .withColumnRenamed("Review Rating", "avaliacao") \
    .withColumnRenamed("Payment Method", "forma_pagamento")

# Salvar como Parquet
df_bronze.write.format("parquet").mode("overwrite").save(bronze_path)
