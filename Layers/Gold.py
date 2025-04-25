from pyspark.sql.functions import monotonically_increasing_id, row_number, year, month, dayofmonth,col
from pyspark.sql.window import Window

#Caminho Silver
silver_path = "Files/Silver/Fashion_Sales"
gold_path = "Files/Gold/Fashion_Sales"

#Ler Silver
df = spark.read.format("parquet").load(silver_path)

# ----------------------------
#DIMENSÃO CLIENTE
dim_cliente = df.select("id_cliente").dropDuplicates()
dim_cliente = dim_cliente.withColumn("sk_cliente", row_number().over(Window.orderBy(monotonically_increasing_id())))

# ----------------------------
#DIMENSÃO ITEM
dim_item = df.select("item_comprado").dropDuplicates()
dim_item = dim_item.withColumn("sk_item", row_number().over(Window.orderBy(monotonically_increasing_id())))

# ----------------------------
#DIMENSÃO TEMPO
dim_tempo = df.select("data_compra", "mes_nome").dropDuplicates()
dim_tempo = dim_tempo.withColumn("sk_tempo", row_number().over(Window.orderBy(monotonically_increasing_id()))) \
    .withColumn("ano", year(col("data_compra"))) \
    .withColumn("mes", month(col("data_compra"))) \
    .withColumn("dia", dayofmonth(col("data_compra")))


# ----------------------------
#DIMENSÃO FORMA DE PAGAMENTO
dim_pagamento = df.select("forma_pagamento").dropDuplicates()
dim_pagamento = dim_pagamento.withColumn("sk_pagamento", row_number().over(Window.orderBy(monotonically_increasing_id())))

# ----------------------------
#TABELA FATO
fato_vendas = df \
    .join(dim_cliente, on="id_cliente", how="left") \
    .join(dim_item, on="item_comprado", how="left") \
    .join(dim_tempo, on="data_compra", how="left") \
    .join(dim_pagamento, on="forma_pagamento", how="left") \
    .select("sk_cliente", "sk_item", "sk_tempo", "sk_pagamento", "valor_compra", "avaliacao")

# ----------------------------
#SALVAR AS TABELAS EM PARQUET
dim_cliente.write.mode("overwrite").parquet(f"{gold_path}/dim_cliente")
dim_item.write.mode("overwrite").parquet(f"{gold_path}/dim_item")
dim_tempo.write.mode("overwrite").parquet(f"{gold_path}/dim_tempo")
dim_pagamento.write.mode("overwrite").parquet(f"{gold_path}/dim_pagamento")
fato_vendas.write.mode("overwrite").parquet(f"{gold_path}/fato_vendas")


dim_cliente.show(5)
dim_item.show(5)
dim_tempo.show(5)
dim_pagamento.show(5)
fato_vendas.show(5)
