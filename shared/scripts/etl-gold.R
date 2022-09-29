library(sparklyr)
library(dplyr)
library(lubridate)

# Desconectar alguma conexão ativa com o spark
spark_disconnect_all()

# Definir configurações iniciais
conf <- spark_config()

# alterar memória utilizada pelo núcleo spark
conf$spark.driver.memory <- "16g"

# conectar ao spark
sc <- spark_connect(master = "local", config = conf)

path <- "/usr/local/spark/app/@silver/airline.parquet"

# ler o arquivo csv e atribuir o dataframe
df <- spark_read_parquet(sc, name = "df",  path = path)

glimpse(df)

# carregar o dataframe tratado para a camada silver
spark_write_parquet(df, path = "/usr/local/spark/app/@gold/airline.parquet", mode = "overwrite", partition_by = c('YEAR','MONTH'))

# desconectar o spark context
spark_disconnect(sc)


