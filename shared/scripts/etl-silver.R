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

path <- "/usr/local/spark/app/@bronze"

# ler o arquivo csv e atribuir o dataframe
df <- spark_read_csv(sc, name = "df",  path = path, infer_schema = TRUE, header = TRUE)

# dropar a coluna sem nome da dataset
df <- select(df, -Unnamed_27)

# mostrar todas as variáveis em linha
glimpse(df)

# adicionar coluna de mês e ano
df <- df %>% 
  mutate(MONTH = month(FL_DATE),
         YEAR = year(FL_DATE))

# carregar o dataframe tratado para a camada silver
spark_write_parquet(df, path = "/usr/local/spark/app/@silver/airline.parquet", mode = "overwrite")

# desconectar o spark context
spark_disconnect(sc)
  
  
