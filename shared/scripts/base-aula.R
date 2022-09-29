library(sparklyr)
library(dplyr)
library(ggplot2)
library(carrier)
library(mlflow)
library(dplyr)
library(reticulate)
library(stats)
library(glue)

use_condaenv('mlflow')

# alterar caminho da variável de ambiente do mlflow no python
Sys.setenv(MLFLOW_BIN="/home/rstudio/.local/share/r-miniconda/envs/mlflow/bin/mlflow")

# alterar caminho da variável de ambiente do python
Sys.setenv(MLFLOW_PYTHON_BIN="/home/rstudio/.local/share/r-miniconda/envs/mlflow/bin/python")

anos <- list.files('shared/@gold/airline.parquet/')

elementos <- c()

for (ano in anos) {
  for (mes in list.files(glue('shared/@gold/airline.parquet/{ano}'))) {
    elementos <- c(elementos,paste('shared/@gold/airline.parquet',ano,mes,sep="/"))
  }
}

paths <- tail(elementos,3)

paths

# Desconectar alguma conexão ativa com o spark
spark_disconnect_all()

# Definir configurações iniciais
conf <- spark_config()

# alterar memória utilizada pelo núcleo spark
conf$spark.driver.memory <- "6"
conf$spark.executor.memory <- "6G"
conf$spark.driver.maxResultSize <- "6g"

# conectar ao spark
sc <- spark_connect(master = "local", config = conf)

#ler o arquivo csv e atribuir o dataframe
df <- spark_read_parquet(sc, name = "df",  path = paths)

# verificar as 5 primeiras linhas do dataset
head(df)

#selecionar algumas colunas
df <- select(df, AIR_TIME, DISTANCE)

# retirar missing values do dataset
df <- df %>% 
  na.omit

df <- collect(df)

# inicializar o mlflow
mlflow_set_tracking_uri('http://mlflow:5000')

# criar o experimento
mlflow_set_experiment("/regressao-linear")

with(mlflow_start_run(), {
  
  mlflow_log_param("fórmula", "AIR_TIME ~ DISTANCE")
  
  # refazer o modelo dentro do encapsulamento do mlflow
  airline_lm <- lm(formula=AIR_TIME ~ DISTANCE, data=df)
  
  # sumário do modelo
  summary <- summary(airline_lm)
  
  # valores fitted do modelo
  fitted <- predict(airline_lm, df)
  
  # armazenar o r2 e r2 ajsutado
  r2 <- summary$r.squared
  r2_ajustado <- summary$adj.r.squared
  
  # printar mensagens no log do mlflow
  message("  r2: ", r2)
  message("  r2_ajustado: ", r2_ajustado)
  
  # logar as métricas do run atual do mlflow
  mlflow_log_metric("r2", r2)
  mlflow_log_metric("r2_ajustado", r2_ajustado)
  
  packaged_airline_lm <- carrier::crate(
    function(x) stats::predict.lm(airline_lm, .x),
    airline_lm = airline_lm
  )
  
  mlflow_log_model(packaged_airline_lm, "airline")
  
})


