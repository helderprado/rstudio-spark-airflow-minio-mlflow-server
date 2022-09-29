library(sparklyr)
library(dplyr)
library(ggplot2)
library(carrier)
library(mlflow)
library(dplyr)
library(reticulate)

use_condaenv('mlflow')

# alterar caminho da variável de ambiente do mlflow no python
Sys.setenv(MLFLOW_BIN="/usr/local/airflow/.local/share/r-miniconda/envs/mlflow/bin/mlflow")

# alterar caminho da variável de ambiente do python
Sys.setenv(MLFLOW_PYTHON_BIN="/usr/local/airflow/.local/share/r-miniconda/envs/mlflow/bin/python")

path <- "/usr/local/spark/app/@bronze/2017.csv"

df <- read.df("/usr/local/spark/app/@bronze/2017.csv", "csv")

# Desconectar alguma conexão ativa com o spark
spark_disconnect_all()

# Definir configurações iniciais
conf <- spark_config()

# alterar memória utilizada pelo núcleo spark
conf$spark.driver.memory <- "16g"

# conectar ao spark
sc <- spark_connect(master = "local", config = conf)

#ler o arquivo csv e atribuir o dataframe
df <- spark_read_csv(sc, name = "df",  path = path)

# verificar as 5 primeiras linhas do dataset
head(df)

#selecionar algumas colunas
df <- select(df, AIR_TIME, DISTANCE)

# retirar missing values do dataset
df <- df %>% 
  na.omit

# inicializar o mlflow
mlflow_set_tracking_uri('http://mlflow:5000')

# criar o experimento
mlflow_set_experiment("/regressao-linear")

with(mlflow_start_run(), {
  
  # refazer o modelo dentro do encapsulamento do mlflow
  airline_lm <- ml_linear_regression(df, AIR_TIME ~ DISTANCE)
  
  # valores fitted do modelo
  fitted <- ml_predict(airline_lm, df)
  
  # armazenar o rmse, mae e r2
  rmse <- airline_lm$summary$root_mean_squared_error
  mae <- airline_lm$summary$mean_squared_error
  r2 <- airline_lm$summary$r2
  
  # printar mensagens no log do mlflow
  message("  RMSE: ", rmse)
  message("  MAE: ", mae)
  message("  R2: ", r2)
  
  # logar as métricas do run atual do mlflow
  mlflow_log_metric("rmse", rmse)
  mlflow_log_metric("r2", r2)
  mlflow_log_metric("mae", mae)
  
  packaged_airline_lm <- carrier::crate(
    function(x) sparklyr::ml_predict(airline_lm),
    airline_lm = airline_lm
  )
  
  mlflow_log_model(packaged_airline_lm, "airline")
})

predict <- mlflow_load_model('models:/airline/production')

predict(df)

spark_disconnect(sc)
