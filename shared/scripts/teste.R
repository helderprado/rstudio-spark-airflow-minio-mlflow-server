library(sparklyr)

use_condaenv('mlflow')

# alterar caminho da variável de ambiente do mlflow no python
Sys.setenv(MLFLOW_BIN="/home/rstudio/.local/share/r-miniconda/envs/mlflow/bin/mlflow")

# alterar caminho da variável de ambiente do python
Sys.setenv(MLFLOW_PYTHON_BIN="/home/rstudio/.local/share/r-miniconda/envs/mlflow/bin/python")

# Desconectar alguma conexão ativa com o spark
spark_disconnect_all()

# Set configuration:
conf <- spark_config()

# Bypass the JAR's issues:

conf$sparklyr.defaultPackages <- c("com.amazonaws:aws-java-sdk-bundle:1.11.819",
                                   "org.apache.hadoop:hadoop-aws:3.2.3",
                                   "org.apache.hadoop:hadoop-common:3.2.3")


# alterar memória utilizada pelo núcleo spark
conf$spark.driver.memory <- "6"
conf$spark.executor.memory <- "6G"
conf$spark.driver.maxResultSize <- "6g"

sparklyr.log.console = TRUE

# conectar ao spark
sc <- spark_connect(master = "local", config = conf, spark_home = '/home/rstudio/spark/spark-3.3.0-bin-hadoop3')

ctx <- spark_context(sc)

jsc <- invoke_static(sc, 
                     "org.apache.spark.api.java.JavaSparkContext", 
                     "fromSparkContext", 
                     ctx)

# Set the S3 configs: 

hconf <- jsc %>% invoke("hadoopConfiguration")

hconf %>% invoke("set", "fs.s3a.access.key", "admin")
hconf %>% invoke("set", "fs.s3a.secret.key", "sample_key")
hconf %>% invoke("set", "fs.s3a.endpoint", "s3:9000")
hconf %>% invoke("set", "fs.s3a.path.style.access", "true")
hconf %>% invoke("set", "fs.s3a.connection.ssl.enabled", "false")
hconf %>% invoke("set", "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

sc

df <- spark_read_parquet(sc, name="df", path="s3a://raw/data")
  



