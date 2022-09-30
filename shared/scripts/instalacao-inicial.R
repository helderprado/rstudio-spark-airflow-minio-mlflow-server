library(reticulate)
library(sparklyr)

install_miniconda()

conda_install('mlflow', packages=c('mlflow','boto3'), forge = TRUE, pip = FALSE, pip_ignore_installed = TRUE)

spark_install("3.3", hadoop_version = "3")

