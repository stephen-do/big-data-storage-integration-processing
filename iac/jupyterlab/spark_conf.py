import pyspark
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
conf = pyspark.SparkConf()
conf.setMaster("k8s://https://192.168.65.3:6443") # Your master address name
conf.set("spark.kubernetes.container.image", "dongoctuyen/spark:base-v3.4") # Spark image name
conf.set("spark.driver.port", "2222") # Needs to match svc
conf.set("spark.driver.blockManager.port", "7777")
conf.set("spark.driver.host", "jupyterlab.jupyterlab.svc.cluster.local") # Needs to match svc
conf.set("spark.driver.bindAddress", "0.0.0.0")
conf.set("spark.kubernetes.namespace", "jupyterlab")
conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-driver")
conf.set("spark.kubernetes.authenticate.serviceAccountName", "spark-driver")
conf.set("spark.executor.instances", "1")
conf.set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
spark = SparkSession.builder.appName('test').config(conf=conf).getOrCreate()