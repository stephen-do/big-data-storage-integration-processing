"""
Module Name: config.py
Author: tuyendn3
Version: 1.0
Description: a spark config for initialized spark session
"""

from typing import Optional
from pyspark.conf import SparkConf
# import boto3


def get_key():
    """Get Key from Secret Manager"""


def create_spark_config(env: Optional[str] = "prod") -> SparkConf:
    """Create spark config to initialized spark session
    :rtype: SparkConf
    """
    assert isinstance(env, str)
    conf = SparkConf()
    conf.set("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    conf.set("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")
    conf.set("spark.hadoop.fs.s3a.region", 'us-east-1')
    conf.set("spark.sql.catalog.golden", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.golden.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    conf.set("spark.sql.catalog.raw.warehouse", "s3a://raw")
    conf.set("spark.sql.catalog.golden.warehouse", "s3a://golden")
    conf.set("spark.sql.catalog.golden.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.golden.s3.endpoint", "http://minio-service.minio-dev.svc.cluster.local:9000")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    conf.set("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.raw.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    conf.set("spark.sql.catalog.raw.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.raw.s3.endpoint", "http://minio-service.minio-dev.svc.cluster.local:9000")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    conf.set("spark.sql.defaultCatalog", "golden")
    conf.set("spark.sql.catalogImplementation", "in-memory")
    return conf
