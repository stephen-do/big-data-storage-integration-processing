"""
Module Name: main.py
Author: tuyendn3
Version: 1.0
Description: an entrypoint session for all spark jobs
"""

import argparse
import importlib
import sys
import os
import configparser
from init.session import create_spark_session

sys.path.insert(0, "jobs.zip")
sys.path.insert(0, "init.zip")
sys.path.insert(0, "shared.zip")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a PySpark job")
    parser.add_argument(
        "--env",
        type=str,
        default="prod",
        dest="env",
        help="""running evironment. (example: --env dev)""",
    )
    parser.add_argument(
        "--job",
        type=str,
        required=True,
        dest="job_name",
        help="""The name of the job module you want to run.
                                (example: --job foo will run job on jobs.foo package)""",
    )
    args = parser.parse_args()
    job_name = args.job_name
    env = args.env
    # read config from SPARK_CONF_DIR/spark-env.sh or spark-defaults.conf
    config = configparser.ConfigParser()
    config.read(
        f"{os.environ.get('SPARK_OTHER_CONFIG_DIR', '/opt/spark/config')}/other_config.ini"
    )

    # create spark session and logger
    spark, logger = create_spark_session(job_name, env)
    logger.info(f"Running application {job_name}...\nenvironment is {env}")
    logger.info(f"Configuration: {list(config.keys())}")
    logger.info(f"Importing job {args.job_name}")
    # import module job to run
    job = importlib.import_module(f"jobs.{job_name}")
    logger.info(f"Imported job {args.job_name} Successfully")
    logger.info(f"Starting job {args.job_name}")
    job.run(spark=spark, logger=logger, config=config, env=env)
    logger.info(f"Run job {args.job_name} Successfully")
    spark.stop()
