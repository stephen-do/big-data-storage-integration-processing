FROM apache/spark-py:v3.4.0

USER root

RUN apt-get update && apt install -y wget

RUN cd /opt/spark/jars \
 && wget "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.99/aws-java-sdk-bundle-1.12.99.jar" \
 && wget "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
 && wget "https://download.oracle.com/otn-pub/otn_software/jdbc/1921/ojdbc10.jar" \
 && wget "https://jdbc.postgresql.org/download/postgresql-42.5.4.jar" \
 && wget "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar" \
 && wget "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar" \
 && wget "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.230/bundle-2.17.230.jar" \
 && wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/1.4.3/iceberg-core-1.4.3.jar" \
 && wget "https://repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.8/caffeine-3.1.8.jar"

ENV SPARK_HOME=/opt/spark
WORKDIR /opt/spark/work-dir

ARG spark_uid=185
USER ${spark_uid}
