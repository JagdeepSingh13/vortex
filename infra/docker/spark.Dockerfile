# Spark image for the streaming jobs (ADR 0016).
#
# Spark 4.1.3 (Scala 2.13, Java 21, Python 3) plus the jars it needs, each pinned by version and
# SHA-256: the build fails if a download does not match. Versions must match each other:
#   iceberg-spark-runtime-4.1_2.13   Spark minor 4.1 and Scala 2.13, as the Spark image
#   spark-sql-kafka-0-10_2.13 4.1.3  same version as Spark; kafka-clients and commons-pool2 at the
#                                    versions its POM declares
#   iceberg-aws-bundle 1.12.0        same version as the Iceberg runtime (S3FileIO)
FROM apache/spark:4.1.3-scala2.13-java21-python3-ubuntu@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211

USER root

ARG MAVEN=https://repo1.maven.org/maven2

ADD --chmod=644 --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /opt/spark/jars/

# streaming checkpoints live on a volume mounted here; it must belong to the spark user
RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints

USER spark
WORKDIR /opt/vortex
