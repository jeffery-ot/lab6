FROM docker.io/bitnami/spark:3.5

USER root

# Update packages and install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 python3-pip \
    unzip curl groff less && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark

# Copy and install Python requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Prepare spark folders and permissions
RUN mkdir -p /opt/bitnami/spark/jars \
    && mkdir -p /opt/bitnami/spark/tmp \
    && mkdir -p /tmp/.ivy2 \
    && chmod -R 777 /opt/bitnami/spark/tmp \
    && chmod -R 777 /tmp/.ivy2

# Set HOME to avoid Ivy issues
ENV HOME=/tmp

# Download compatible JARs for Spark 3.5
# Delta Lake 3.0.0 is compatible with Spark 3.5
RUN curl -L -o /opt/bitnami/spark/jars/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar

# Add AWS SDK and Hadoop JARs for S3 support (updated versions for Spark 3.5)
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar && \
    curl -L -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.525.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.525/aws-java-sdk-bundle-1.12.525.jar

# Download additional Delta Lake dependencies
RUN curl -L -o /opt/bitnami/spark/jars/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# Set proper permissions for all JAR files
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Create a custom spark-defaults.conf
RUN echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" > /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access=true" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.jars.ivy=/tmp/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.committer.name=directory" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.committer.staging.conflict-mode=append" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.fast.upload=true" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.block.size=134217728" >> /opt/bitnami/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.multipart.size=67108864" >> /opt/bitnami/spark/conf/spark-defaults.conf

# Optional: set default user back (commented)
# USER 1001