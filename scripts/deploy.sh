#!/bin/bash

# Spark Submit Script for deploying the Spark Streaming Application

# Set environment variables if needed
# export SPARK_HOME=/path/to/spark
# export HADOOP_CONF_DIR=/path/to/hadoop/conf
# export JAVA_HOME=/path/to/java

# Path to your PySpark script
APP_PYTHON_SCRIPT="path/to/your/main.py"

# Specify any additional Python files that need to be included
# You can zip your entire Python project and specify the zip file here
# APP_PY_FILES="path/to/your/project.zip"

# Kafka and HDFS dependencies, if required
# In many environments, these are already included in the Spark setup
KAFKA_ASSEMBLY_JAR="path/to/spark-sql-kafka-0-10_2.12-3.x.x.jar"
HADOOP_AWS_JAR="path/to/hadoop-aws-3.x.x.jar"

# Define other parameters such as memory and cores
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="4g"
EXECUTOR_CORES=4
NUM_EXECUTORS=2

# The path to a configuration file that your application uses
CONFIG_FILE="path/to/config/config.yaml"

# Run spark-submit with all required parameters
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory $DRIVER_MEMORY \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --num-executors $NUM_EXECUTORS \
  --jars $KAFKA_ASSEMBLY_JAR,$HADOOP_AWS_JAR \
  --files $CONFIG_FILE \
  --py-files $APP_PY_FILES \
  $APP_PYTHON_SCRIPT
