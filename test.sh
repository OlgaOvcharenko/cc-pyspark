#!/bin/sh

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --output="logs/%j-client.out"
#SBATCH --error="logs/%j-client.err"
#SBATCH --time=00:40:00
#SBATCH --job-name=spark-client

#./get_data.sh

mkdir -p tmp
mkdir -p logs

source "python_venv/bin/activate"

module load jdk
module load python/3.7.4

SPARK_HOME="../spark-on-euler/spark_home/spark-3.2.3-bin-hadoop3.2"

master_file=$(ls -I "spark-*" -I "app-*" -t ../spark-on-euler/logs/spark-master/ | head -1)

url_master=$(cat ../spark-on-euler/logs/spark-master/$master_file)

nrows=$(wc -l < input/6_test_warc.txt | xargs)

$SPARK_HOME/bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
examples/jars/spark-examples*.jar \
10