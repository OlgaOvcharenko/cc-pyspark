#!/bin/sh

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --output="logs/%j-client.out"
#SBATCH --error="logs/%j-client.err"
#SBATCH --time=00:20:00
#SBATCH --job-name=spark-client

#./get_data.sh
mkdir -p tmp
mkdir -p logs

source "python_venv/bin/activate"

module load jdk
module load python/3.7.4

SPARK_HOME="../spark-on-euler/spark_home/spark-3.2.2-bin-hadoop3.2"

master_file=$(ls -t ../spark-on-euler/logs/spark-master/ | head -1)

url_master=$(cat ../spark-on-euler/logs/spark-master/$master_file)

$SPARK_HOME/bin/spark-submit \
           --conf "spark.local.dir=tmp" \
           --master $url_master \
           --driver-memory 20g \
           --executor-memory 20g \
           --deploy-mode client \
           --py-files sparkcc.py,libs.zip \
           --archives python_venv.zip \
           --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python_venv/bin/python \
           doc_link.py \
           --num_output_partitions 1000 \
           --log_level ERROR \
           --output_format parquet \
           --output_compression None \
           ./input/test_warc.txt docs
