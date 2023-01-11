#!/bin/sh

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --output="logs/%j-client.out"
#SBATCH --error="logs/%j-client.err"
#SBATCH --time=01:00:00
#SBATCH --job-name=spark-client

module load jdk
module load python/3.7.4

SPARK_ROOT="$../spark-on-euler/spark_home/spark-3.2.3-bin-hadoop3.2"

# read master node address
MASTER_FILE=$(ls -I "spark-*" -I "app-*" -t $(pwd)/spark/logs/spark-master/ | head -1)
SPARK_MASTER_NODE=$(cat ../spark-on-euler/logs/spark-master/$MASTER_FILE)
NROWS=$(wc -l <input/test_warc.txt | xargs)

$SPARK_ROOT/bin/spark-submit \
  --conf "spark.local.dir=tmp" \
  --master "$SPARK_MASTER_NODE" \
  --driver-memory 10g \
  --executor-memory 10g \
  --deploy-mode client \
  --py-files sparkcc.py,libs.zip \
  --archives python_venv.zip \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python_venv/bin/python \
  doc_link.py \
  --nstep 2 \
  --nrows $NROWS \
  --num_output_partitions 1 \
  --log_level ERROR \
  --output_format parquet \
  --output_compression None \
  ./input/test_warc.txt 6_docs_2017_mod
