#!/bin/sh

#./get_data.sh

nrows=$(wc -l < input/test_warc.txt | xargs)
#echo "$nrows"

 /usr/local/Cellar/apache-spark/3.3.1/bin/spark-submit ./doc_link.py \
                                   --nstep 240 \
                                   --nrows $nrows \
                                   --num_output_partitions 1 --log_level ERROR \
                                   --output_format csv \
                                   --output_compression None \
                                   ./input/test_warc.txt valid_docs
