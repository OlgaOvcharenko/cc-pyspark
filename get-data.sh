#!/bin/sh

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --output="logs/get_data.out"
#SBATCH --error="logs/get_data.err"
#SBATCH --time=24:00:00
#SBATCH --job-name=spark-client

CRAWL=CC-MAIN-2022-33

# number of links if local, 0 - whole dump
nlinks=0

# base URL used to download the path listings
BASE_URL=https://data.commoncrawl.org

set -e

test -d input || mkdir input

if [ -e input/test.txt ]; then
	echo "File input/test.txt already exist"
	echo "... delete it to write a new one"
	exit 1
fi

for data_type in warc wat wet; do

	echo "Downloading Common Crawl paths listings (${data_type} files of $CRAWL)..."

	mkdir -p crawl-data/$CRAWL/
	listing=crawl-data/$CRAWL/$data_type.paths.gz
	cd crawl-data/$CRAWL/
	wget --timestamping $BASE_URL/$listing
	cd -

	echo "Downloading sample ${data_type} file..."

#	file=$(gzip -dc $listing | head -1)
#	files=$(gzip -dc $listing | head -$nlinks)
	if [ $nlinks -eq 0 ]
	then
	  files=$(gzip -dc $listing)
	else
    files=$(gzip -dc $listing | head -$nlinks)
  fi

	for file in $files
	do
	    mkdir -p $(dirname $file)
      cd $(dirname $file)
      wget --timestamping $BASE_URL/$file
      cd -

      echo "Writing input file listings..."

      input=input/test_${data_type}.txt
      echo "Test file: $input"
      echo file:$PWD/$file >>$input

      input=input/all_${data_type}_${CRAWL}.txt
      echo "All ${data_type} files of ${CRAWL}: $input"
      gzip -dc $listing >$input
	done

done
