import re

from collections import Counter

from pyspark.sql import SparkSession

from sparkcc import CCSparkJob


class TagCountJob(CCSparkJob):
    """ Count HTML tag names in Common Crawl WARC files"""

    name = "doc_count"

    doc_link_pattern = re.compile(b"\.doc")

    # def process_record(self, record):
    #     data = record.content_stream().read()
    #     counts = Counter(TagCountJob.doc_link_pattern.findall(data))
    #     for tag, count in counts.items():
    #         yield tag.decode('ascii').lower(), count

    def run_job(self, session):
        input_data = session.sparkContext.textFile(self.args.input, minPartitions=self.args.num_input_partitions)
        df = input_data.map(lambda x: (x, )).toDF()
        df.show()


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[8]") \
        .appName("count_tags") \
        .config("spark.driver.memory", "9g").getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    spark.sparkContext.setLogLevel("ERROR")

    job = TagCountJob()
    job.run()
