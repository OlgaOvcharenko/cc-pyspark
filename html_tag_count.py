import re

from collections import Counter

from pyspark.sql import SparkSession

from sparkcc import CCSparkJob


class TagCountJob(CCSparkJob):
    """ Count HTML tag names in Common Crawl WARC files"""

    name = "TagCount"

    # match HTML tags (element names) on binary HTML data
    html_tag_pattern = re.compile(b'<([a-z0-9]+)')

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            # skip non-HTML or unknown content types
            return
        data = record.content_stream().read()
        counts = Counter(TagCountJob.html_tag_pattern.findall(data))
        for tag, count in counts.items():
            yield tag.decode('ascii').lower(), count


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[8]") \
        .appName("Spark_count tags") \
        .config("spark.driver.memory", "9g").getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "6")

    spark.sparkContext.setLogLevel("ERROR")

    job = TagCountJob()
    job.run()
