import re

from collections import Counter

from pyspark.sql import SparkSession

from sparkcc import CCSparkJob


class TagCountJob(CCSparkJob):
    """ Count doc files and their names in Common Crawl WARC files"""

    name = "doc_count"
    # doc_pattern = pattern = re.compile(b"((www|http:|https:)+[^\s]+[\w]\.(doc|docx))")
    # doc_pattern = re.compile(b"(('|\")(www|http:|https:)+[^\s]+[\w]\.(doc|docx)('|\"))")
    doc_google_pattern = re.compile(b"(('|\")(www|http:|https:)+(docs\.google\.com/document)[^\s]+[\w]('|\"))")

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            # skip non-HTML or unknown content types
            return
        data = record.content_stream().read()
        counts = Counter(TagCountJob.doc_google_pattern.findall(data))
        for tag, count in counts.items():
            tag = tag[0]
            yield tag.decode('ascii').lower().replace("\"\\\"", "").replace("\'", "").replace("\"", ""), count


if __name__ == '__main__':
    # spark = SparkSession.builder.master("local[8]") \
    #     .appName("count_tags") \
    #     .config("spark.driver.memory", "9g").getOrCreate()
    #
    # spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # spark.conf.set("spark.sql.codegen.wholeStage", "false")
    # spark.conf.set("spark.sql.shuffle.partitions", "6")
    #
    # spark.sparkContext.setLogLevel("ERROR")

    job = TagCountJob()
    job.run()
