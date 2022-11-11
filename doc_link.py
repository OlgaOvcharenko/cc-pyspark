import re
from time import sleep
from urllib.request import urlopen, Request
from collections import Counter
import requests
import httplib2
from pyspark.sql import SparkSession

from sparkcc import CCSparkJob


class TagCountJob(CCSparkJob):
    """ Count doc files and their names in Common Crawl WARC files"""

    name = "doc_count"
    # doc_pattern = pattern = re.compile(b"((www|http:|https:)+[^\s]+[\w]\.(doc|docx))")
    doc_pattern = re.compile(b"(('|\")(www|http:|https:)+[^\s]+[\w]\.(doc|docx)('|\"))")
    # doc_google_pattern = re.compile(b"(('|\")(www|http:|https:)+(docs\.google\.com/document)[^\s]+[\w]('|\"))")

    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            # skip non-HTML or unknown content types
            return
        data = record.content_stream().read()
        counts = Counter(TagCountJob.doc_pattern.findall(data))
        for tag, count in counts.items():
            tag = tag[0]
            url = tag.decode('utf-8').lower().replace("\"\\\"", "").replace("\'", "").replace("\"", "")

            try:
                h = httplib2.Http()
                resp = h.request(url, 'HEAD')
                if int(resp[0]['status']) < 400:
                    req = Request(url=url)
                    resp = urlopen(req, timeout=3)
                    not_redirected = resp.geturl().endswith(".doc") or resp.geturl().endswith(".docx") \
                        if resp.geturl() != url else True

                    if not_redirected:
                        yield url, count

            except Exception:
                pass


if __name__ == '__main__':
    job = TagCountJob()
    job.run()
