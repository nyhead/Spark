'''
ASSIGNMENT:
Print out ten visitors with most hits, ten urls with most hits and ten html pages (i.e. urls ending with .html) with most hits.
Save the each of those top ten to a corresponding CSV file.

Sample output:
+--------------------+-----+
|addr                |count|
+--------------------+-----+
|edams.ksc.nasa.gov  |6530 |
|piweba4y.prodigy.com|4846 |
|163.206.89.4        |4791 |
|piweba5y.prodigy.com|4607 |
|piweba3y.prodigy.com|4416 |
|www-d1.proxy.aol.com|3889 |
|www-b2.proxy.aol.com|3534 |
|www-b3.proxy.aol.com|3463 |
|www-c5.proxy.aol.com|3423 |
|www-b5.proxy.aol.com|3411 |
+--------------------+-----+

+---------------------------------------+-----+
|url                                    |count|
+---------------------------------------+-----+
|/images/NASA-logosmall.gif             |97293|
|/images/KSC-logosmall.gif              |75283|
|/images/MOSAIC-logosmall.gif           |67356|
|/images/USA-logosmall.gif              |66975|
|/images/WORLD-logosmall.gif            |66351|
|/images/ksclogo-medium.gif             |62670|
|/ksc.html                              |43619|
|/history/apollo/images/apollo-logo1.gif|37806|
|/images/launch-logo.gif                |35119|
|/                                      |30123|
+---------------------------------------+-----+

+-----------------------------------------------+-----+
|url                                            |count|
+-----------------------------------------------+-----+
|/ksc.html                                      |43619|
|/shuttle/missions/sts-69/mission-sts-69.html   |24592|
|/shuttle/missions/missions.html                |22429|
|/software/winvn/winvn.html                     |10343|
|/history/history.html                          |10111|
|/history/apollo/apollo.html                    |8973 |
|/shuttle/countdown/liftoff.html                |7858 |
|/history/apollo/apollo-13/apollo-13.html       |7160 |
|/shuttle/technology/sts-newsref/stsref-toc.html|6506 |
|/shuttle/missions/sts-69/images/images.html    |5261 |
+-----------------------------------------------+-----+
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, desc
import sys
import os

os.environ['HADOOP_HOME'] = "C:/apps/spark-3.5.1-bin-hadoop3"
sys.path.append("C:/apps/spark-3.5.1-bin-hadoop3/bin")
# Initialize Spark session
spark = SparkSession.builder.appName("AccessLogAnalysis").getOrCreate()

# Define the path to the access log file
access_log_path = 'access_log'

try:
    # Read the access log file into a Spark DataFrame
    logs_df = spark.read.text(access_log_path)

    # Define the regular expression pattern to extract address and URL
    regex_addr = r'^(\S+)'
    regex_url = r'"(?:\S+)\s*(\S+)\s*(?:\S+)\s*"'

    # Extract visitor addresses and URLs
    logs_df = logs_df.withColumn("addr", regexp_extract("value", regex_addr, 1)) \
                    .withColumn("url", regexp_extract("value", regex_url, 1))

    # Count hits per visitor address
    addr_counts = logs_df.groupBy("addr").count().orderBy(desc("count")).limit(10)

    # Count hits per URL
    url_counts = logs_df.groupBy("url").count().orderBy(desc("count")).limit(10)

    # Filter URLs ending with .html and count hits
    html_counts = logs_df.filter(col("url").endswith(".html")) \
                        .groupBy("url").count().orderBy(desc("count")).limit(10)

    # Show results
    print("Top 10 visitors with most hits:")
    addr_counts.show(truncate=False)

    print("Top 10 URLs with most hits:")
    url_counts.show(truncate=False)

    print("Top 10 HTML pages with most hits:")
    html_counts.show(truncate=False)
    
    addr_counts.coalesce(1).write.csv("top_10_visitors.csv", header=True)
    url_counts.coalesce(1).write.csv("top_10_urls.csv", header=True)
    html_counts.coalesce(1).write.csv("top_10_html_pages.csv", header=True)
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()

