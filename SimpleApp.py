"""SimpleApp.py"""
from pyspark import SparkContext
# this is the location of the cluster
master = "spark://ec2-52-10-98-37.us-west-2.compute.amazonaws.com:7077"

logFile = "googlebooks-eng-all-1gram-20120701-other"  # Should be some file on your system
sc = SparkContext(master, "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)