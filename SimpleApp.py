#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
# this is the location of the cluster
master = "spark://ec2-52-10-98-37.us-west-2.compute.amazonaws.com:7077"
appName = "DataScience HW1 CT"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

class WordFreqCluster:
  """HW1 Task2, examine unigram frequencies, find those with highly correlated frequencies and those with low correlated frequencies"""
  def __init__(self):
    self.ngramsFile = "googlebooks-eng-all-1gram-20120701-other"  # Should be some file on HDFS
    self.ngramsData = sc.textFile(self.ngramsFile).cache()

  def wordFrequency(self, word):
    result = self.ngramsData.filter(lambda s: word in s).count()
    return result




if __name__ == "__main__":
  task2 = WordFreqCluster()

  # numAs = logData.filter(lambda s: 'a' in s).count()
  # numBs = logData.filter(lambda s: 'b' in s).count()
  r1 = task2.wordFrequency('Øverst_ADV') 
  print 'Øverst_ADV appears:', r1 
  # print 'Øverst_ADV appears: ', task2.wordFrequency('b') 
  # print "Lines with a: %i, lines with b: %i" % (numAs, numBs)