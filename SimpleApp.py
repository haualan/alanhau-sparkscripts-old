#!/usr/bin/python
# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
# this is the location of the cluster
master = "spark://ec2-52-10-98-37.us-west-2.compute.amazonaws.com:7077"
appName = "DataScience HW1 CT"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def split_and_cast(x):
  r = x.split()[0:3]
  r = tuple([r[0],int(r[1]),int(r[2])])
  return r


class WordFreqCluster:
  """HW1 Task2, examine unigram frequencies, find those with highly correlated frequencies and those with low correlated frequencies"""
  def __init__(self):
    # self.ngramsFile = "googlebooks-eng-all-1gram-20120701-a"  # Should be some file on HDFS
    self.ngramsFile = "sampledata.txt"
    self.ngramsData = sc.textFile(self.ngramsFile).cache()


  def topWordFrequencies(self):
    r = self.ngramsData.map(split_and_cast) \
                        .filter(lambda x: x[1] > 1980 ).cache()

    self.recentngrams= r

    r =r.map(lambda x: x[0::2]) \
                        .reduceByKey(lambda x,y: x+y) \
                        .map(lambda x:(x[1],x[0])) \
                        .sortByKey(True)

    return r.take(10)


  def find_distinct(self):
    recentngrams = self.recentngrams
    r = recentngrams.distinct()\
                    .count()
    return r

  def find_correlation(self):
    recentngrams = self.recentngrams




  # def correl(self, word1, word2):

# plan:
# 1. count frequencies of last 50 years group by word, grab 100 most frequent unigrams
# 2. find correl coefficient of 100 by 100 Words
# 3. sort by correl and find top and bottom 20 



if __name__ == "__main__":
  task2 = WordFreqCluster()

  # numAs = logData.filter(lambda s: 'a' in s).count()
  # numBs = logData.filter(lambda s: 'b' in s).count()

  print task2.topWordFrequencies()
  print task2.find_distinct()
  # print 'Øverst_ADV appears: ', task2.wordFrequency('b') 
  # print "Lines with a: %i, lines with b: %i" % (numAs, numBs)