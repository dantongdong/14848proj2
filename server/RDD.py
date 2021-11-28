import os
from pyspark import SparkContext, SparkConf, SparkFiles

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)
stopWords = ["they", "she", "he", "it", "the", "as", "if", "and", ""]
terms = None
topN = None

files = sc.textFile("gs://dataproc-staging-us-west1-127099418400-2p0asb0o/fileList.txt")
allFile = files.collect()
for filePath in allFile:
    try:
        words = sc.textFile('///user/dantongdong310/' + filePath).flatMap(lambda line: line.split(" ")).filter(lambda word: word not in stopWords)
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
        # terms
        if not topN:
            topN = wordCounts
        else:
            topN = topN.union(wordCounts).reduceByKey(lambda a, b: a + b)
        # topN
        word4File = wordCounts.mapValues(lambda count: filePath + " " + str(count))
        if not terms:
            terms = word4File
        else:
            terms = terms.union(word4File)
    except:
        continue

terms.saveAsTextFile('///user/dantongdong310/' + "searchTerm")
topN = topN.sortBy(lambda wordCount: wordCount[1], ascending=False)
topN.saveAsTextFile('///user/dantongdong310/' + "topN")



