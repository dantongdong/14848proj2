import os
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)
stopWords = ["they", "she", "he", "it", "the", "as", "if", "and", ""]
terms = None
topN = None


for root, dirs, files in os.walk('Data', topdown=True):
    for file in files:
        filePath = root + '/' + file
        print(filePath)
        words = sc.textFile(filePath).flatMap(lambda line: line.split(" ")).filter(lambda word: word not in stopWords)
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

terms.saveAsTextFile("searchTerm")
topN = topN.sortBy(lambda wordCount: wordCount[1], ascending=False)
topN.saveAsTextFile("topN")



