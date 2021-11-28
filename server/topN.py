from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")

N = int(sys.argv[1])

def parse(line):
    line = line[1: len(line)-1]
    line = line.split(", ")
    key = line[0][1: len(line[0])-1]
    value = line[1]
    return (key, value)


words = sc.textFile('///user/dantongdong310/topN/part-*').map(lambda x: parse(x))
for word in words.take(N):
    print(word)