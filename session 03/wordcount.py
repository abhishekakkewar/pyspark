# spark 1.x = RDD - Risilient Distributed Dataset, its collection of object (in memory),
# spark 2.x = DataFrame
# spark 3.x = DataFrame

# Problem statement : count unique words
from pyspark import SparkConf, SparkContext
import re
from cgitb import reset
import findspark
findspark.init()


confobj = SparkConf().setMaster("local[2]").setAppName("wordapp")

sc = SparkContext(conf=confobj)

# "hello how are you"


def splitword(line):
    return re.compile(r'\W', re.UNICODE).split(line.lower())


lines = sc.textFile("content.txt")

words = lines.flatMap(splitword)
wordsmap = words.map(lambda w: (w, 1))

wordsunique = wordsmap.reduceByKey(lambda c1, c2: c1 + c2)

wordsmap2 = wordsunique.map(lambda w: (w[1], w[0]))

wordsfinal = wordsmap2.sortByKey(False)

result = wordsfinal.collect()

for r in result:
    print(r, " ", type(r))
