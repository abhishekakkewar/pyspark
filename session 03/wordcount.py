# spark 1.x = RDD - Risilient Distributed Dataset, its collection of object (in memory),
# spark 2.x = DataFrame
# spark 3.x = DataFrame

# Problem statement : count unique words
import re
from cgitb import reset


from pyspark import SparkConf, SparkContext
#import findspark
#findspark.init()


confobj = SparkConf().setMaster("local[2]").setAppName("wordapp")

sc = SparkContext(conf=confobj)

# "hello how are you"


def splitword(line):
    return re.compile(r'\W', re.UNICODE).split(line.lower())


lines = sc.textFile("content.txt")

words = lines.flatMap(splitword)

result = words.collect()

for r in result:
    print(r, " ", type(r))
