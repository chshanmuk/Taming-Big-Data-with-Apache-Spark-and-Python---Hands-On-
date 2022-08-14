# This python program is used to count the ratings of the files provided u.data

from pyspark import SparkConf, SparkContext
import collections

#initialize Spark Context

conf = SparkConf().setMaster("local").setAppName("Ratinghistogram")
sc = SparkContext(conf = conf)

# spark driver has been intialized now its time to start with the transformations
# spark has lazy evalution so nothing will get executed unitl and unless the action function is been triggered

lines = sc.textFile("u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults= collections.OrderedDict(sorted(result.items()))

for key,value in sortedResults.items():
    print ("%s %i" % (key, value))