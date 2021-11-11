from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("ContarValoraciones")
sc = SparkContext(conf = conf)

lines = sc.textFile("ratings.csv")

ratings = lines.map(lambda x: x.split(",")[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
