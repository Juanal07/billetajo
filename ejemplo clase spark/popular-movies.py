from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PeliculasMasPopulares")
sc = SparkContext(conf = conf)

lines = sc.textFile("ratings.csv")
movies = lines.map(lambda x: (int(x.split(",")[1]), 1))
#NO FUNCIONA, dejado a medias en clase
# para evita la primera línea del headre hacemos
# movies = lines.zipWithIndex().filter(lambda (row, idx): idx > 0).map(lambda (row,idx): row).map(lambda x: (int(x.split(",")[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)