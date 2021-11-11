from flask import Flask, jsonify, abort, request, make_response
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# import spark
# from flask_cors import CORS

def main():
    app = Flask(__name__)
    # CORS(app)

    @app.errorhandler(404)
    def not_found(error):
        return make_response(jsonify({'error': 'Not found'}), 404)

    @app.route("/")
    def hello_world():
        return "<p>Hello, World!</p>"

    @app.route('/api', methods=['GET'])
    def test():
        return make_response(jsonify({'test': 'hola mundo!'}), 200)

    # @app.route('/api/sentiment', methods=['POST'])
    # def sentimiento():
    #     return sentiment.getSentiment(request.json['text'])

    app.run(debug=True)

if __name__ == '__main__':

    # print('hola')
    # main()
    # sc = SparkContext(master = local[*], app id = local-1636643309602)
    # rdd = sc.textFile('README.md')
    # rdd.count()

    datos_csv = (spark.read.csv('datos/cards.csv',header=True, inferSchema=True, sep ="|"))
    datos_csv.createOrReplaceTempView('tarjetas')
    spark.sql('''SELECT SECTOR, IMPORTE FROM tarjetas ORDER BY IMPORTE DESC''').show()