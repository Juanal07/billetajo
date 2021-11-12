from flask import Flask, jsonify, abort, request, make_response
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
import os

os.system('set GOOGLE_APPLICATION_CREDENTIALS=big-data-328215-74151e35e325.json')

spark = SparkSession.builder.master("local[*]").getOrCreate()
storage_client = storage.Client()


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
    

    #PARA DERCARGA DE ARCHIVOS DEL BUCKET
    # source_blob_name= 'cards.csv'
    # destination_file_name = 'downloaded_cards.csv'
    # bucket_name = 'datosbd'
    # # get bucket object 

    # try:
    #     bucket = storage_client.bucket(bucket_name)
    #     blob = bucket.blob(source_blob_name)
    #     blob.download_to_filename(destination_file_name)
    #     print('file: ',destination_file_name,' downloaded from bucket: ',bucket_name,' successfully')
    # except Exception as e:
    #     print(e)