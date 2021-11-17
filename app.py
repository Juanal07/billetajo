from flask import Flask, jsonify, abort, request, make_response
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')



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

    uploadToGoogle = False
    createCSV = False

    # print('hola')
    # main()
    # sc = SparkContext(master = local[*], app id = local-1636643309602)
    # rdd = sc.textFile('README.md')
    # rdd.count()
    datos_csv = (spark.read.csv('datos/cards.csv',header=True, inferSchema=True, sep ="|"))
    datos_csv.createOrReplaceTempView('tarjetas')
    datos_csv = (spark.read.csv('datos/weather.csv',header=True, inferSchema=True, sep =";"))
    datos_csv.createOrReplaceTempView('clima')

    # Top Ingresos por Sector
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('topIngresosSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topIngresosSector.csv')
        blob.upload_from_filename('topIngresosSector.csv')

    # Top Movimientos por Sector
    result = spark.sql('''SELECT SECTOR, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('topMovimientosSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topMovimientosSector.csv')
        blob.upload_from_filename('topMovimientosSector.csv')

    # Total Movimientos por Horas
    result = spark.sql('''SELECT FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY FRANJA_HORARIA ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('totalMovsPorHoras.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsPorHoras.csv')
        blob.upload_from_filename('totalMovsPorHoras.csv')

    # Total Movimientos por día de la semana
    result = spark.sql('''SELECT WEEKDAY(DIA) as dia, COUNT(IMPORTE) FROM tarjetas GROUP BY WEEKDAY(DIA) ORDER BY dia ASC''')
    result.show()
    if createCSV: result.toPandas().to_csv('totalMovsDiaSemana.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsDiaSemana.csv')
        blob.upload_from_filename('totalMovsDiaSemana.csv')

    # En qué sector se gasta mas los dias lluviosos
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) as total FROM clima JOIN tarjetas ON clima.FECHA=tarjetas.DIA where Precip>=2.0 GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('topSectorLluvia.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topSectorLluvia.csv')
        blob.upload_from_filename('topSectorLluvia.csv')    

    # Barrios (código postal) ordenados por importe total
    result = spark.sql('''SELECT CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('barriosPorImporte.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosPorImporte.csv')
        blob.upload_from_filename('barriosPorImporte.csv')

    # Total Movimientos por Horas y Sector
    result = spark.sql('''SELECT SECTOR, FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR, FRANJA_HORARIA ORDER BY SECTOR, FRANJA_HORARIA ASC''')
    result.show()
    if createCSV: result.toPandas().to_csv('totalMovsPorHorasSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsPorHorasSector.csv')
        blob.upload_from_filename('totalMovsPorHorasSector.csv')

    # Barrios donde se compre muchos alimentos pero no hay comercio de alimentación
    result = spark.sql('''SELECT CP_CLIENTE, count(IMPORTE) as total FROM tarjetas WHERE CP_CLIENTE!=CP_COMERCIO AND SECTOR="ALIMENTACION" GROUP BY CP_CLIENTE ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('barriosAlimentacioSinTiendas.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosAlimentacioSinTiendas.csv')
        blob.upload_from_filename('barriosAlimentacioSinTiendas.csv')

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

    #PARA SUBIR ARCHIVOS AL BUCKET
    # bucket = storage_client.get_bucket('datosbd')
    # blob = bucket.blob('test.csv')
    # blob.upload_from_filename('test.csv')