from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
import pandas as pd
import datetime

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')

spark = SparkSession.builder.master("local[*]").getOrCreate()

if __name__ == '__main__':

    uploadToGoogle = True
    createCSV = True

    # main()
    #hacemos lectura de CSVs
    datos_csv = (spark.read.csv('datos/cards.csv',header=True, inferSchema=True, sep ="|"))
    datos_csv.createOrReplaceTempView('tarjetas')
    datos_csv = (spark.read.csv('datos/weather.csv',header=True, inferSchema=True, sep =";"))
    datos_csv.createOrReplaceTempView('clima')

    # Top Ingresos por Sector
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/topIngresosSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topIngresosSector.csv')
        blob.upload_from_filename('output/topIngresosSector.csv')

    # Top Movimientos por Sector
    result = spark.sql('''SELECT SECTOR, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/topMovimientosSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topMovimientosSector.csv')
        blob.upload_from_filename('output/topMovimientosSector.csv')

    # Total Movimientos por Horas
    result = spark.sql('''SELECT FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY FRANJA_HORARIA ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/totalMovsPorHoras.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsPorHoras.csv')
        blob.upload_from_filename('output/totalMovsPorHoras.csv')

    # Total Movimientos por día de la semana
    result = spark.sql('''SELECT WEEKDAY(DIA) as dia, COUNT(IMPORTE) as total FROM tarjetas GROUP BY WEEKDAY(DIA) ORDER BY dia ASC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/totalMovsDiaSemana.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsDiaSemana.csv')
        blob.upload_from_filename('output/totalMovsDiaSemana.csv')

    # En qué sector se gasta mas los dias lluviosos
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) as total FROM clima JOIN tarjetas ON clima.FECHA=tarjetas.DIA where Precip>=2.0 GROUP BY SECTOR ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/topSectorLluvia.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('topSectorLluvia.csv')
        blob.upload_from_filename('output/topSectorLluvia.csv')    

    # Barrios (código postal) ordenados por importe total
    result = spark.sql('''SELECT CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/barriosPorImporte.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosPorImporte.csv')
        blob.upload_from_filename('output/barriosPorImporte.csv')

    # Total Movimientos por Horas y Sector
    result = spark.sql('''SELECT SECTOR, FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR, FRANJA_HORARIA ORDER BY SECTOR, FRANJA_HORARIA ASC''')
    result.show()
    df = result.toPandas()
    sectores = df['SECTOR'].unique()
    franjas = df['FRANJA_HORARIA'].unique()

    df2 = pd.DataFrame(index = franjas, columns=sectores)
    print(df2)
    for i in sectores:
        for j in franjas:
            try:
                df2.loc[j, i]=df.loc[(df['SECTOR']==i)&(df['FRANJA_HORARIA']==j),'total'].values[0]
            except:
                df2.loc[j, i]=0
    

    print(df2)
    if createCSV: df2.to_csv('output/totalMovsPorHorasSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsPorHorasSector.csv')
        blob.upload_from_filename('output/totalMovsPorHorasSector.csv')

    # Barrios donde se compre muchos alimentos pero no hay comercio de alimentación
    result = spark.sql('''SELECT CP_CLIENTE, count(IMPORTE) as total FROM tarjetas WHERE CP_CLIENTE!=CP_COMERCIO AND SECTOR="ALIMENTACION" GROUP BY CP_CLIENTE ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/barriosAlimentacioSinTiendas.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosAlimentacioSinTiendas.csv')
        blob.upload_from_filename('output/barriosAlimentacioSinTiendas.csv')

    # Barrios con mayor gasto en salud
    result = spark.sql('''SELECT CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas WHERE SECTOR="SALUD" GROUP BY CP_CLIENTE ORDER BY total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/barriosMayorSalud.csv')
    if uploadToGoogle:
        result.toPandas().to_csv('barriosMayorSalud.csv')
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosMayorSalud.csv')
        blob.upload_from_filename('output/barriosMayorSalud.csv')

    # Barrios que mas gastan en cada sector
    result = spark.sql('''SELECT SECTOR, CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE, SECTOR ORDER BY SECTOR, total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/barriosMayorSector.csv')
    if uploadToGoogle:
        result.toPandas().to_csv('barriosMayorSector.csv')
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('barriosMayorSector.csv')
        blob.upload_from_filename('output/barriosMayorSector.csv')

    # Volumen de compras por sector y barrio
    result = spark.sql('''SELECT SECTOR, CP_CLIENTE, COUNT(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE, SECTOR ORDER BY SECTOR, total DESC''')
    result.show()
    if createCSV: result.toPandas().to_csv('output/volumenComprasSector.csv')
    if uploadToGoogle:
        result.toPandas().to_csv('volumenComprasSector.csv')
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('volumenComprasSector.csv')
        blob.upload_from_filename('output/volumenComprasSector.csv')

    # Gasto medio compra
    # result = spark.sql('''SELECT ''')
    # result.show()
    # if createCSV: result.toPandas().to_csv('/output/gastoMedioCompra.csv')
    # if uploadToGoogle:
    #     bucket = storage_client.get_bucket('datosbd')
    #     blob = bucket.blob('gastoMedioCompra.csv')
    #     blob.upload_from_filename('/output/gastoMedioCompra.csv')

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
