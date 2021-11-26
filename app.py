from pyspark.sql import SparkSession
from google.cloud import storage
import pandas as pd

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')
spark = SparkSession.builder.master("local[*]").getOrCreate()

#funcion de guardado y subida al bucket de csv
def uploadBucket(name):

    result.toPandas().to_csv('output/'+name)
    bucket = storage_client.get_bucket('datosbd')
    blob = bucket.blob(name)
    blob.upload_from_filename('output/'+name)

if __name__ == '__main__':

    #hacemos lectura de CSVs
    datos_csv = (spark.read.csv('datos/cards.csv',header=True, inferSchema=True, sep ="|"))
    datos_csv.createOrReplaceTempView('tarjetas')
    datos_csv = (spark.read.csv('datos/weather.csv',header=True, inferSchema=True, sep =";"))
    datos_csv.createOrReplaceTempView('clima')

    # Top Ingresos por Sector
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    # result.show()
    uploadBucket('topIngresosSector.csv')

    # Top Movimientos por Sector
    result = spark.sql('''SELECT SECTOR, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR ORDER BY total DESC''')
    # result.show()
    uploadBucket('topMovimientosSector.csv')

    # Total Movimientos por Horas
    result = spark.sql('''SELECT FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY FRANJA_HORARIA ORDER BY total DESC''')
    # result.show()
    uploadBucket('totalMovsPorHoras.csv')

    # Total Movimientos por día de la semana
    result = spark.sql('''SELECT WEEKDAY(DIA) as dia, COUNT(IMPORTE) as total FROM tarjetas GROUP BY WEEKDAY(DIA) ORDER BY dia ASC''')
    # result.show()
    uploadBucket('totalMovsDiaSemana.csv')

    # En qué sector se gasta mas los dias lluviosos
    result = spark.sql('''SELECT SECTOR, SUM(IMPORTE) as total FROM clima JOIN tarjetas ON clima.FECHA=tarjetas.DIA where Precip>=2.0 GROUP BY SECTOR ORDER BY total DESC''')
    # result.show()
    uploadBucket('topSectorLluvia.csv')

    # Barrios (código postal) ordenados por importe total
    result = spark.sql('''SELECT CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE ORDER BY total DESC''')
    # result.show()
    uploadBucket('barriosPorImporte.csv')

    # Total Movimientos por Horas y Sector
    result = spark.sql('''SELECT SECTOR, FRANJA_HORARIA, COUNT(IMPORTE) AS total FROM tarjetas GROUP BY SECTOR, FRANJA_HORARIA ORDER BY SECTOR, FRANJA_HORARIA ASC''')
    # result.show()
    df = result.toPandas()
    sectores = df['SECTOR'].unique()
    franjas = df['FRANJA_HORARIA'].unique()
    df2 = pd.DataFrame(index = franjas, columns=sectores)
    for i in sectores:
        for j in franjas:
            try:
                df2.loc[j, i]=int(df.loc[(df['SECTOR']==i)&(df['FRANJA_HORARIA']==j),'total'].values[0])
            except:
                df2.loc[j, i]=0

    # En este caso no se llama a la función al ser de diferente casuistica
    if createCSV: df2.to_csv('output/totalMovsPorHorasSector.csv')
    if uploadToGoogle:
        bucket = storage_client.get_bucket('datosbd')
        blob = bucket.blob('totalMovsPorHorasSector.csv')
        blob.upload_from_filename('output/totalMovsPorHorasSector.csv')

    # Barrios donde se compre muchos alimentos pero no hay comercio de alimentación
    result = spark.sql('''SELECT CP_CLIENTE, count(IMPORTE) as total FROM tarjetas WHERE CP_CLIENTE!=CP_COMERCIO AND SECTOR="ALIMENTACION" GROUP BY CP_CLIENTE ORDER BY total DESC''')
    # result.show()
    uploadBucket('barriosAlimentacioSinTiendas.csv')

    # Barrios con mayor gasto en salud
    result = spark.sql('''SELECT CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas WHERE SECTOR="SALUD" GROUP BY CP_CLIENTE ORDER BY total DESC''')
    # result.show()
    uploadBucket('barriosMayorSalud.csv')

    # Barrios que mas gastan en cada sector
    result = spark.sql('''SELECT SECTOR, CP_CLIENTE, SUM(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE, SECTOR ORDER BY SECTOR, total DESC''')
    # result.show()
    uploadBucket('barriosMayorSector.csv')

    # Volumen de compras por sector y barrio
    result = spark.sql('''SELECT SECTOR, CP_CLIENTE, COUNT(IMPORTE) as total FROM tarjetas GROUP BY CP_CLIENTE, SECTOR ORDER BY SECTOR, total DESC''')
    # result.show()
    uploadBucket('volumenComprasSector.csv')

    # Media de importes por sector pudiendo elegir el clima
    result = spark.sql('''SELECT SECTOR, FECHA, AVG(IMPORTE) as media, first(Rad) as Rad, first(Precip) as Precip FROM tarjetas, clima WHERE tarjetas.DIA=clima.FECHA GROUP BY FECHA, SECTOR ORDER BY FECHA ASC, SECTOR DESC''')
    # result.show()
    uploadBucket('mediaImportesClima.csv')

    print("Todos los kpi se han realizado")
