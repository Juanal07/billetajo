import streamlit as st
import pandas as pd
from google.cloud import storage
from enum import Enum
import os
import json
from streamlit_folium import folium_static
import folium

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "big-data-328215-74151e35e325.json"

def launchSpark():
    os.system("spark-submit app.py")

with open('datos/almeria_20.json') as f:
    states_topo = json.load(f)

st.set_page_config(
   page_title="Almería 2015",
   page_icon="☀️",
)

st.title('Almeria 2015')

def fetch_data(name):
    return pd.read_csv('gs://datosbd/{}'.format(name))

secciones = [
	["1.Ránking de sectores más rentables", "topIngresosSector.csv"],
	["2.Top Movimientos por Sector", "topMovimientosSector.csv"],
	["3.Total Movimientos por Horas", "totalMovsPorHoras.csv"],
	["4.Total Movimientos por día de la semana", "totalMovsDiaSemana.csv"],
	["5.En qué sector se gasta más los días lluviosos", "topSectorLluvia.csv"],
	["6.Barrios (código postal) ordenados por importe total", "barriosPorImporte.csv"],
	["7.Gráfica con las transacciones de media en cada franja horaria de los 10 sectores", "totalMovsPorHorasSector.csv"],
	["8.Barrios donde se compre muchos alimentos pero no hay comercio de alimentación", "barriosAlimentacioSinTiendas.csv"],
	["9.Localizar zona donde se gasta más en salud para futuras campañas de captación en seguros", "barriosMayorSalud.csv"],
	["10.Ranking barrios que más gastan en cada sector", "barriosMayorSector.csv"],
	["11.Volumen de compras de todo el año por sector", "volumenComprasSector.csv"],
    ["12.Media de importes por sector pudiendo elegir el clima", "mediaImportesClima.csv"]
]

checkboxes = []

checkboxes.append(st.sidebar.checkbox("Introducción", True))

for section in secciones:
    checkboxes.append(st.sidebar.checkbox(section[0]))

if checkboxes[0]:

    st.write("""
    # Bienvenido a nuestro trabajo de Big Data!
    👋 En esta web encontrará información de utilidad para distintos futuros servicios que puede ofrecer un banco.

    🌅 Esta información esta basada en datos de movimientos bancarios y el tiempo atmosférico de la región de Almeria en el año 2015.

    👈 En la barra lateral podrá ir navegando por los distintos KPIs.

    🚀 Pulsando en el siguiente botón podrá actualizar las consultas Spark por si hay nuevos datos""")
    # Boton para relanzar la ejecución de Spark
    if st.button('Lanzar spark'):
        launchSpark()

if checkboxes[1]:
    # -- 1. Ránking de sectores más rentables (topIngresosSector.csv)

    st.write(secciones[0][0])
    st.caption('En este grafica se ve los sectores mas rentables')

    with st.spinner('Cargando...'):
        df = fetch_data(secciones[0][1])
        df = df.drop(df.columns[[0]], axis=1)
        st.bar_chart(pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR']))

if checkboxes[2]:
# -- 2. Top Movimientos por Sector (topMovimientosSector.csv)

    st.write(secciones[1][0])
    st.caption('En este grafica se ve el movimiento de los sectores')
        
    df2 = fetch_data(secciones[1][1])
    df2.drop(df2.columns[[0]], axis=1, inplace=True)
    st.bar_chart(pd.DataFrame(data={'Total':df2['total'].values},index=df2['SECTOR']))


if checkboxes[3]:
# -- 3. Total Movimientos por horas (totalMovsPorHoras.csv)

    st.write(secciones[2][0])
    st.caption('En este grafica se ve el Movimiento por las horas')

    df = fetch_data(secciones[2][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df.sort_values(by=['FRANJA_HORARIA'], inplace=True)
    df = pd.DataFrame(data={'Movimiento total':df['total'].values},index=df['FRANJA_HORARIA'])
    st.bar_chart(df)


if checkboxes[4]:
# -- 4. Total Movimientos por dia de semana (totalMovsDiaSemana.csv)

    st.write(secciones[3][0])
    st.caption('En este grafica se ve el Movimiento por los dias de la semana')

    df = fetch_data(secciones[3][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    df = pd.DataFrame(data={'Movimiente total':df['total'].values}, index=dias)
    st.bar_chart(df)


if checkboxes[5]:
# -- 5. En qué sector se gasta más los días lluviosos

    st.write(secciones[4][0])
    st.caption('En este grafica se podria ver que la gente compra cuando lluvia')
    df = fetch_data(secciones[4][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df = pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR'].values)
    st.bar_chart(df)

if checkboxes[6]:

	st.write(secciones[5][0])
	st.caption('Aquí podrá ser interesante para conceder hipotecas')

	df = fetch_data(secciones[5][1])
	df.drop(df.columns[[0]], axis=1, inplace=True)
	df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

	m = folium.Map(location=[37.16, -2.33], zoom_start=9)

	folium.Choropleth(
        geo_data=states_topo,
        topojson='objects.almeria_wm',
        name="choropleth",
        data=df,
        columns=["CP_CLIENTE", "total"],
        key_on="feature.properties.COD_POSTAL",
        fill_color="YlGn",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Barrios por importe total",
    ).add_to(m)

	folium_static(m)
	st.write(df)

if checkboxes[7]:

    st.write(secciones[6][0])
    st.caption('Aquí podrá visualizar el volumen de transaciones por sector y por franja horaria')

    df = fetch_data(secciones[6][1])
    df.rename(columns = {'Unnamed: 0':'time'}, inplace = True)
    df = df.set_index('time')
    st.bar_chart(df)

if checkboxes[8]:

    st.write(secciones[7][0])
    st.caption('En este mapa podrá visualizar los puntos donde puede ser más rentable abrir un supermercado')

    df = fetch_data(secciones[7][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

    m = folium.Map(location=[37.16, -2.33], zoom_start=9)

    folium.Choropleth(
        geo_data=states_topo,
        topojson='objects.almeria_wm',
        name="choropleth",
        data=df,
        columns=["CP_CLIENTE", "total"],
        key_on="feature.properties.COD_POSTAL",
        fill_color="Reds",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Barrios con mas compras sin tienda de alimentacion",
    ).add_to(m)

    folium_static(m)


if checkboxes[9]:
    st.write(secciones[8][0])
    st.caption('AUN A DEFINIR')
    df = fetch_data(secciones[8][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

    m2 = folium.Map(location=[37.16, -2.33], zoom_start=9)

    folium.Choropleth(
        geo_data=states_topo,
        topojson='objects.almeria_wm',
        name="choropleth",
        data=df,
        columns=["CP_CLIENTE", "total"],
        key_on="feature.properties.COD_POSTAL",
        fill_color="BuGn",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="otra",
    ).add_to(m2)

    folium_static(m2)

if checkboxes[10]:
    st.write(secciones[9][0])
    st.caption('AUN A DEFINIR')
    df = fetch_data(secciones[9][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df['CP_CLIENTE'] = df['CP_CLIENTE'].apply(lambda x: '{0:0>5}'.format(x))

    m3 = folium.Map(location=[37.16, -2.33], zoom_start=9)

    folium.Choropleth(
        geo_data=states_topo,
        topojson='objects.almeria_wm',
        name="choropleth",
        data=df,
        columns=["CP_CLIENTE", "total"],
        key_on="feature.properties.COD_POSTAL",
        fill_color="RdYlGn_r",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="otra",
    ).add_to(m3)

    folium_static(m3)


if checkboxes[11]:

    st.write(secciones[11][0])
    st.caption('AUN A DEFINIR')

    # Mejora: poner el nombre del barrio junto con el cod postal
    df = fetch_data(secciones[10][1])
    cod_postales = df['CP_CLIENTE'].unique()
    cod_postales.sort()
    cod_selec = st.selectbox('Elige codigo postal', cod_postales,)
    df = df.loc[ (df['CP_CLIENTE'] == int(cod_selec))]
    # df
    st.bar_chart(pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR']))

if checkboxes[12]:

    st.write(secciones[11][0])
    st.caption('AUN A DEFINIR')

    df = fetch_data(secciones[11][1])
    cod_selec = st.selectbox('Elige el tiempo', ["Soleado", "Lluvioso", "Nublado"],)
    if cod_selec=="Soleado":
        df = df.loc[ (df['Rad'] > 15.0) & (df['Precip'] == 0.0)]
    elif cod_selec=="Lluvioso":
        df = df.loc[ (df['Precip'] >= 2.0)]
    else:
        df = df.loc[ (df['Rad'] < 15.0) & (df['Precip'] <= 2.0)]
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df

    data = []
    for sec in df['SECTOR'].unique():
        data.append(df['media'].loc[ (df['SECTOR'] == sec) ].mean())
    st.bar_chart(pd.DataFrame(data,index=df['SECTOR'].unique()))    
