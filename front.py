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

# os.system("spark-submit app.py")

with open('datos/almeria_20.json') as f:
    states_topo = json.load(f)

st.set_page_config(
   page_title="Almería 2015",
   page_icon="☀️",
   # layout="wide",
   # initial_sidebar_state="expanded",
)

st.title('Almeria 2015')

# @st.cache()
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
]

class Section(Enum):
    INTRO 	= "Introducción"
    PRIMERO = secciones[0][0]
    SEGUNDO = secciones[1][0]
    TERCERO = secciones[2][0]
    CUARTO 	= secciones[3][0]
    QUINTO 	= secciones[4][0]
    SEXTO 	= secciones[5][0]
    SEPTIMO = secciones[6][0]
    OCTAVO 	= secciones[7][0]
    NOVENO 	= secciones[8][0]
    DECIMO 	= secciones[9][0]
    UNDECIMO 	= secciones[10][0]

sections = list(map(lambda d: d.value, Section))
section_i = 0
section_param = st.experimental_get_query_params().get("section")
if section_param and section_param[0] in sections:
    section_i = sections.index(section_param[0])

section = st.sidebar.radio(
    "Section",
    sections,
    index=section_i
)

if section == Section.INTRO.value:
    # st.experimental_set_query_params(section=Section.INTRO.value)

    st.write("""
# Introducing [`streamlit-observable`](https://github.com/asg017/streamlit-observable)!
👋🏼 Hello! This Streamlit app is an introduction to the [`streamlit-observable`](https://github.com/asg017/streamlit-observable) 
library - a Streamlit custom component for embeding [Observable notebooks](https://observablehq.com)
into Streamlit apps. You can render, re-use, and recycle any Observable notebook
found on [observablehq.com](https://observablehq.com), 
giving you access to hundreds of data visualizations,
maps, charts, and animations that you can embed into any Streamlit app!
👈🏼Check out the sidebar for a deep-dive into different ways you can use 
`streamlit-observable` in your apps. Each example has a checkbox that looks like this:""")

if section == Section.PRIMERO.value:

    # -- 1. Ránking de sectores más rentables (topIngresosSector.csv)

    st.write("1. Ránking de sectores más rentables")
    st.caption('En este grafica se ve los sectores mas rentables')

    with st.spinner('Cargando...'):
        df = fetch_data(secciones[0][1])
        df = df.drop(df.columns[[0]], axis=1)
        st.bar_chart(pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR']))

if section == Section.SEGUNDO.value:
# -- 2. Top Movimientos por Sector (topMovimientosSector.csv)

    st.write("2. Top Movimientos por Sector")
    st.caption('En este grafica se ve el movimiento de los sectores')

    #  my_bar = st.progress(0)
    #  for percent_complete in range(20):
    #      time.sleep(0.1)
    #      my_bar.progress(percent_complete + 1)
        
    df2 = fetch_data(secciones[1][1])
    df2.drop(df2.columns[[0]], axis=1, inplace=True)
    st.bar_chart(pd.DataFrame(data={'Total':df2['total'].values},index=df2['SECTOR']))


if section == Section.TERCERO.value:
# -- 3. Total Movimientos por horas (totalMovsPorHoras.csv)

    st.write("3. Total Movimientos por horas")
    st.caption('En este grafica se ve el Movimiento por las horas')

    df = fetch_data(secciones[2][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df.sort_values(by=['FRANJA_HORARIA'], inplace=True)
    df = pd.DataFrame(data={'Movimiento total':df['total'].values},index=df['FRANJA_HORARIA'])
    st.bar_chart(df)


if section == Section.CUARTO.value:
# -- 4. Total Movimientos por dia de semana (totalMovsDiaSemana.csv)

    st.write("4. Total Movimientos por dia de la semana")
    st.caption('En este grafica se ve el Movimiento por los dias de la semana')

    df = fetch_data(secciones[3][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
	# df.replace({'dia': 0}, "Lunes", inplace=True)
    df = pd.DataFrame(data={'Movimiente total':df['total'].values}, index=dias)
    st.bar_chart(df)


if section == Section.QUINTO.value:
# -- 5. En qué sector se gasta más los días lluviosos

    st.write("5. En qué sector se gasta más los días lluviosos")
    st.caption('En este grafica se podria ver que la gente compra cuando lluvia')
    df = fetch_data(secciones[4][1])
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df = pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR'].values)
# df = df.T
    st.bar_chart(df)

if section == Section.SEXTO.value:

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

if section == Section.SEPTIMO.value:

    st.write(secciones[6][0])
    st.caption('Aquí podrá visualizar el volumen de transaciones por sector y por franja horaria')

    df = fetch_data(secciones[6][1])
    df.rename(columns = {'Unnamed: 0':'time'}, inplace = True)
    df = df.set_index('time')
    st.bar_chart(df)

if section == Section.OCTAVO.value:

    st.write("8.Barrios donde se compre muchos alimentos pero no hay comercio de alimentación")
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


if section == Section.NOVENO.value:
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

if section == Section.DECIMO.value:
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


if section == Section.UNDECIMO.value:
    # Mejora: poner el nombre del barrio junto con el cod postal
    df = fetch_data(secciones[10][1])
    cod_postales = df['CP_CLIENTE'].unique()
    cod_postales.sort()
    cod_selec = st.selectbox('Elige codigo postal', cod_postales,)
    df = df.loc[ (df['CP_CLIENTE'] == int(cod_selec))]
    # df
    st.bar_chart(pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR']))
    
