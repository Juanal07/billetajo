import numpy as np
import streamlit as st
import pandas as pd
from google.cloud import storage
import datetime
from enum import Enum
import os

storage_client = storage.Client.from_service_account_json('big-data-328215-74151e35e325.json')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "big-data-328215-74151e35e325.json"

st.set_page_config(
   page_title="Almería 2015",
   page_icon="☀️",
   # layout="wide",
   # initial_sidebar_state="expanded",
)

st.title('Almeria 2015')

csv_names=[
   'totalMovsPorHorasSector.csv',
   'barriosAlimentacioSinTiendas.csv',
   'barriosMayorSalud.csv',
   'barriosMayorSector.csv',
   'volumenComprasSector.csv',
   'totalMovsPorHoras.csv',
   'totalMovsDiaSemana.csv',
   'topSectorLluvia.csv',
]

# -- 3. Total Movimientos por horas (totalMovsPorHoras.csv)

st.write("3. Total Movimientos por horas")
st.caption('En este grafica se ve el Movimiento por las horas')

df = pd.read_csv('gs://datosbd/{}'.format(csv_names[5]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df.sort_values(by=['FRANJA_HORARIA'], inplace=True)
df = pd.DataFrame(data={'Movimiento total':df['total'].values},index=df['FRANJA_HORARIA'])
st.bar_chart(df)

# -- 4. Total Movimientos por dia de semana (totalMovsDiaSemana.csv)

st.write("3. Total Movimientos por dia de la semana")
st.caption('En este grafica se ve el Movimiento por los dias de la semana')

df = pd.read_csv('gs://datosbd/{}'.format(csv_names[6]))
df.drop(df.columns[[0]], axis=1, inplace=True)
dias = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
# df.replace({'dia': 0}, "Lunes", inplace=True)
df = pd.DataFrame(data={'Movimiente total':df['total'].values}, index=dias)
st.bar_chart(df)

# -- 5. En qué sector se gasta más los días lluviosos

st.write("5. En qué sector se gasta más los días lluviosos")
st.caption('En este grafica se podria ver que la gente compra cuando lluvia')
df = pd.read_csv('gs://datosbd/{}'.format(csv_names[7]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df = pd.DataFrame(data={'Total':df['total'].values},index=df['SECTOR'].values)
# df = df.T
st.bar_chart(df)


st.write("7.Gráfica con las transacciones de media en cada franja horaria de los 10 sectores")
st.caption('Aquí podrá visualizar el volumen de transaciones por sector y por franja horaria')

df = pd.read_csv('gs://datosbd/{}'.format(csv_names[0]))
sectores = df['SECTOR'].unique()
franjas = df['FRANJA_HORARIA'].unique()

alimentacion = df.loc[df['SECTOR']=='ALIMENTACION','total'].values
auto = df.loc[df['SECTOR']=='AUTO','total'].values
belleza = df.loc[df['SECTOR']=='BELLEZA','total'].values
hogar = df.loc[df['SECTOR']=='HOGAR','total'].values
moda = df.loc[df['SECTOR']=='MODA Y COMPLEMENTOS','total'].values
ocio = df.loc[df['SECTOR']=='OCIO Y TIEMPO LIBRE','total'].values
otros = df.loc[df['SECTOR']=='OTROS','total'].values
restauracion = df.loc[df['SECTOR']=='RESTAURACION','total'].values
salud = df.loc[df['SECTOR']=='SALUD','total'].values
tecnologia = df.loc[df['SECTOR']=='TECNOLOGIA','total'].values
# Faltan auto, belleza, hogar
d={'Alimentacion':alimentacion, 'Moda':moda, 'Ocio':ocio,'Otros':otros,'Restauracion':restauracion,'Salud':salud,'Tecnología':tecnologia}
df = pd.DataFrame(data=d,index=franjas)
st.area_chart(df)
st.bar_chart(df)


st.write("8.Barrios donde se compre muchos alimentos pero no hay comercio de alimentación")
st.caption('En este mapa podrá visualizar los puntos donde puede ser más rentable abrir un supermercado')

df = pd.read_csv('gs://datosbd/{}'.format(csv_names[1]))
df.drop(df.columns[[0]], axis=1, inplace=True)
df
df = pd.DataFrame(
np.random.randn(1000, 2) / [50, 50] + [37.16, -2.33],
columns=['lat', 'lon'])
df
st.map(df)






# for csv in csv_names:
#   st.write(csv)
#   data = pd.read_csv('gs://datosbd/{}'.format(csv))
#   data.drop(data.columns[0], axis=1, inplace=True)
#   data






# class Section(Enum):
#     INTRO = "Introduction"
    
#     BAR_CHART = "Static Embed: Bar Chart"
#     PENGUINS =  "Static Embed: Scatterplot Matrix Penguins"
#     SPIKE = "Static Embed: Spike Map"
#     VORONOI = "Static Embed: Trader Joes Voronoi Map"
#     WIKI = "Static Embed: Bar Chart Race of Wikipedia Views"

#     FORM = "Bi-Directional Embed: HTML Form"
#     DRAW = "Bi-Directional Embed: Drawing Canvas"
#     COUNTIES = "Bi-Directional Embed: Selecting Counties"
#     MATRIX = "Bi-Directional Embed: Matrix Input"


# sections = list(map(lambda d: d.value, Section))
# section_i = 0
# section_param = st.experimental_get_query_params().get("section")
# if section_param and section_param[0] in sections:
#     section_i = sections.index(section_param[0])

# section = st.sidebar.radio(
#     "Section",
#     sections,
#     index=section_i
# )

# if section == Section.INTRO.value:
#     st.experimental_set_query_params(section=Section.INTRO.value)

#     st.write("""
# # Introducing [`streamlit-observable`](https://github.com/asg017/streamlit-observable)!
# 👋🏼 Hello! This Streamlit app is an introduction to the [`streamlit-observable`](https://github.com/asg017/streamlit-observable) 
# library - a Streamlit custom component for embeding [Observable notebooks](https://observablehq.com)
# into Streamlit apps. You can render, re-use, and recycle any Observable notebook
# found on [observablehq.com](https://observablehq.com), 
# giving you access to hundreds of data visualizations,
# maps, charts, and animations that you can embed into any Streamlit app!
# 👈🏼Check out the sidebar for a deep-dive into different ways you can use 
# `streamlit-observable` in your apps. Each example has a checkbox that looks like this:""")
